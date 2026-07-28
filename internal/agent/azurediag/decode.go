package azurediag

// Decoding of Azure diagnostic-settings payloads. Each Event Hubs message
// body is a JSON envelope {"records": [...]}; every record is either a
// resource LOG (category/level/operationName + a properties payload) or a
// platform METRIC (metricName + the pre-aggregated count/total/min/max/avg
// of one timeGrain window). The two are distinguished per record, not per
// hub, because a diagnostic setting may point logs and metrics at the same
// hub.
//
// Records are kept as RAW slices: the log path exports the record verbatim
// as the body (so logAttributes/enrich/logMetrics see exactly what Azure
// emitted), and only the handful of envelope fields needed for routing and
// attributes are extracted — via lightning's single-pass GetPaths, the same
// discipline as internal/logline. The array walk is lightning's ArrayEach
// (>= v0.0.62; extracted from the hand-rolled splitter that used to live
// here): strict syntax, so a malformed envelope is ONE decode error at the
// split, never elements silently fused or dropped.

import (
	"errors"
	"time"

	ljson "github.com/JohanLindvall/lightning/pkg/json"

	"github.com/JohanLindvall/kubescrape/internal/logline"
)

// record is one diagnostic record, decoded just far enough to convert.
type record struct {
	raw []byte // the record's verbatim JSON (aliases the message buffer)

	ts         time.Time
	resourceID string

	// log fields
	category, level, opName           string
	resultType, resultDesc            string
	correlationID, tenantID, location string

	// metric fields
	metricName, timeGrain string
	aggs                  [nAggs]float64
	has                   [nAggs]bool

	metric bool
}

// The five window aggregations an Azure metric record carries.
const (
	aggCount = iota
	aggTotal
	aggMinimum
	aggMaximum
	aggAverage
	nAggs
)

// aggNames index the metric-name suffixes by agg constant.
var aggNames = [nAggs]string{"count", "total", "minimum", "maximum", "average"}

// recordPaths is the fixed field set extracted from every record in one
// GetPaths pass; the indices below name the slots.
var recordPaths = func() [][]string {
	keys := []string{
		"time", "timeStamp", "resourceId",
		"category", "level", "operationName", "resultType", "resultDescription",
		"correlationId", "tenantId", "location",
		"metricName", "timeGrain", "count", "total", "minimum", "maximum", "average",
	}
	out := make([][]string, len(keys))
	for i, k := range keys {
		out[i] = []string{k}
	}
	return out
}()

const (
	pTime = iota
	pTimeStamp
	pResourceID
	pCategory
	pLevel
	pOperationName
	pResultType
	pResultDescription
	pCorrelationID
	pTenantID
	pLocation
	pMetricName
	pTimeGrain
	pCount
	pTotal
	pMinimum
	pMaximum
	pAverage
)

var errNotJSON = errors.New("message is not a JSON object or array")

// splitEnvelope invokes fn with each raw record inside one Event Hubs
// message: {"records":[...]} (the diagnostic-settings shape), a bare array,
// or a bare single record object. The callback form skips materializing a
// [][]byte per poll; slices alias msg.
func splitEnvelope(msg []byte, fn func(raw []byte) error) error {
	i := skipWS(msg, 0)
	if i >= len(msg) {
		return errNotJSON
	}
	switch msg[i] {
	case '{':
		err := ljson.ArrayEach(msg[i:], fn, "records")
		if errors.Is(err, ljson.ErrKeyNotFound) || errors.Is(err, ljson.ErrExpectArray) {
			// No records array: the object IS the record. Both errors are
			// pre-iteration (the descent fails before fn ever runs), so this
			// cannot double-emit.
			return fn(msg[i:])
		}
		return err
	case '[':
		return ljson.ArrayEach(msg[i:], fn)
	}
	return errNotJSON
}

func skipWS(b []byte, i int) int {
	for i < len(b) && (b[i] == ' ' || b[i] == '\t' || b[i] == '\n' || b[i] == '\r') {
		i++
	}
	return i
}

// decodeRecord extracts the envelope fields of one record. The raw slice is
// retained verbatim (it becomes the log body).
func decodeRecord(raw []byte, out [][]byte) (record, [][]byte, error) {
	out, err := ljson.GetPaths(raw, recordPaths, out)
	if err != nil {
		return record{}, out, err
	}
	r := record{raw: raw}
	str := func(i int) string {
		if len(out[i]) == 0 {
			return ""
		}
		s, ok := logline.RawScalarString(out[i])
		if !ok {
			return ""
		}
		return s
	}
	r.resourceID = str(pResourceID)
	r.category = str(pCategory)
	r.level = str(pLevel)
	r.opName = str(pOperationName)
	r.resultType = str(pResultType)
	r.resultDesc = str(pResultDescription)
	r.correlationID = str(pCorrelationID)
	r.tenantID = str(pTenantID)
	r.location = str(pLocation)
	r.metricName = str(pMetricName)
	r.timeGrain = str(pTimeGrain)

	if ts := str(pTime); ts != "" {
		r.ts, _ = time.Parse(time.RFC3339Nano, ts)
	}
	if r.ts.IsZero() {
		if ts := str(pTimeStamp); ts != "" {
			r.ts, _ = time.Parse(time.RFC3339Nano, ts)
		}
	}

	for agg, slot := range [nAggs]int{pCount, pTotal, pMinimum, pMaximum, pAverage} {
		if len(out[slot]) == 0 {
			continue
		}
		if f, err := ljson.ParseFloat(out[slot]); err == nil {
			r.aggs[agg], r.has[agg] = f, true
		}
	}
	// A metric record names its metric AND carries at least one aggregation;
	// anything else — including a log that happens to have a "count" field —
	// stays a log.
	if r.metricName != "" {
		for _, ok := range r.has {
			if ok {
				r.metric = true
				break
			}
		}
	}
	return r, out, nil
}
