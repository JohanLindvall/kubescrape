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
// discipline as internal/logline.

import (
	"bytes"
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

// splitEnvelope returns the raw records inside one Event Hubs message:
// {"records":[...]} (the diagnostic-settings shape), a bare array, or a bare
// single record object.
func splitEnvelope(msg []byte) ([][]byte, error) {
	i := skipWS(msg, 0)
	if i >= len(msg) {
		return nil, errNotJSON
	}
	switch msg[i] {
	case '{':
		recs, err := ljson.Lookup(msg[i:], "records")
		if err == nil && len(recs) > 0 && recs[0] == '[' {
			return splitArray(recs)
		}
		// No records array: treat the object as a single record.
		return [][]byte{msg[i:]}, nil
	case '[':
		return splitArray(msg[i:])
	}
	return nil, errNotJSON
}

// splitArray splits a raw JSON array into its top-level elements (verbatim
// slices of arr). Strings — with escapes — and nesting are respected.
func splitArray(arr []byte) ([][]byte, error) {
	var out [][]byte
	depth, start := 0, -1
	inStr, esc := false, false
	flush := func(end int) {
		if el := bytes.TrimSpace(arr[start:end]); len(el) > 0 {
			out = append(out, el)
		}
		start = -1
	}
	for i := 1; i < len(arr); i++ {
		c := arr[i]
		if inStr {
			switch {
			case esc:
				esc = false
			case c == '\\':
				esc = true
			case c == '"':
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
			if start < 0 {
				start = i
			}
		case '{', '[':
			if start < 0 {
				start = i
			}
			depth++
		case '}':
			depth--
		case ']':
			if depth == 0 {
				if start >= 0 {
					flush(i)
				}
				return out, nil
			}
			depth--
		case ',':
			if depth == 0 && start >= 0 {
				flush(i)
			}
		case ' ', '\t', '\n', '\r':
		default:
			if start < 0 {
				start = i
			}
		}
	}
	return nil, errors.New("unterminated JSON array")
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
