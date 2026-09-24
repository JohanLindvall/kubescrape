package azurediag

import (
	"log/slog"
	"testing"
)

// addDecodeSeeds is the seed corpus both fuzz targets start from.
func addDecodeSeeds(f *testing.F) {
	f.Add([]byte(metricEnvelope))
	f.Add([]byte(logEnvelope))
	f.Add([]byte(`{"records":[]}`))
	f.Add([]byte(`{"records":[{"category":"c","count":3}]}`))
	f.Add([]byte(`{"records":[{"time":"not a time","metricName":"m","average":"x","total":1e400}]}`))
	f.Add([]byte(`{"records":[{"resourceId":"/SUBSCRIPTIONS/x/RESOURCEGROUPS/","metricName":"m","count":1}]}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"records":[{`))
	f.Add([]byte(`{"records":[{"time":"2026-07-28T10:01:00Z","metricName":"cpu","average":1,"average":2}]}`))
	f.Add([]byte(`{"records":[{"resourceId":"//PROVIDERS//RESOURCEGROUPS","metricName":"M","count":1},{"resourceId":"/subscriptions/s/providers/a/b/c/d/e","category":"x"}]}`))
}

// FuzzDecode drives the envelope splitter and the record decoder over
// arbitrary bytes. What arrives on a hub is whatever the diagnostic setting —
// or anything else holding a producer credential for the namespace — put
// there, and the decode runs on the reader goroutine of a cluster singleton:
// a panic here takes the whole Deployment down (there is no recover(), by
// design), so the parser must refuse or count every malformed shape rather
// than fail on one.
func FuzzDecode(f *testing.F) {
	addDecodeSeeds(f)
	f.Fuzz(func(_ *testing.T, msg []byte) {
		var out [][]byte
		_ = splitEnvelope(msg, func(raw []byte) error {
			_, out, _ = decodeRecord(raw, out)
			return nil
		})
	})
}

// FuzzConvert carries the same untrusted bytes through the stages AFTER the
// decode, which run on that same singleton goroutine: the production decode
// loop (Reader.decode, its skip-on-error and counters included), the ARM id
// parser, and both converters — the resource build, the log chain and the
// metric-name build. FuzzDecode alone discarded every decoded record, so none
// of that was ever fuzzed. It also asserts conservation: with no rules
// configured the log converter must emit exactly one record per decoded log
// record, and the metric converter one data point per aggregation present.
func FuzzConvert(f *testing.F) {
	addDecodeSeeds(f)
	r := New(Config{Logger: slog.New(slog.DiscardHandler)})
	f.Fuzz(func(t *testing.T, msg []byte) {
		// The id parser sees the raw bytes too, so it is exercised even when
		// the envelope does not decode.
		parseResourceID(string(msg))
		recs := r.decode([][]byte{msg})
		var logs, points int
		for i := range recs {
			parseResourceID(recs[i].resourceID)
			if !recs[i].metric {
				logs++
				continue
			}
			for _, has := range recs[i].has {
				if has {
					points++
				}
			}
		}
		if got := r.convertLogs(recs).LogRecordCount(); got != logs {
			t.Fatalf("convertLogs emitted %d records for %d decoded log records", got, logs)
		}
		if got := r.convertMetrics(recs).DataPointCount(); got != points {
			t.Fatalf("convertMetrics emitted %d data points for %d aggregations present", got, points)
		}
	})
}
