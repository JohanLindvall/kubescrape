package azurediag

import "testing"

// FuzzDecode drives the envelope splitter and the record decoder over
// arbitrary bytes. What arrives on a hub is whatever the diagnostic setting —
// or anything else holding a producer credential for the namespace — put
// there, and the decode runs on the reader goroutine of a cluster singleton:
// a panic here takes the whole Deployment down (there is no recover(), by
// design), so the parser must refuse or count every malformed shape rather
// than fail on one.
func FuzzDecode(f *testing.F) {
	f.Add([]byte(metricEnvelope))
	f.Add([]byte(logEnvelope))
	f.Add([]byte(`{"records":[]}`))
	f.Add([]byte(`{"records":[{"category":"c","count":3}]}`))
	f.Add([]byte(`{"records":[{"time":"not a time","metricName":"m","average":"x","total":1e400}]}`))
	f.Add([]byte(`{"records":[{"resourceId":"/SUBSCRIPTIONS/x/RESOURCEGROUPS/","metricName":"m","count":1}]}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(`{"records":[{`))
	f.Add([]byte(`{"records":[{"time":"2026-07-28T10:01:00Z","metricName":"cpu","average":1,"average":2}]}`))
	f.Fuzz(func(_ *testing.T, msg []byte) {
		var out [][]byte
		_ = splitEnvelope(msg, func(raw []byte) error {
			_, out, _ = decodeRecord(raw, out)
			return nil
		})
	})
}
