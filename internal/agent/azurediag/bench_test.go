package azurediag

// Benchmarks for the azurediag hot path. The reader is a cluster-singleton
// (256Mi limit) consuming an Event Hubs namespace: platform metrics alone are
// (resources x ~5 aggregations)/minute and resource logs (SQL audit, AKS
// control plane) reach thousands of records/second sustained — so the
// per-record decode/convert cost is what bounds the pipeline, the same way
// the tailer's per-line budget bounds log ingestion. Fixtures are built with
// fmt OUTSIDE the timed loops; ns/record is reported wherever one op covers a
// whole batch.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// benchArmID returns one of `resources` distinct ARM resource IDs, in the
// uppercase form Azure emits.
func benchArmID(i, resources int) string {
	return fmt.Sprintf("/SUBSCRIPTIONS/6B2B3E76-0000-0000-0000-000000000000/RESOURCEGROUPS/PROD-RG/PROVIDERS/MICROSOFT.SQL/SERVERS/PRODSRV/DATABASES/DB%03d", i%resources)
}

// benchLogEnvelope synthesizes a {"records":[...]} envelope of SQL-audit
// style resource logs — the sustained high-rate shape — spread across
// `resources` distinct ARM resources (~700 bytes per record, like the real
// thing).
func benchLogEnvelope(records, resources int) []byte {
	var sb strings.Builder
	sb.WriteString(`{"records":[`)
	for i := 0; i < records; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb,
			`{"time":"2026-07-28T10:%02d:%02d.4832910Z","resourceId":%q,"category":"SQLSecurityAuditEvents","operationName":"AuditEvent","level":"Informational","resultType":"Succeeded","correlationId":"9f2a77aa-3c1e-4b6f-9d10-6f0c%08d","tenantId":"72f988bf-86f1-41af-91ab-2d7cd011db47","location":"westeurope","properties":{"action_id":"BAV","succeeded":true,"session_id":%d,"statement":"SELECT o.id, o.status, o.total FROM orders o WHERE o.customer_id = @p1 AND o.created_at > @p2 ORDER BY o.created_at DESC","database_name":"db%03d","server_principal_name":"app-payments","client_ip":"10.24.1.%d","application_name":"payments-api","duration_milliseconds":%d,"response_rows":%d}}`,
			(i/60)%60, i%60, benchArmID(i, resources), i, 4000+i, i%resources, i%250, i%40, i%500)
	}
	sb.WriteString("]}")
	return []byte(sb.String())
}

// benchMetricNames is a realistic Azure SQL platform-metric spread.
var benchMetricNames = [5]string{"cpu_percent", "dtu_consumption_percent", "storage_percent", "physical_data_read_percent", "connection_successful"}

// benchMetricEnvelope synthesizes a {"records":[...]} envelope of platform
// metric records (all five window aggregations present) across `resources`
// distinct ARM resources.
func benchMetricEnvelope(records, resources int) []byte {
	var sb strings.Builder
	sb.WriteString(`{"records":[`)
	for i := 0; i < records; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb,
			`{"count":%d,"total":%d,"minimum":%d.25,"maximum":%d.5,"average":%d.125,"resourceId":%q,"time":"2026-07-28T10:%02d:00Z","metricName":%q,"timeGrain":"PT1M"}`,
			4+i%7, 40+i, 1+i%3, 19+i%11, 10+i%5, benchArmID(i, resources), i%60, benchMetricNames[i%len(benchMetricNames)])
	}
	sb.WriteString("]}")
	return []byte(sb.String())
}

// reportPerRecord normalizes a batch-per-op benchmark to per-record cost.
func reportPerRecord(b *testing.B, records int) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(records), "ns/record")
}

// BenchmarkSplitEnvelope measures splitting one Event Hubs message body into
// its raw records (100 records per envelope).
func BenchmarkSplitEnvelope(b *testing.B) {
	env := benchLogEnvelope(100, 10)
	b.SetBytes(int64(len(env)))
	b.ReportAllocs()
	b.ResetTimer()
	var n int
	for i := 0; i < b.N; i++ {
		n = 0
		if err := splitEnvelope(env, func([]byte) error { n++; return nil }); err != nil {
			b.Fatal(err)
		}
	}
	if n != 100 {
		b.Fatalf("records = %d, want 100", n)
	}
	reportPerRecord(b, 100)
}

// BenchmarkDecodeRecord measures decoding one record's envelope fields,
// reusing the GetPaths scratch slice across records the way Reader.decode
// does.
func BenchmarkDecodeRecord(b *testing.B) {
	logRaws, err := collectEnvelope(benchLogEnvelope(1, 1))
	if err != nil || len(logRaws) != 1 {
		b.Fatalf("log fixture: %v (%d records)", err, len(logRaws))
	}
	metRaws, err := collectEnvelope(benchMetricEnvelope(1, 1))
	if err != nil || len(metRaws) != 1 {
		b.Fatalf("metric fixture: %v (%d records)", err, len(metRaws))
	}
	b.Run("log", func(b *testing.B) {
		raw := logRaws[0]
		var scratch [][]byte
		b.SetBytes(int64(len(raw)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rec, s, err := decodeRecord(raw, scratch)
			scratch = s
			if err != nil || rec.metric {
				b.Fatalf("decode: err=%v metric=%v", err, rec.metric)
			}
		}
	})
	b.Run("metric", func(b *testing.B) {
		raw := metRaws[0]
		var scratch [][]byte
		b.SetBytes(int64(len(raw)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rec, s, err := decodeRecord(raw, scratch)
			scratch = s
			if err != nil || !rec.metric {
				b.Fatalf("decode: err=%v metric=%v", err, rec.metric)
			}
		}
	})
}

// BenchmarkConvertLogs measures record → plog conversion for a batch of 100
// log records over 10 distinct ARM resources: bare (no options), and the full
// shared chain in the tailer's order (scrub → logAttributes → enrich →
// rules).
func BenchmarkConvertLogs(b *testing.B) {
	env := benchLogEnvelope(100, 10)
	run := func(b *testing.B, r *Reader) {
		recs := r.decode([][]byte{env})
		if len(recs) != 100 {
			b.Fatalf("decoded records = %d, want 100", len(recs))
		}
		b.SetBytes(int64(len(env)))
		b.ReportAllocs()
		b.ResetTimer()
		var n int
		for i := 0; i < b.N; i++ {
			n = r.convertLogs(recs).LogRecordCount()
		}
		if n != 100 {
			b.Fatalf("log records = %d, want 100", n)
		}
		reportPerRecord(b, 100)
	}
	b.Run("bare", func(b *testing.B) {
		run(b, New(Config{}))
	})
	b.Run("full-chain", func(b *testing.B) {
		scrub, err := logscrub.New(logscrub.Config{Builtin: []string{"defaults"}})
		if err != nil {
			b.Fatal(err)
		}
		extractor, err := logattrs.New(&logattrs.Config{Rules: []logattrs.Rule{
			{Key: "properties.database_name", Attribute: "db.name"},
		}})
		if err != nil {
			b.Fatal(err)
		}
		rules, err := logline.NewLineFilter([]logline.LineRule{
			{Action: "keep", Match: []string{"azure.category=SQLSecurityAuditEvents"}},
		})
		if err != nil {
			b.Fatal(err)
		}
		run(b, New(Config{Enrich: true, Scrub: scrub, LogAttrs: extractor, Rules: rules}))
	})
}

// BenchmarkConvertMetrics measures record → pmetric conversion for a batch of
// 100 metric records (5 aggregations each) over 10 distinct ARM resources.
func BenchmarkConvertMetrics(b *testing.B) {
	env := benchMetricEnvelope(100, 10)
	r := New(Config{})
	recs := r.decode([][]byte{env})
	if len(recs) != 100 {
		b.Fatalf("decoded records = %d, want 100", len(recs))
	}
	b.SetBytes(int64(len(env)))
	b.ReportAllocs()
	b.ResetTimer()
	var n int
	for i := 0; i < b.N; i++ {
		n = r.convertMetrics(recs).DataPointCount()
	}
	if n != 500 {
		b.Fatalf("data points = %d, want 500", n)
	}
	reportPerRecord(b, 100)
}

// BenchmarkDecodeAll measures Reader.decode over one poll of 10 messages of
// 10 records each (half log envelopes, half metric envelopes), the shape one
// consume iteration hands to deliver.
func BenchmarkDecodeAll(b *testing.B) {
	msgs := make([][]byte, 10)
	var total int
	for i := range msgs {
		if i%2 == 0 {
			msgs[i] = benchLogEnvelope(10, 10)
		} else {
			msgs[i] = benchMetricEnvelope(10, 10)
		}
		total += len(msgs[i])
	}
	r := New(Config{})
	b.SetBytes(int64(total))
	b.ReportAllocs()
	b.ResetTimer()
	var n int
	for i := 0; i < b.N; i++ {
		n = len(r.decode(msgs))
	}
	if n != 100 {
		b.Fatalf("records = %d, want 100", n)
	}
	reportPerRecord(b, 100)
}
