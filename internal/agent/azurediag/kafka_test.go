package azurediag

// End-to-end over kfake: a real kgo consumer group against an in-memory
// cluster, proving the wiring the fakeSource tests bypass — group join,
// regex topic selection, offset commit, and that a SECOND reader (a new
// group generation, as after a pod replacement) resumes past what the first
// committed rather than re-consuming or skipping ahead.

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestKafkaEndToEnd(t *testing.T) {
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, "insights-logs-audit"),
	)
	if err != nil {
		t.Skipf("kfake unavailable: %v", err)
	}
	defer cluster.Close()

	produce := func(payload string) {
		t.Helper()
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(cluster.ListenAddrs()...),
			kgo.DefaultProduceTopic("insights-logs-audit"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := cl.ProduceSync(ctx, &kgo.Record{Value: []byte(payload)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	produce(logEnvelope)

	run := func(exp *captureExporter, wantRecords int) {
		t.Helper()
		r := New(Config{
			Kafka: KafkaConfig{
				Brokers: cluster.ListenAddrs(),
				Group:   "kubescrape-test",
				Start:   StartBeginning,
				// No Topics: the ^insights-.* regex path is under test.
			},
			Exporter: exp,
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { r.Run(ctx); close(done) }()
		deadline := time.After(20 * time.Second)
		for {
			exp.mu.Lock()
			got := 0
			for _, ld := range exp.logs {
				got += ld.LogRecordCount()
			}
			exp.mu.Unlock()
			if got >= wantRecords {
				break
			}
			select {
			case <-deadline:
				cancel()
				<-done
				t.Fatalf("timed out; got %d of %d records", got, wantRecords)
			case <-time.After(20 * time.Millisecond):
			}
		}
		// Give the commit after the last export a moment before tearing down.
		time.Sleep(100 * time.Millisecond)
		cancel()
		<-done
	}

	// First reader consumes the backlog (2 records in the envelope).
	first := &captureExporter{}
	run(first, 2)

	// A replacement reader in the SAME group must resume past the committed
	// offset: only the new envelope, never the old one again.
	produce(metricEnvelope)
	second := &captureExporter{}
	r := New(Config{
		Kafka: KafkaConfig{
			Brokers: cluster.ListenAddrs(),
			Group:   "kubescrape-test",
			Start:   StartBeginning,
		},
		Exporter: second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	deadline := time.After(20 * time.Second)
	for {
		second.mu.Lock()
		metrics := len(second.metrics)
		logs := len(second.logs)
		second.mu.Unlock()
		if metrics > 0 {
			if logs != 0 {
				t.Error("the second reader re-consumed committed records")
			}
			break
		}
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("timed out waiting for the second reader")
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	<-done
}
