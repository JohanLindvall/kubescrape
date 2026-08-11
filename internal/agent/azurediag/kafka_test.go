package azurediag

// End-to-end over kfake: a real kgo consumer group against an in-memory
// cluster, proving the wiring the fakeSource tests bypass — group join,
// regex topic selection, offset commit, and that a SECOND reader (a new
// group generation, as after a pod replacement) resumes past what the first
// committed rather than re-consuming or skipping ahead.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
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

// errFetch is the record-less shape kgo injects for every partition-level
// notification (addFakeReadyForDraining).
func errFetch(topic string, partition int32, err error) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic:      topic,
		Partitions: []kgo.FetchPartition{{Partition: partition, Err: err}},
	}}}}
}

// A fetch carrying errors and NO records must not clear the readiness gate: by
// its records alone it is indistinguishable from a clean empty poll, so
// consume would report a consumer that can never consume as ready. It must
// equally not fail the poll when the error is SCOPED to one topic — Run then
// closes the source (a full LeaveGroup) and rebuilds after a backoff, so the
// likeliest partial shape (read permission on nine of ten insights-* hubs, a
// hub mid-deletion, Event Hubs recycling a group session) turns into a
// permanent rebalance loop that takes the nine healthy hubs down with it.
func TestScopedFetchErrorsAreNotFatal(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tc := range []struct {
		name    string
		fetches kgo.Fetches
	}{
		// A metadata load error scoped to ONE topic; ConsumeRegex over
		// ^insights-.* makes this the shape of a per-hub ACL gap.
		{"topic authorization", errFetch("insights-logs-audit", -1, kerr.TopicAuthorizationFailed)},
		// A hub mid-deletion.
		{"unknown topic", errFetch("insights-logs-audit", -1, kerr.UnknownTopicOrPartition)},
		// Informational: kgo resets consuming itself.
		{"data loss", errFetch("insights-logs-audit", 0, &kgo.ErrDataLoss{Topic: "insights-logs-audit"})},
		// Informational: kgo rejoins the group itself. Event Hubs recycles
		// connections aggressively, so this one is routine.
		{"group session", errFetch("", 0, &kgo.ErrGroupSession{Err: kerr.RebalanceInProgress})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msgs, healthy, err := pollResult(tc.fetches, log)
			if err != nil {
				t.Fatalf("err = %v, want nil: a rebuilt client cannot fix this and the LeaveGroup takes every other hub with it", err)
			}
			if healthy {
				t.Error("healthy = true for an errors-only fetch; the readiness gate would clear for an unconsumable hub")
			}
			if len(msgs) != 0 {
				t.Errorf("msgs = %q, want none", msgs)
			}
		})
	}
}

// The other half: a condition no further fetch can clear still reaches Run's
// reopen-with-backoff arm. Unscoped and non-retriable means every hub is
// unreachable, which is the only shape a fresh client (with freshly read
// credentials) can recover from.
func TestUnscopedFatalFetchErrorsFailThePoll(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tc := range []struct {
		name    string
		fetches kgo.Fetches
		want    error
	}{
		{"cluster authorization", errFetch("", -1, kerr.ClusterAuthorizationFailed), kerr.ClusterAuthorizationFailed},
		{"sasl authentication", errFetch("", -1, kerr.SaslAuthenticationFailed), kerr.SaslAuthenticationFailed},
		{"client closed", errFetch("", -1, kgo.ErrClientClosed), kgo.ErrClientClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msgs, healthy, err := pollResult(tc.fetches, log)
			if err == nil {
				t.Fatal("a cluster-wide failure must fail the poll and reach the reopen arm")
			}
			if !errors.Is(err, tc.want) {
				t.Errorf("err = %v, want the partition error preserved for errors.Is", err)
			}
			if healthy || msgs != nil {
				t.Errorf("healthy=%v msgs=%v, want neither from a failed poll", healthy, msgs)
			}
		})
	}
}

// Records beside errors stay a healthy poll: the records are processed, kgo
// retries the broken partitions, and a clean empty fetch is what lets the
// readiness gate clear on a quiet but reachable hub.
func TestRecordsAndCleanFetchesAreHealthy(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	partial := kgo.Fetches{{Topics: []kgo.FetchTopic{{
		Topic: "insights-logs-audit",
		Partitions: []kgo.FetchPartition{
			{Partition: 0, Records: []*kgo.Record{{Value: []byte("x")}}},
			{Partition: 1, Err: kerr.NotLeaderForPartition},
		},
	}}}}
	msgs, healthy, err := pollResult(partial, log)
	if err != nil || !healthy {
		t.Fatalf("errors beside records: healthy=%v err=%v, want healthy and nil", healthy, err)
	}
	if len(msgs) != 1 || string(msgs[0]) != "x" {
		t.Fatalf("msgs = %q, want the fetched record kept", msgs)
	}

	if msgs, healthy, err = pollResult(kgo.Fetches{}, log); err != nil || !healthy || len(msgs) != 0 {
		t.Fatalf("clean empty fetch: msgs=%v healthy=%v err=%v, want empty, healthy and nil", msgs, healthy, err)
	}
}

// writeCS drops a connection string in a temp file and returns its path.
func writeCS(t *testing.T, cs string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cs")
	if err := os.WriteFile(path, []byte(cs+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// An ENTITY-SCOPED connection string names the one hub the credential may
// read. Resolve must consume exactly that hub: the `^insights-.*` default
// needs namespace-wide metadata this credential cannot list, and would not
// match the entity's name anyway — so without this the consumer joins the
// group and then sits at zero topics, indistinguishable from a quiet hub.
func TestResolveDerivesTopicFromEntityPath(t *testing.T) {
	const key = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
	cs := "Endpoint=sb://mydiag-we-0a1b2c3d.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=" + key + ";EntityPath=azure"

	k := KafkaConfig{ConnectionStringFile: writeCS(t, cs)}
	if err := k.Resolve(slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	if want := []string{"mydiag-we-0a1b2c3d.servicebus.windows.net:9093"}; !slices.Equal(k.Brokers, want) {
		t.Fatalf("brokers = %v, want %v", k.Brokers, want)
	}
	if want := []string{"azure"}; !slices.Equal(k.Topics, want) {
		t.Fatalf("topics = %v, want %v (the EntityPath hub)", k.Topics, want)
	}
	if k.Mechanism == nil || k.Mechanism.Name() != "PLAIN" {
		t.Fatalf("mechanism = %v, want PLAIN", k.Mechanism)
	}
	if k.TLSConfig == nil {
		t.Fatal("Event Hubs' Kafka surface is TLS-only; TLSConfig must be set")
	}
}

// A namespace-scoped string carries no EntityPath, so the regex default must
// survive untouched (empty Topics is what newKafkaSource reads as ^insights-.*).
func TestResolveNamespaceScopedKeepsRegexDefault(t *testing.T) {
	k := KafkaConfig{ConnectionStringFile: writeCS(t,
		"Endpoint=sb://myns.servicebus.windows.net/;SharedAccessKeyName=Root;SharedAccessKey=AAAA=")}
	if err := k.Resolve(slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	if len(k.Topics) != 0 {
		t.Fatalf("topics = %v, want empty (the ^insights-.* regex path)", k.Topics)
	}
}

// Explicit topics beat the derived default, and an explicit list that omits
// the entity is warned about at startup — every fetch would otherwise fail
// authorization with the reason only in kgo's error log.
func TestResolveExplicitTopicsWinOverEntityPath(t *testing.T) {
	cs := "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=AAAA=;EntityPath=azure"

	var buf bytes.Buffer
	k := KafkaConfig{ConnectionStringFile: writeCS(t, cs), Topics: []string{"azure", "other"}}
	if err := k.Resolve(slog.New(slog.NewTextHandler(&buf, nil))); err != nil {
		t.Fatal(err)
	}
	if want := []string{"azure", "other"}; !slices.Equal(k.Topics, want) {
		t.Fatalf("topics = %v, want %v (explicit wins)", k.Topics, want)
	}
	if strings.Contains(buf.String(), "fail authorization") {
		t.Fatalf("warned although the entity IS in the list: %s", buf.String())
	}

	buf.Reset()
	k = KafkaConfig{ConnectionStringFile: writeCS(t, cs), Topics: []string{"other"}}
	if err := k.Resolve(slog.New(slog.NewTextHandler(&buf, nil))); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "fail authorization") {
		t.Fatalf("no warning for topics excluding the entity: %s", buf.String())
	}
}

// An explicit -azure-eventhub-namespace still wins over the Endpoint, but the
// EntityPath must be honoured either way — the two are independent halves.
func TestResolveExplicitNamespaceStillTakesEntityPath(t *testing.T) {
	k := KafkaConfig{
		Namespace:            "override.servicebus.windows.net",
		ConnectionStringFile: writeCS(t, "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKey=AAAA=;EntityPath=azure"),
	}
	if err := k.Resolve(slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	if k.Brokers[0] != "override.servicebus.windows.net:9093" {
		t.Fatalf("brokers = %v, want the explicit namespace", k.Brokers)
	}
	if want := []string{"azure"}; !slices.Equal(k.Topics, want) {
		t.Fatalf("topics = %v, want %v", k.Topics, want)
	}
}

// A connection string with no Endpoint and no -azure-eventhub-namespace has
// nothing to dial: refuse at startup, naming the file.
func TestResolveWithoutEndpointFails(t *testing.T) {
	k := KafkaConfig{ConnectionStringFile: writeCS(t, "SharedAccessKeyName=test;SharedAccessKey=AAAA=")}
	err := k.Resolve(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil || !strings.Contains(err.Error(), "Endpoint=sb://") {
		t.Fatalf("err = %v, want a refusal naming the missing Endpoint", err)
	}
}

// The entity-scoped path end to end over a real kgo client: a hub whose name
// does NOT match ^insights-.* is consumed when the EntityPath named it. This
// is the half Resolve's unit tests cannot reach — that populating Topics puts
// newKafkaSource on kgo.ConsumeTopics instead of the ConsumeRegex branch, and
// that the resulting client really does read the hub.
func TestKafkaConsumesEntityPathTopic(t *testing.T) {
	const hub = "azure" // deliberately not an insights-* name
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, hub))
	if err != nil {
		t.Skipf("kfake unavailable: %v", err)
	}
	defer cluster.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic(hub))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := cl.ProduceSync(ctx, &kgo.Record{Value: []byte(logEnvelope)}).FirstErr(); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	cl.Close()

	// What Resolve derives from ...;EntityPath=azure.
	exp := &captureExporter{}
	r := New(Config{
		Kafka: KafkaConfig{
			Brokers: cluster.ListenAddrs(),
			Group:   "kubescrape-entity-test",
			Start:   StartBeginning,
			Topics:  []string{hub},
		},
		Exporter: exp,
	})
	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(runCtx); close(done) }()
	defer func() { stop(); <-done }()

	deadline := time.After(20 * time.Second)
	for {
		exp.mu.Lock()
		got := 0
		for _, ld := range exp.logs {
			got += ld.LogRecordCount()
		}
		exp.mu.Unlock()
		if got >= 2 { // the envelope carries two records
			return
		}
		select {
		case <-deadline:
			t.Fatal("timed out: the EntityPath hub was never consumed")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Two consumers, two hubs, one export chain: the multi-source shape end to
// end over real kgo clients. Each Reader owns its client, group and offsets,
// and both deliver into the shared exporter — which is what running N
// entity-scoped credentials in one process actually does.
func TestKafkaMultipleSourcesConsumeInParallel(t *testing.T) {
	const hubA, hubB = "azure", "otherhub"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, hubA, hubB))
	if err != nil {
		t.Skipf("kfake unavailable: %v", err)
	}
	defer cluster.Close()

	for _, hub := range []string{hubA, hubB} {
		cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic(hub))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := cl.ProduceSync(ctx, &kgo.Record{Value: []byte(logEnvelope)}).FirstErr(); err != nil {
			cancel()
			cl.Close()
			t.Fatal(err)
		}
		cancel()
		cl.Close()
	}

	// What ResolveSources produces for two entity-scoped credentials in one
	// namespace: a client per hub, each in its OWN group.
	exp := &captureExporter{}
	runCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{}, 2)
	for _, hub := range []string{hubA, hubB} {
		r := New(Config{
			Kafka: KafkaConfig{
				Brokers: cluster.ListenAddrs(),
				Group:   "kubescrape-multi." + hub,
				Start:   StartBeginning,
				Topics:  []string{hub},
			},
			Exporter: exp,
		})
		go func() { r.Run(runCtx); done <- struct{}{} }()
	}
	defer func() {
		stop()
		<-done
		<-done
	}()

	// Both hubs carry the two-record envelope, so all four must arrive.
	deadline := time.After(30 * time.Second)
	for {
		exp.mu.Lock()
		got := 0
		for _, ld := range exp.logs {
			got += ld.LogRecordCount()
		}
		exp.mu.Unlock()
		if got >= 4 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: only %d of 4 records arrived from the two hubs", got)
		case <-time.After(20 * time.Millisecond):
		}
	}
}
