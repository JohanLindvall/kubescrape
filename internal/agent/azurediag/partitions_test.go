package azurediag

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

// TestAssignedPartitionsFollowsTheGroup pins the "joined and owns nothing"
// signal: a consumer that reaches its hub owns its partitions while it runs,
// and hands them back when it leaves, so kubescrape_azure_partitions_assigned
// is 0 exactly when the group has assigned this member nothing.
func TestAssignedPartitionsFollowsTheGroup(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, "insights-logs-part"))
	if err != nil {
		t.Skipf("kfake unavailable: %v", err)
	}
	defer cluster.Close()

	before := AssignedPartitions()
	r := New(Config{
		Kafka: KafkaConfig{
			Brokers: cluster.ListenAddrs(),
			Group:   "kubescrape-partitions-test",
			Topics:  []string{"insights-logs-part"},
			Start:   StartBeginning,
		},
		Exporter: &captureExporter{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()

	deadline := time.Now().Add(20 * time.Second)
	for AssignedPartitions()-before < 2 {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("assigned partitions = %v after 20s, want both of the topic's partitions: the assignment "+
				"callback is not feeding the gauge", AssignedPartitions()-before)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	<-done
	deadline = time.Now().Add(10 * time.Second)
	for AssignedPartitions() != before {
		if time.Now().After(deadline) {
			t.Fatalf("assigned partitions = %v after the consumer left, want %v: a revoke is not being subtracted",
				AssignedPartitions(), before)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
