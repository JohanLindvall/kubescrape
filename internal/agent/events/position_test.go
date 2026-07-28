package events

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func store(t *testing.T) (*ConfigMapStore, *fake.Clientset) {
	t.Helper()
	c := fake.NewSimpleClientset()
	return &ConfigMapStore{Client: c, Namespace: "monitoring", Name: "kubescrape-events-position"}, c
}

// A missing ConfigMap is a cold start, not an error: the successor of a pod
// that never checkpointed must apply the start policy rather than fail.
func TestPositionMissingIsColdStart(t *testing.T) {
	s, _ := store(t)
	pos, found, err := s.Load(context.Background())
	if err != nil || found || pos.ResourceVersion != "" {
		t.Fatalf("missing = (%+v, %v, %v), want a clean cold start", pos, found, err)
	}
}

// Save creates the ConfigMap on first use, then updates it; Load round-trips.
func TestPositionSaveCreatesThenUpdates(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	mark := time.Now().UTC().Truncate(time.Second)

	if err := s.Save(ctx, Position{ResourceVersion: "10", Watermark: mark, Holder: "pod-a"}); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.Load(ctx)
	if err != nil || !found {
		t.Fatalf("load after create: found=%v err=%v", found, err)
	}
	if got.ResourceVersion != "10" || !got.Watermark.Equal(mark) || got.Holder != "pod-a" {
		t.Fatalf("round trip = %+v", got)
	}

	if err := s.Save(ctx, Position{ResourceVersion: "20", Watermark: mark}); err != nil {
		t.Fatal(err)
	}
	if got, _, _ = s.Load(ctx); got.ResourceVersion != "20" {
		t.Fatalf("after update = %q, want 20", got.ResourceVersion)
	}
	// One object, one key — a write is atomic.
	cms, _ := c.CoreV1().ConfigMaps("monitoring").List(ctx, metav1.ListOptions{})
	if len(cms.Items) != 1 || len(cms.Items[0].Data) != 1 {
		t.Fatalf("want exactly one ConfigMap with one key, got %d/%v", len(cms.Items), cms.Items)
	}
}

// An unparseable position is reported as an ERROR (not silently as a cold
// start): the caller counts it, so a corrupt checkpoint can never masquerade
// as a first run the way an empty one does.
func TestPositionCorruptIsReported(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	if _, err := c.CoreV1().ConfigMaps("monitoring").Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace},
		Data:       map[string]string{positionKey: "{not json"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	_, found, err := s.Load(ctx)
	if err == nil || found {
		t.Fatalf("corrupt position must surface an error, got found=%v err=%v", found, err)
	}
}

// A conflicting write (a replica that has not noticed it lost the lease)
// retries rather than failing. Either value is safe: the position is a lower
// bound on what was delivered, so a stale write only replays.
func TestPositionRetriesOnConflict(t *testing.T) {
	s, c := store(t)
	ctx := context.Background()
	if err := s.Save(ctx, Position{ResourceVersion: "10"}); err != nil {
		t.Fatal(err)
	}
	first := true
	c.PrependReactor("update", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		if first {
			first = false
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "kubescrape-events-position", nil)
		}
		return false, nil, nil
	})
	if err := s.Save(ctx, Position{ResourceVersion: "20"}); err != nil {
		t.Fatalf("a conflict must be retried, got %v", err)
	}
	if got, _, _ := s.Load(ctx); got.ResourceVersion != "20" {
		t.Fatalf("after the retry = %q, want 20", got.ResourceVersion)
	}
}
