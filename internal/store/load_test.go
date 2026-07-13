package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentChurnInvariants hammers the read API from 32 goroutines while
// writers upsert/delete pods at high rate, checking the invariants that hold
// at any interleaving:
//
//   - a GetContainer hit always returns the container's own pod (container
//     IDs encode their pod, so cross-wiring is detectable);
//   - after DeletePod returns, the pod is no longer in PodsOnNode (checked
//     synchronously by the writer that deleted it);
//   - GetPodByIP never returns a deleted or finished pod.
//
// Run under -race; it is the concurrency exerciser for the serving path.
func TestConcurrentChurnInvariants(t *testing.T) {
	s := New(time.Minute) // real clock: expiry interplay is not under test here
	duration := 2 * time.Second
	if testing.Short() {
		duration = 500 * time.Millisecond
	}

	const (
		writers        = 4
		podsPerWriter  = 8
		readers        = 32
		containerCount = writers * podsPerWriter
	)
	podName := func(w, p int) string { return fmt.Sprintf("pod-%d-%d", w, p) }
	podUID := func(w, p int) string { return fmt.Sprintf("uid-%d-%d", w, p) }
	contID := func(w, p int) string { return fmt.Sprintf("c0ffee%02d%02d", w, p) }
	podIP := func(w, p int) string { return fmt.Sprintf("10.1.%d.%d", w, p) }
	node := func(w int) string { return fmt.Sprintf("node-%d", w) }

	stop := make(chan struct{})
	var reads, writes atomic.Int64
	var wg sync.WaitGroup

	// Writers: each owns a disjoint pod range, so its delete-then-check is
	// not raced by another writer resurrecting the pod.
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rv := 0
			for gen := 0; ; gen++ {
				for p := range podsPerWriter {
					select {
					case <-stop:
						return
					default:
					}
					rv++
					pod := makePod(podUID(w, p), podName(w, p), node(w), fmt.Sprint(rv), map[string]string{"app": contID(w, p)})
					pod.Status.PodIP = podIP(w, p)
					s.UpsertPod(pod)
					writes.Add(1)
					if gen%3 == 2 { // periodically delete and verify
						uid := pod.UID
						s.DeletePod(uid)
						writes.Add(1)
						for _, np := range s.PodsOnNode(node(w)) {
							if np.Pod.Name == podName(w, p) && np.Pod.DeletedAt == nil {
								t.Errorf("pod %s on node %s after DeletePod returned", np.Pod.Name, node(w))
							}
							if np.Pod.Name == podName(w, p) {
								t.Errorf("deleted pod %s still in PodsOnNode", np.Pod.Name)
							}
						}
					}
				}
			}
		}()
	}

	// Readers: random-ish mix over the whole keyspace.
	readerErr := make(chan string, 1)
	for r := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := context.Background()
			i := r
			for {
				select {
				case <-stop:
					return
				default:
				}
				i++
				w, p := i%writers, (i/writers)%podsPerWriter
				switch i % 4 {
				case 0:
					// Non-blocking lookup: expired ctx degrades to a plain read.
					cctx, cancel := context.WithCancel(ctx)
					cancel()
					if res, ok, _ := s.GetContainer(cctx, contID(w, p)); ok {
						if want := podName(w, p); res.Pod.Name != want {
							select {
							case readerErr <- fmt.Sprintf("container %s resolved to pod %s, want %s", contID(w, p), res.Pod.Name, want):
							default:
							}
						}
					}
				case 1:
					if np, ok := s.GetPodByIP(podIP(w, p)); ok {
						if np.Pod.DeletedAt != nil || finishedPhase(np.Pod.Phase) {
							select {
							case readerErr <- fmt.Sprintf("GetPodByIP(%s) returned deleted/finished pod %s", podIP(w, p), np.Pod.Name):
							default:
							}
						}
					}
				case 2:
					for _, np := range s.PodsOnNode(node(w)) {
						_ = np.Pod.Name
					}
				case 3:
					_, _ = s.GetPodByUID(podUID(w, p))
				}
				reads.Add(1)
			}
		}()
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()
	select {
	case msg := <-readerErr:
		t.Fatal(msg)
	default:
	}
	t.Logf("churn: %d writes, %d reads in %v (%.0f reads/s)",
		writes.Load(), reads.Load(), duration, float64(reads.Load())/duration.Seconds())
	if reads.Load() == 0 || writes.Load() == 0 {
		t.Fatal("load test did no work")
	}
}
