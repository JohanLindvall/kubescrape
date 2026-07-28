package events

// The stream position, stored in a ConfigMap.
//
// It cannot be a node-local file like the tailer's positions store: the
// leader moves. After a kill the successor is a different pod, usually on a
// different node, and it must be able to read where the previous one got to —
// so the position lives in the API server, in the release namespace.
//
// SAFETY INVARIANT: a persisted position is only ever a LOWER BOUND on what
// has been delivered. Every writer advances it solely past events it saw
// acknowledged by the collector. Moving backwards (a stale write from a
// replica that lost the lease without noticing) replays events, which
// at-least-once already tolerates; moving forwards is only ever done by
// someone who exported that far. Two concurrent ack-gating writers therefore
// need no fencing, and never produce loss.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// positionKey is the ConfigMap data key holding the JSON document. One key,
// so a write is one atomic object update.
const positionKey = "position"

// Position is the resume point.
type Position struct {
	// ResourceVersion resumes the watch exactly. It ages out of the API
	// server's watch window within minutes, after which the watch fails Gone
	// and Watermark takes over.
	ResourceVersion string `json:"resourceVersion"`
	// Watermark is the last-observed time of the newest exported event, used
	// to filter a relist when the resourceVersion is too old.
	Watermark time.Time `json:"watermark"`
	// Holder and Updated are diagnostics: `kubectl get cm -o yaml` then says
	// who checkpointed and when.
	Holder  string    `json:"holder,omitempty"`
	Updated time.Time `json:"updated,omitempty"`
}

// PositionStore persists the resume point somewhere every replica can read.
type PositionStore interface {
	Load(ctx context.Context) (pos Position, found bool, err error)
	Save(ctx context.Context, pos Position) error
}

// ConfigMapStore keeps the position in a ConfigMap in one namespace.
type ConfigMapStore struct {
	Client    kubernetes.Interface
	Namespace string
	Name      string

	// cached is the last object we read or wrote, for optimistic concurrency
	// on update (a lost update is harmless per the invariant above, but a
	// conflict retry is nearly free).
	cached *corev1.ConfigMap
}

// Load reads the stored position. A missing ConfigMap is (found=false, nil
// error) — a cold start. An unparseable one is (found=false, error): the
// caller counts it and applies its cold-start policy, exactly as the agent's
// positions store surfaces Corrupt() rather than letting an undecodable file
// masquerade as a first run.
func (s *ConfigMapStore) Load(ctx context.Context) (Position, bool, error) {
	cm, err := s.Client.CoreV1().ConfigMaps(s.Namespace).Get(ctx, s.Name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return Position{}, false, nil
		}
		return Position{}, false, err
	}
	s.cached = cm
	raw, ok := cm.Data[positionKey]
	if !ok || raw == "" {
		return Position{}, false, nil
	}
	var pos Position
	if err := json.Unmarshal([]byte(raw), &pos); err != nil {
		return Position{}, false, fmt.Errorf("decoding %s/%s: %w", s.Namespace, s.Name, err)
	}
	if pos.ResourceVersion == "" {
		return Position{}, false, nil
	}
	return pos, true, nil
}

// Save writes the position, creating the ConfigMap on first use. One conflict
// retry: a concurrent writer means a replica that has not noticed it lost the
// lease, and either value is safe.
func (s *ConfigMapStore) Save(ctx context.Context, pos Position) error {
	pos.Updated = time.Now().UTC()
	body, err := json.Marshal(pos)
	if err != nil {
		return err
	}
	for attempt := 0; attempt < 2; attempt++ {
		cm := s.cached
		if cm == nil {
			cm, err = s.Client.CoreV1().ConfigMaps(s.Namespace).Get(ctx, s.Name, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				created, cerr := s.Client.CoreV1().ConfigMaps(s.Namespace).Create(ctx, &corev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{Name: s.Name, Namespace: s.Namespace},
					Data:       map[string]string{positionKey: string(body)},
				}, metav1.CreateOptions{})
				if cerr == nil {
					s.cached = created
					return nil
				}
				if !apierrors.IsAlreadyExists(cerr) {
					return cerr
				}
				s.cached = nil
				continue // someone created it first; re-read and update
			}
			if err != nil {
				return err
			}
		}
		next := cm.DeepCopy()
		if next.Data == nil {
			next.Data = map[string]string{}
		}
		next.Data[positionKey] = string(body)
		updated, uerr := s.Client.CoreV1().ConfigMaps(s.Namespace).Update(ctx, next, metav1.UpdateOptions{})
		if uerr == nil {
			s.cached = updated
			return nil
		}
		if !apierrors.IsConflict(uerr) && !apierrors.IsNotFound(uerr) {
			return uerr
		}
		s.cached = nil // re-read on the retry
		err = uerr
	}
	return err
}
