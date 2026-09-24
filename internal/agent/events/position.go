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
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/JohanLindvall/kubescrape/internal/obs"
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
	// who checkpointed and when. Updated is the last checkpoint that MOVED (or
	// a leadership term's first write, or a shutdown): Reader.persist does not
	// rewrite a position that has not changed, so it is not a heartbeat.
	Holder  string    `json:"holder,omitempty"`
	Updated time.Time `json:"updated"`
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
// error) — a cold start. An unparseable one is (found=false, error), and so is
// a failed Get: the caller counts it and, under `auto`, replays the backlog
// rather than taking the cold-start policy (see Reader.loadPosition) — an
// explicit start mode is honoured — exactly as the agent's positions store
// surfaces Corrupt() rather than letting an undecodable file masquerade as a
// first run.
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
	for range 2 {
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

// Op labels for obs.EventPositionErrors.
const (
	opLoad = "load"
	opSave = "save"
)

// loadPosition reads the stored resume point and applies the start policy.
func (r *Reader) loadPosition(ctx context.Context) {
	if r.cfg.Positions == nil {
		return
	}
	pos, found, err := r.cfg.Positions.Load(ctx)
	switch {
	case err != nil:
		// An unreadable position is not a first run, and it must not be
		// answered with the LOSS direction. Two failure classes arrive here in
		// ONE shape — the store makes a single Get, so an undecodable document
		// and a 503 / timeout / expired credential are indistinguishable — and
		// under `auto` the cold-start policy takes the CURRENT revision: one
		// failed Get during a control-plane roll (exactly when leadership
		// churns) discards every event since the predecessor stopped, and the
		// first persist then overwrites the only record of where that was.
		//
		// So replay the TTL backlog instead. It is unfiltered — no watermark
		// loaded — so it costs duplicates, which at-least-once already
		// tolerates, and it is what the sibling this store's doc claims parity
		// with does: the tailer's positions store reports Corrupt() so `auto`
		// RE-READS rather than skipping every file to its end as history. An
		// explicit `end` or `start` is the operator naming a cold-start policy
		// outright and is honoured.
		//
		// OR-ed, never assigned: one Reader serves every leadership term the
		// process wins, so a relist expire() armed in the previous term may
		// still be pending, and a failed Get must not DISARM it — under `end`
		// the next stream would then start at the CURRENT revision and discard
		// the gap since the expired one, uncounted. expire() states the same
		// rule from the other side.
		obs.EventPositionErrors.WithLabelValues(opLoad).Inc()
		r.relist = r.relist || r.cfg.StartMode == StartAuto
		r.log.Warn("event position unreadable", "error", err,
			"startMode", r.cfg.StartMode, "replayingBacklog", r.relist)
	case found:
		r.committed = pos
		r.log.Info("resuming events", "resourceVersion", pos.ResourceVersion, "watermark", pos.Watermark)
	}
}

// persist writes the position, rate-limited unless forced (shutdown).
//
// A position that has not MOVED since this leadership term last wrote it is
// not written again. ConfigMapStore.Save stamps Updated and always issues an
// Update, so every call is a new etcd revision; on a quiet cluster — where
// core/v1 event watches get no bookmarks and nothing advances the position —
// the ticker otherwise rewrote the same position every PersistInterval, ~8,640
// writes a day recording nothing. The first write of a term is always made, so
// Holder names the current leader, and so is the forced one at shutdown.
// Position.Updated therefore means "last checkpoint that moved", not "last
// write attempt".
func (r *Reader) persist(ctx context.Context, force bool) {
	if r.cfg.Positions == nil || r.committed.ResourceVersion == "" {
		return
	}
	now := r.now()
	if !force && now.Sub(r.lastPersist) < r.cfg.PersistInterval {
		return
	}
	if !force && r.savedThisTerm && r.lastSaved.ResourceVersion == r.committed.ResourceVersion &&
		r.lastSaved.Watermark.Equal(r.committed.Watermark) {
		return
	}
	r.lastPersist = now
	pos := r.committed
	pos.Holder = hostname()
	if err := r.cfg.Positions.Save(ctx, pos); err != nil {
		obs.EventPositionErrors.WithLabelValues(opSave).Inc()
		// The transition-warn shape (cmd/kubescrape/apiserver.go): the write
		// retries every PersistInterval, so an unwritable ConfigMap — a lost
		// RBAC rule, an API server that is down — would otherwise be one line
		// every ten seconds for the length of the outage. The FIRST failure is
		// what an operator needs; the rest is the counter's job.
		if _, loud := r.persistOutage.Fail(now, positionWarnEvery); loud {
			r.log.Warn("writing the event position failed; a restart or leader handover will resume from the last position that was written, replaying what has happened since",
				"error", err, "eventsPositionConfigmap", r.positionRef(), "resourceVersion", pos.ResourceVersion)
		}
		return
	}
	r.lastSaved, r.savedThisTerm = r.committed, true
	if failures, lasted, ok := r.persistOutage.Recover(now); ok {
		r.log.Info("writing the event position recovered", "resourceVersion", pos.ResourceVersion,
			"failures", failures, "outage", lasted)
	}
	// The position is the one piece of this pipeline's state that outlives the
	// process, and "is it advancing?" is the first question a handover raises.
	// Debug, not Info: it writes every PersistInterval forever.
	r.log.Debug("event position written", "resourceVersion", pos.ResourceVersion,
		"watermark", pos.Watermark, "forced", force)
}

// positionWarnEvery re-warns about an unwritable position at this cadence.
const positionWarnEvery = 5 * time.Minute

// positionRef names the ConfigMap the position lives in, for a log line that
// has to be actionable without the operator knowing the flag defaults. It is a
// best-effort read of the store's own fields: a test store is not one.
func (r *Reader) positionRef() string {
	if s, ok := r.cfg.Positions.(*ConfigMapStore); ok {
		return s.Namespace + "/" + s.Name
	}
	return ""
}

func hostname() string {
	h, _ := osHostname()
	return h
}

// osHostname is a variable so tests can pin the holder field.
var osHostname = func() (string, error) {
	return os.Hostname()
}
