package scrape

// merge_guard_test.go holds MergeMonitorEndpoint's hand-written field lists —
// the bare gate and the authMaterial trio — to the one classification in
// merge.go (endpointMergeClass). TestEveryEndpointFieldReachesTheTarget forces
// a new Endpoint field into stampEndpoint; nothing but this file forces it
// into the merge, and a mergeable field the gate cannot see is silently
// dropped on every URL two monitors share.

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/servicemonitors"
	"github.com/JohanLindvall/kubescrape/pkg/kubemeta"
)

// Every servicemonitors.Endpoint field must be classified — in exactly one
// class, by map construction — and every classified name must still be a
// field, so a rename cannot leave a stale entry vouching for nothing.
func TestEveryEndpointFieldIsClassifiedForTheMerge(t *testing.T) {
	typ := reflect.TypeOf(servicemonitors.Endpoint{})
	fields := map[string]bool{}
	for i := range typ.NumField() {
		name := typ.Field(i).Name
		fields[name] = true
		if _, ok := endpointMergeClass[name]; !ok {
			t.Errorf("Endpoint.%s is not classified in merge.go's endpointMergeClass: add it there — "+
				"inertClass only if two endpoints resolving to ONE URL cannot meaningfully differ on it; "+
				"a mergeable class also needs the bare gate in MergeMonitorEndpoint extended, and auth "+
				"material needs authMaterial/endpointAuth/targetAuth/stampAuth too", name)
		}
	}
	for name := range endpointMergeClass {
		if !fields[name] {
			t.Errorf("endpointMergeClass names %q, which is not a servicemonitors.Endpoint field (renamed or removed?)", name)
		}
	}
}

// authMaterial's width must match the declared auth class: a field added to
// authMaterial without being declared (or declared without a carrier) is
// either dead material or a bare-gate hole. Value flow through the trio is
// pinned by the round trip in TestMergeHonoursEveryClassifiedField.
func TestAuthMaterialMatchesTheDeclaredAuthClass(t *testing.T) {
	declared := 0
	for _, class := range endpointMergeClass {
		if class == authClass {
			declared++
		}
	}
	if got := reflect.TypeOf(authMaterial{}).NumField(); got != declared {
		t.Errorf("authMaterial has %d fields but endpointMergeClass declares %d authClass fields: "+
			"extend the group in both places (and in endpointAuth/targetAuth/stampAuth)", got, declared)
	}
}

// endpointCarryingOnly fabricates an endpoint whose ONLY set field is the
// named one — the shape a merge-gate omission turns into silent loss.
func endpointCarryingOnly(t *testing.T, field string) *servicemonitors.Endpoint {
	t.Helper()
	ep := &servicemonitors.Endpoint{}
	setEndpointField(t, ep, field, "sentinel-"+field)
	return ep
}

// setEndpointField handles the kinds Endpoint carries today; a field of a new
// kind must extend this rather than dodge the guard.
func setEndpointField(t *testing.T, ep *servicemonitors.Endpoint, field, sentinel string) {
	t.Helper()
	v := reflect.ValueOf(ep).Elem().FieldByName(field)
	if !v.IsValid() {
		t.Fatalf("Endpoint has no field %q", field)
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(sentinel)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
	default:
		t.Fatalf("Endpoint.%s has kind %s this guard cannot fabricate a value for: extend setEndpointField", field, v.Kind())
	}
}

func targetCarriesString(tgt kubemeta.ScrapeTarget, sentinel string) bool {
	v := reflect.ValueOf(tgt)
	for i := range v.NumField() {
		if f := v.Field(i); f.Kind() == reflect.String && strings.Contains(f.String(), sentinel) {
			return true
		}
	}
	return false
}

// For each classified field, an endpoint carrying ONLY that field must get
// what its class declares: mergeable fields pass the bare gate, contribute,
// and land on the target (auth via the endpointAuth → stampAuth → targetAuth
// round trip, adopted one-sided); inert fields read as bare and leave the
// holder untouched. A mergeable field the gate misses fails HERE, not as
// up=0 in production.
func TestMergeHonoursEveryClassifiedField(t *testing.T) {
	for field, class := range endpointMergeClass {
		t.Run(field, func(t *testing.T) {
			ep := endpointCarryingOnly(t, field)
			held := mergeHeld("ns/a", servicemonitors.Endpoint{})
			if class == inertClass {
				want := held
				rep := MergeMonitorEndpoint(&held, "ns/b", ep)
				adopted, conflict := rep.AuthAdopted, rep.AuthConflict
				if adopted || conflict || held.Monitors != nil || !reflect.DeepEqual(held, want) {
					t.Fatalf("Endpoint.%s is classified inert but the merge acted on it: adopted=%v conflict=%v target=%+v",
						field, adopted, conflict, held)
				}
				return
			}
			if class == authClass && endpointAuth(ep) == (authMaterial{}) {
				t.Fatalf("Endpoint.%s is classified auth material but endpointAuth does not carry it: an endpoint "+
					"carrying only it reads as BARE and the declaration is dropped on any shared URL "+
					"(extend authMaterial/endpointAuth/targetAuth/stampAuth in merge.go)", field)
			}
			rep := MergeMonitorEndpoint(&held, "ns/b", ep)
			adopted, conflict := rep.AuthAdopted, rep.AuthConflict
			if conflict {
				t.Fatalf("Endpoint.%s carried alone conflicted against an empty holder", field)
			}
			if want := []string{"ns/a", "ns/b"}; !slices.Equal(held.Monitors, want) {
				t.Fatalf("Endpoint.%s is classified mergeable but did not contribute (monitors %v): the bare gate "+
					"in MergeMonitorEndpoint cannot see it — add an arm beside the other mergeable classes", field, held.Monitors)
			}
			switch class {
			case relabelClass:
				if len(held.MetricRelabelings) == 0 {
					t.Fatalf("Endpoint.%s did not land on the target's relabel chain", field)
				}
			case cadenceClass:
				if !targetCarriesString(held, "sentinel-"+field) {
					t.Fatalf("Endpoint.%s did not land on the target: %+v", field, held)
				}
			case authClass:
				if !adopted {
					t.Fatalf("Endpoint.%s: one-sided auth material was not reported adopted", field)
				}
				if got, want := targetAuth(&held), endpointAuth(ep); got != want {
					t.Fatalf("Endpoint.%s was lost in the adopt round trip (endpointAuth → stampAuth → targetAuth): "+
						"got %+v want %+v", field, got, want)
				}
			}
		})
	}
}

// A holder and an endpoint differing in any SINGLE declared auth field must
// conflict with the holder's group kept whole — the outcome a field present in
// endpointAuth but missing from targetAuth would corrupt into a re-adoption.
func TestEachDeclaredAuthFieldConflictsWhenDiffering(t *testing.T) {
	epType := reflect.TypeOf(servicemonitors.Endpoint{})
	for field, class := range endpointMergeClass {
		if class != authClass {
			continue
		}
		if f, ok := epType.FieldByName(field); !ok || f.Type.Kind() != reflect.String {
			// A zero bool cannot express a second, differing one-field
			// endpoint; InsecureSkipVerify's conflict arm is pinned in
			// TestMergeAuthIdenticalKeepsConflictingReports.
			continue
		}
		t.Run(field, func(t *testing.T) {
			ep := endpointCarryingOnly(t, field)
			held := mergeHeld("ns/a", *ep)
			ep2 := &servicemonitors.Endpoint{}
			setEndpointField(t, ep2, field, "sentinel-2-"+field)
			rep := MergeMonitorEndpoint(&held, "ns/b", ep2)
			adopted, conflict := rep.AuthAdopted, rep.AuthConflict
			if adopted || !conflict {
				t.Fatalf("Endpoint.%s differing: adopted=%v conflict=%v, want the conflict reported", field, adopted, conflict)
			}
			if got, want := targetAuth(&held), endpointAuth(ep); got != want {
				t.Fatalf("Endpoint.%s: the holder's auth group was not kept whole: got %+v want %+v", field, got, want)
			}
		})
	}
}
