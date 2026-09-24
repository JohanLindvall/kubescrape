package attrs

import (
	"reflect"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// The pipeline set is spelled three times — pipelineNames, the Builders
// fields, and NewBuilders' assignment table — and a drift between them is
// SILENT in one direction: a *Builder field added and wired into a pipeline
// but missing from the other two stays nil, and Build on a nil receiver means
// defaults only — no static attributes, no templates, no instance prefix, no
// enable/disable filter — for that whole pipeline, with nothing reporting it.
//
// So every *Builder field must be assigned, by the pipeline its own name
// spells: each pipeline section below carries a static attribute naming it, and
// the field's builder must stamp exactly that name (a swapped assignment
// passes a nil check and fails this).
func TestEveryBuildersFieldIsAssignedItsOwnPipeline(t *testing.T) {
	cfg := &Config{Pipelines: map[string]*Config{}}
	for _, name := range pipelineNames {
		cfg.Pipelines[name] = &Config{Static: map[string]string{"test.pipeline": name}}
	}
	b, err := NewBuilders(cfg, nil)
	if err != nil {
		t.Fatalf("NewBuilders: %v", err)
	}
	v := reflect.ValueOf(b).Elem()
	builderType := reflect.TypeFor[*Builder]()
	fields := 0
	for i := 0; i < v.NumField(); i++ {
		f := v.Type().Field(i)
		if f.Type != builderType {
			continue
		}
		fields++
		pb := v.Field(i).Interface().(*Builder)
		if pb == nil {
			t.Errorf("Builders.%s is never assigned by NewBuilders: its pipeline would silently build defaults only", f.Name)
			continue
		}
		res := pcommon.NewResource()
		pb.Build(res, Context{})
		got, _ := res.Attributes().Get("test.pipeline")
		if want := strings.ToLower(f.Name); got.Str() != want {
			t.Errorf("Builders.%s builds with the %q pipeline's config, want %q", f.Name, got.Str(), want)
		}
	}
	if fields != len(pipelineNames) {
		t.Errorf("Builders has %d *Builder fields but pipelineNames lists %d pipelines", fields, len(pipelineNames))
	}
}
