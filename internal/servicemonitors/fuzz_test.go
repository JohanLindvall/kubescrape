package servicemonitors

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// FuzzParse feeds arbitrary JSON (decoded into the unstructured object shape a
// dynamic informer would deliver) to Parse. Invariant: never panics; either
// returns an error or a Monitor whose ServiceNamespaces() is callable and
// whose selector is non-nil.
func FuzzParse(f *testing.F) {
	seeds := []string{
		`{"metadata":{"namespace":"monitoring","name":"m1"},"spec":{"selector":{"matchLabels":{"app":"web"}},"namespaceSelector":{"matchNames":["a","b"]},"endpoints":[{"port":"http","path":"/metrics","scheme":"https"}]}}`,
		`{"metadata":{"namespace":"ns","name":"n"},"spec":{"selector":{"matchExpressions":[{"key":"k","operator":"In","values":["v"]}]},"namespaceSelector":{"any":true},"endpoints":[{"targetPort":8080},{"targetPort":"named"}]}}`,
		`{"spec":{}}`,
		`{"spec":{"selector":{"matchExpressions":[{"key":"k","operator":"BadOp"}]}}}`,
		`{"spec":{"endpoints":[{"port":123}]}}`, // wrong type for port
		`{"spec":{"selector":"not-an-object"}}`,
		`{"spec":"not-an-object"}`,
		`{}`,
		`{"spec":{"endpoints":"not-a-list"}}`,
		`{"spec":{"namespaceSelector":{"any":"notabool"}}}`,
		`{"spec":{"selector":{"matchLabels":{"k":123}}}}`,
		`{"spec":{"endpoints":[{"targetPort":{"nested":"object"}}]}}`,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		var obj map[string]any
		if err := json.Unmarshal(data, &obj); err != nil {
			t.Skip() // only structurally valid JSON reaches an informer
		}
		u := &unstructured.Unstructured{Object: obj}
		m, err := Parse(u)
		if err != nil {
			return
		}
		if m.Selector == nil {
			t.Fatalf("Parse returned a monitor with a nil selector for %q", data)
		}
		_ = m.ServiceNamespaces() // must not panic
		for _, ep := range m.Endpoints {
			_ = ep.Port
			_ = ep.TargetPort
		}
	})
}

// FuzzIndexUpsert exercises the whole Index lifecycle (Upsert parses, Delete,
// All) against fuzzed objects so the concurrency-free store paths never panic
// on malformed input.
func FuzzIndexUpsert(f *testing.F) {
	f.Add([]byte(`{"metadata":{"namespace":"n","name":"a"},"spec":{"selector":{}}}`))
	f.Add([]byte(`{"spec":{"selector":{"matchExpressions":[{"key":"k","operator":"Bad"}]}}}`))
	f.Add([]byte(`{}`))
	ix := NewIndex()
	f.Fuzz(func(t *testing.T, data []byte) {
		var obj map[string]any
		if err := json.Unmarshal(data, &obj); err != nil {
			t.Skip()
		}
		u := &unstructured.Unstructured{Object: obj}
		if err := ix.Upsert(u); err != nil {
			return
		}
		_ = ix.All()
		ix.Delete(u.GetNamespace(), u.GetName())
	})
}
