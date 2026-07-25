package logattrs

import "testing"

// A JSON integer beyond 2^53 must survive exactly. Decoding every number as
// float64 rounded 64-bit ids (snowflake, order/user ids) silently — and the
// result still looked like an exact integer downstream, because whole floats
// are stored with PutInt.
func TestLargeJSONIntegerKeepsPrecision(t *testing.T) {
	e, err := New(&Config{Rules: []Rule{
		{Key: "big"}, {Key: "neg"}, {Key: "small"}, {Key: "frac"}, {Key: "exp"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	res := e.Extract(`{"big":9007199254740993,"neg":-9007199254740993,"small":42,"frac":1.5,"exp":1e3}`)

	got := map[string]any{}
	for _, a := range res.Log {
		got[a.Key] = a.Val
	}
	if v, ok := got["big"].(int64); !ok || v != 9007199254740993 {
		t.Errorf("big = %#v, want int64 9007199254740993", got["big"])
	}
	if v, ok := got["neg"].(int64); !ok || v != -9007199254740993 {
		t.Errorf("neg = %#v, want int64 -9007199254740993", got["neg"])
	}
	if v, ok := got["small"].(int64); !ok || v != 42 {
		t.Errorf("small = %#v, want int64 42", got["small"])
	}
	// Non-integral tokens stay float64.
	if v, ok := got["frac"].(float64); !ok || v != 1.5 {
		t.Errorf("frac = %#v, want float64 1.5", got["frac"])
	}
	if v, ok := got["exp"].(float64); !ok || v != 1000 {
		t.Errorf("exp = %#v, want float64 1000", got["exp"])
	}
}
