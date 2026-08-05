package otlpexport

import (
	"reflect"
	"testing"
)

// Every field of Config is classified as either TRANSPORT (inherited by
// TransportOnly) or DESTINATION-scoped (zeroed by it). A new Config field
// fails this test until someone puts it in one of the two lists — which is the
// whole point: the destination side carries credentials and trust decisions,
// and a field that drifts into being inherited by accident is how a collector
// token gets presented to a different host.
var (
	transportFields = map[string]bool{
		"Protocol":         true,
		"Compression":      true,
		"CompressionLevel": true,
		"Timeout":          true,
		"RetryAttempts":    true,
		"RetryBackoff":     true,
		"MaxSendBytes":     true,
	}
	destinationFields = map[string]bool{
		"Endpoint":           true,
		"Insecure":           true,
		"InsecureSkipVerify": true,
		"CAFile":             true,
		"ClientCertFile":     true,
		"ClientKeyFile":      true,
		"Headers":            true,
		"BearerTokenFile":    true,
	}
)

// fillConfig sets every field of a Config to a distinctive non-zero value.
func fillConfig(t *testing.T) Config {
	t.Helper()
	var c Config
	v := reflect.ValueOf(&c).Elem()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		name := v.Type().Field(i).Name
		switch f.Kind() {
		case reflect.String:
			f.SetString("x-" + name)
		case reflect.Int, reflect.Int64: // time.Duration is Int64
			f.SetInt(7)
		case reflect.Bool:
			f.SetBool(true)
		case reflect.Map:
			f.Set(reflect.MakeMap(f.Type()))
			f.SetMapIndex(reflect.ValueOf("k"), reflect.ValueOf("v"))
		default:
			t.Fatalf("Config.%s has kind %s fillConfig cannot populate; extend it", name, f.Kind())
		}
	}
	return c
}

func TestConfigFieldsAreClassified(t *testing.T) {
	full := fillConfig(t)
	got := reflect.ValueOf(full.TransportOnly())
	src := reflect.ValueOf(full)
	typ := src.Type()

	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		inTransport := transportFields[name]
		inDest := destinationFields[name]
		switch {
		case inTransport && inDest:
			t.Errorf("Config.%s is in BOTH lists; pick one", name)
		case !inTransport && !inDest:
			t.Errorf("Config.%s is UNCLASSIFIED: decide whether it is transport tuning (inherited by TransportOnly) or destination-scoped (credentials/TLS/headers/endpoint — zeroed), and add it to the matching list in this test AND, if transport, to TransportOnly", name)
		case inTransport:
			if !reflect.DeepEqual(got.Field(i).Interface(), src.Field(i).Interface()) {
				t.Errorf("Config.%s is classified transport but TransportOnly does not carry it (got %v, want %v)",
					name, got.Field(i).Interface(), src.Field(i).Interface())
			}
		case inDest:
			if !got.Field(i).IsZero() {
				t.Errorf("Config.%s is destination-scoped but TransportOnly carries it (%v) — that is the credential-leak shape the partition exists to prevent",
					name, got.Field(i).Interface())
			}
		}
	}
}
