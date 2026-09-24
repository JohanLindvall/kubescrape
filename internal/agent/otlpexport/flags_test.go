package otlpexport

import (
	"flag"
	"reflect"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/cli"
)

// Every flag of the shared block lands on the Config field of the same name:
// a field added to cli.OTLPFlags and not mapped here would otherwise be a flag
// both binaries accept and neither exporter reads.
func TestConfigFromFlagsMapsEveryFlag(t *testing.T) {
	fs := flag.NewFlagSet("otlp", flag.ContinueOnError)
	f := cli.RegisterOTLPFlags(fs, "")
	for name, val := range map[string]string{
		"otlp-endpoint": "collector:4317", "otlp-protocol": "http", "otlp-compression": "none",
		"otlp-compression-level": "7", "otlp-insecure": "false", "otlp-tls-insecure-skip-verify": "true",
		"otlp-tls-ca-file": "/ca.pem", "otlp-bearer-token-file": "/token", "otlp-timeout": "3s",
	} {
		if err := fs.Set(name, val); err != nil {
			t.Fatal(err)
		}
	}
	cfg := reflect.ValueOf(ConfigFromFlags(f))
	flags := reflect.ValueOf(*f)
	for i := 0; i < flags.NumField(); i++ {
		name := flags.Type().Field(i).Name
		got := cfg.FieldByName(name)
		if !got.IsValid() {
			t.Errorf("cli.OTLPFlags.%s has no Config field of that name", name)
			continue
		}
		if want := flags.Field(i).Elem().Interface(); !reflect.DeepEqual(got.Interface(), want) {
			t.Errorf("Config.%s = %v, want the flag's %v", name, got.Interface(), want)
		}
	}
}
