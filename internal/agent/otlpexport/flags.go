package otlpexport

import "github.com/JohanLindvall/kubescrape/internal/cli"

// ConfigFromFlags is the shared exporter flag block (cli.RegisterOTLPFlags) as
// a Config. Both binaries start from it and add only what is theirs — the
// agent its retry and send-size fields, the service its headers — so a flag
// added to the block reaches both exporters by construction instead of by two
// hand-written mappings agreeing.
func ConfigFromFlags(f *cli.OTLPFlags) Config {
	return Config{
		Endpoint:           *f.Endpoint,
		Protocol:           *f.Protocol,
		Compression:        *f.Compression,
		CompressionLevel:   *f.CompressionLevel,
		Insecure:           *f.Insecure,
		InsecureSkipVerify: *f.InsecureSkipVerify,
		CAFile:             *f.CAFile,
		BearerTokenFile:    *f.BearerTokenFile,
		Timeout:            *f.Timeout,
	}
}
