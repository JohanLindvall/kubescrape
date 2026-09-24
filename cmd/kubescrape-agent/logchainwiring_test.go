package main

import (
	"reflect"
	"testing"

	"github.com/JohanLindvall/kubescrape/internal/agent/logscrub"
	"github.com/JohanLindvall/kubescrape/internal/logline"
	"github.com/JohanLindvall/kubescrape/internal/metrics"
	"github.com/JohanLindvall/kubescrape/pkg/logattrs"
)

// The tailer, journald, events and Azure take ONE logchain.Config, built once
// by pipelines.logChain. A lever added to logchain.Config that the construction
// forgets would reach none of the four, silently: every field must be set from
// the pipeline's compiled state.
func TestLogChainCarriesEveryLever(t *testing.T) {
	old := *enrichOn
	defer func() { *enrichOn = old }()
	*enrichOn = true
	p := &pipelines{
		scrub:      &logscrub.Scrubber{},
		logAttrs:   &logattrs.Extractor{},
		logMetrics: &metrics.DynamicMetricSet{},
		logRules:   &logline.LineFilter{},
	}
	v := reflect.ValueOf(p.logChain())
	for i := range v.NumField() {
		if v.Field(i).IsZero() {
			t.Errorf("pipelines.logChain leaves logchain.Config.%s unset: the tailer, journald, events and "+
				"Azure producers would never see it", v.Type().Field(i).Name)
		}
	}
}
