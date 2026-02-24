package filter

import (
	"testing"

	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/timestamppb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
)

func TestLexer(t *testing.T) {
	tests := []struct {
		input    string
		wantToks int
		wantErr  bool
	}{
		{`metric.type = "foo"`, 4, false},                    // IDENT EQ STRING EOF
		{`metric.type = "foo" AND resource.type = "bar"`, 8, false}, // 3 + AND + 3 + EOF
		{`NOT metric.type = "x"`, 5, false},
		{`metric.labels.env = one_of("a", "b")`, 9, false},
		{`"unterminated`, 0, true},
	}

	for _, tt := range tests {
		tokens, err := lex(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("lex(%q): err=%v, wantErr=%v", tt.input, err, tt.wantErr)
			continue
		}
		if err == nil && len(tokens) != tt.wantToks {
			t.Errorf("lex(%q): got %d tokens, want %d: %v", tt.input, len(tokens), tt.wantToks, tokens)
		}
	}
}

func TestParseBasic(t *testing.T) {
	tests := []struct {
		input   string
		wantErr bool
	}{
		{`metric.type = "custom.googleapis.com/foo"`, false},
		{`metric.type != "custom.googleapis.com/foo"`, false},
		{`metric.type : "custom"`, false},
		{`metric.type = "a" AND resource.type = "b"`, false},
		{`metric.type = "a" OR resource.type = "b"`, false},
		{`NOT metric.type = "a"`, false},
		{`(metric.type = "a" OR resource.type = "b") AND metric.labels.env = "prod"`, false},
		{`metric.labels.zone = starts_with("us-")`, false},
		{`metric.labels.region = one_of("us-east1", "us-west1")`, false},
		{`metric.type = monitoring.regex.full_match("custom\\..*")`, false},
		{``, false},         // empty filter is valid (returns nil)
		{`= "foo"`, true},  // missing field
		{`metric.type`, true}, // missing operator
	}

	for _, tt := range tests {
		expr, err := Parse(tt.input)
		if (err != nil) != tt.wantErr {
			t.Errorf("Parse(%q): err=%v, wantErr=%v", tt.input, err, tt.wantErr)
		}
		if tt.input == "" && expr != nil {
			t.Error("Parse empty string should return nil")
		}
	}
}

func makeTS(metricType string, metricLabels map[string]string, resourceType string, resourceLabels map[string]string) *monitoringpb.TimeSeries {
	return &monitoringpb.TimeSeries{
		Metric:   &metric.Metric{Type: metricType, Labels: metricLabels},
		Resource: &monitoredres.MonitoredResource{Type: resourceType, Labels: resourceLabels},
		Points: []*monitoringpb.Point{
			{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.Now()},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
			},
		},
	}
}

func TestEvalTimeSeries(t *testing.T) {
	ts := makeTS(
		"custom.googleapis.com/cpu",
		map[string]string{"env": "prod", "zone": "us-east1-b"},
		"gce_instance",
		map[string]string{"project_id": "my-proj", "instance_id": "12345"},
	)

	tests := []struct {
		filter string
		want   bool
	}{
		// Basic equality
		{`metric.type = "custom.googleapis.com/cpu"`, true},
		{`metric.type = "custom.googleapis.com/memory"`, false},
		{`metric.type != "custom.googleapis.com/memory"`, true},
		{`metric.type != "custom.googleapis.com/cpu"`, false},

		// Has (substring)
		{`metric.type : "cpu"`, true},
		{`metric.type : "memory"`, false},

		// Resource type
		{`resource.type = "gce_instance"`, true},
		{`resource.type = "global"`, false},

		// Metric labels
		{`metric.labels.env = "prod"`, true},
		{`metric.labels.env = "staging"`, false},
		{`metric.labels.zone = "us-east1-b"`, true},

		// Resource labels
		{`resource.labels.project_id = "my-proj"`, true},
		{`resource.labels.instance_id = "12345"`, true},

		// AND
		{`metric.type = "custom.googleapis.com/cpu" AND resource.type = "gce_instance"`, true},
		{`metric.type = "custom.googleapis.com/cpu" AND resource.type = "global"`, false},

		// OR
		{`metric.type = "custom.googleapis.com/cpu" OR metric.type = "custom.googleapis.com/memory"`, true},
		{`metric.type = "custom.googleapis.com/disk" OR metric.type = "custom.googleapis.com/memory"`, false},

		// NOT
		{`NOT metric.type = "custom.googleapis.com/memory"`, true},
		{`NOT metric.type = "custom.googleapis.com/cpu"`, false},

		// Functions
		{`metric.labels.zone = starts_with("us-")`, true},
		{`metric.labels.zone = starts_with("eu-")`, false},
		{`metric.labels.zone = ends_with("-b")`, true},
		{`metric.labels.zone = ends_with("-a")`, false},
		{`metric.labels.zone = has_substring("east1")`, true},
		{`metric.labels.zone = has_substring("west1")`, false},
		{`metric.labels.env = one_of("prod", "staging", "dev")`, true},
		{`metric.labels.env = one_of("staging", "dev")`, false},
		{`metric.type = monitoring.regex.full_match("custom\\.googleapis\\.com/.*")`, true},
		{`metric.type = monitoring.regex.full_match("compute\\.googleapis\\.com/.*")`, false},

		// Complex
		{`(metric.type = "custom.googleapis.com/cpu" OR metric.type = "custom.googleapis.com/memory") AND metric.labels.env = "prod"`, true},
		{`NOT (metric.labels.env = "staging" OR metric.labels.env = "dev")`, true},

		// Empty filter
		{``, true},
	}

	for _, tt := range tests {
		expr, err := Parse(tt.filter)
		if err != nil {
			t.Errorf("Parse(%q): %v", tt.filter, err)
			continue
		}
		got := MatchTimeSeries(expr, ts)
		if got != tt.want {
			t.Errorf("Match(%q): got %v, want %v", tt.filter, got, tt.want)
		}
	}
}

func TestEvalMetricDescriptor(t *testing.T) {
	md := &metric.MetricDescriptor{
		Type: "custom.googleapis.com/requests",
	}

	tests := []struct {
		filter string
		want   bool
	}{
		{`metric.type = "custom.googleapis.com/requests"`, true},
		{`metric.type = "custom.googleapis.com/other"`, false},
		{`metric.type = starts_with("custom.")`, true},
	}

	for _, tt := range tests {
		expr, err := Parse(tt.filter)
		if err != nil {
			t.Errorf("Parse(%q): %v", tt.filter, err)
			continue
		}
		got := MatchMetricDescriptor(expr, md)
		if got != tt.want {
			t.Errorf("MatchMetricDescriptor(%q): got %v, want %v", tt.filter, got, tt.want)
		}
	}
}

func TestParseErrors(t *testing.T) {
	badFilters := []string{
		`= "value"`,            // no field
		`metric.type`,          // no operator
		`metric.type = `,       // no value
		`metric.type = foo(`,   // unclosed function
		`(metric.type = "a"`,   // unclosed paren
		`metric.type = "a" XAND metric.type = "b"`, // invalid keyword
	}

	for _, f := range badFilters {
		_, err := Parse(f)
		if err == nil {
			t.Errorf("Parse(%q): expected error, got nil", f)
		}
	}
}
