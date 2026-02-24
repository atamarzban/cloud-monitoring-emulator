package validation

import (
	"testing"

	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
)

func validTimeSeries() *monitoringpb.TimeSeries {
	return &monitoringpb.TimeSeries{
		Metric:   &metric.Metric{Type: "custom.googleapis.com/test"},
		Resource: &monitoredres.MonitoredResource{Type: "global"},
		Points: []*monitoringpb.Point{
			{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.Now()},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
			},
		},
	}
}

func TestValidateCreateTimeSeriesRequest(t *testing.T) {
	tests := []struct {
		name     string
		req      *monitoringpb.CreateTimeSeriesRequest
		wantCode codes.Code
	}{
		{
			name: "valid",
			req: &monitoringpb.CreateTimeSeriesRequest{
				Name:       "projects/test-project",
				TimeSeries: []*monitoringpb.TimeSeries{validTimeSeries()},
			},
			wantCode: codes.OK,
		},
		{
			name:     "missing name",
			req:      &monitoringpb.CreateTimeSeriesRequest{TimeSeries: []*monitoringpb.TimeSeries{validTimeSeries()}},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "missing timeSeries",
			req:      &monitoringpb.CreateTimeSeriesRequest{Name: "projects/p"},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "too many time series",
			req: func() *monitoringpb.CreateTimeSeriesRequest {
				ts := make([]*monitoringpb.TimeSeries, 201)
				for i := range ts {
					ts[i] = validTimeSeries()
				}
				return &monitoringpb.CreateTimeSeriesRequest{Name: "projects/p", TimeSeries: ts}
			}(),
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing metric",
			req: &monitoringpb.CreateTimeSeriesRequest{
				Name: "projects/p",
				TimeSeries: []*monitoringpb.TimeSeries{
					{
						Resource: &monitoredres.MonitoredResource{Type: "global"},
						Points: []*monitoringpb.Point{
							{
								Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.Now()},
								Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
							},
						},
					},
				},
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "two points",
			req: &monitoringpb.CreateTimeSeriesRequest{
				Name: "projects/p",
				TimeSeries: []*monitoringpb.TimeSeries{
					{
						Metric:   &metric.Metric{Type: "custom.googleapis.com/test"},
						Resource: &monitoredres.MonitoredResource{Type: "global"},
						Points: []*monitoringpb.Point{
							{Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.Now()}, Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}}},
							{Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.Now()}, Value: &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 2.0}}},
						},
					},
				},
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCreateTimeSeriesRequest(tt.req)
			if status.Code(err) != tt.wantCode {
				t.Errorf("got code %v, want %v (err=%v)", status.Code(err), tt.wantCode, err)
			}
		})
	}
}

func TestValidateMetricDescriptor(t *testing.T) {
	tests := []struct {
		name     string
		md       *metric.MetricDescriptor
		wantCode codes.Code
	}{
		{
			name: "valid custom",
			md: &metric.MetricDescriptor{
				Type:       "custom.googleapis.com/test",
				MetricKind: metric.MetricDescriptor_GAUGE,
				ValueType:  metric.MetricDescriptor_DOUBLE,
			},
			wantCode: codes.OK,
		},
		{
			name: "valid external",
			md: &metric.MetricDescriptor{
				Type:       "external.googleapis.com/test",
				MetricKind: metric.MetricDescriptor_GAUGE,
				ValueType:  metric.MetricDescriptor_DOUBLE,
			},
			wantCode: codes.OK,
		},
		{
			name: "invalid prefix",
			md: &metric.MetricDescriptor{
				Type:       "compute.googleapis.com/test",
				MetricKind: metric.MetricDescriptor_GAUGE,
				ValueType:  metric.MetricDescriptor_DOUBLE,
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing type",
			md: &metric.MetricDescriptor{
				MetricKind: metric.MetricDescriptor_GAUGE,
				ValueType:  metric.MetricDescriptor_DOUBLE,
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing metric kind",
			md: &metric.MetricDescriptor{
				Type:      "custom.googleapis.com/test",
				ValueType: metric.MetricDescriptor_DOUBLE,
			},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "missing value type",
			md: &metric.MetricDescriptor{
				Type:       "custom.googleapis.com/test",
				MetricKind: metric.MetricDescriptor_GAUGE,
			},
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateMetricDescriptor(tt.md)
			if status.Code(err) != tt.wantCode {
				t.Errorf("got code %v, want %v (err=%v)", status.Code(err), tt.wantCode, err)
			}
		})
	}
}

func TestParseProjectFromName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{"simple", "projects/my-project", "my-project", false},
		{"with suffix", "projects/my-project/timeSeries", "my-project", false},
		{"empty project", "projects/", "", true},
		{"no prefix", "organizations/123", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseProjectFromName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("err=%v, wantErr=%v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseMetricDescriptorName(t *testing.T) {
	tests := []struct {
		name       string
		input      string
		wantProj   string
		wantType   string
		wantErr    bool
	}{
		{
			"valid",
			"projects/my-proj/metricDescriptors/custom.googleapis.com/my_metric",
			"my-proj", "custom.googleapis.com/my_metric", false,
		},
		{"invalid", "projects/my-proj/timeSeries", "", "", true},
		{"no prefix", "foobar", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proj, mt, err := ParseMetricDescriptorName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("err=%v, wantErr=%v", err, tt.wantErr)
			}
			if proj != tt.wantProj || mt != tt.wantType {
				t.Errorf("got (%q, %q), want (%q, %q)", proj, mt, tt.wantProj, tt.wantType)
			}
		})
	}
}
