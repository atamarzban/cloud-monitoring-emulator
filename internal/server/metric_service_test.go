package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
)

func newTestServer() *MetricServiceServer {
	return NewMetricServiceServer(store.NewMemoryStore())
}

func TestListMonitoredResourceDescriptors(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	resp, err := s.ListMonitoredResourceDescriptors(ctx, &monitoringpb.ListMonitoredResourceDescriptorsRequest{
		Name: "projects/test-project",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.ResourceDescriptors) == 0 {
		t.Error("expected seeded resource descriptors")
	}
}

func TestGetMonitoredResourceDescriptor(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	resp, err := s.GetMonitoredResourceDescriptor(ctx, &monitoringpb.GetMonitoredResourceDescriptorRequest{
		Name: "projects/test-project/monitoredResourceDescriptors/global",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Type != "global" {
		t.Errorf("got type %q, want global", resp.Type)
	}

	_, err = s.GetMonitoredResourceDescriptor(ctx, &monitoringpb.GetMonitoredResourceDescriptorRequest{
		Name: "projects/test-project/monitoredResourceDescriptors/nonexistent",
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

func TestCreateGetDeleteMetricDescriptor(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	// Create
	created, err := s.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
		Name: "projects/test-project",
		MetricDescriptor: &metric.MetricDescriptor{
			Type:       "custom.googleapis.com/test_metric",
			MetricKind: metric.MetricDescriptor_GAUGE,
			ValueType:  metric.MetricDescriptor_DOUBLE,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Name != "projects/test-project/metricDescriptors/custom.googleapis.com/test_metric" {
		t.Errorf("unexpected name: %s", created.Name)
	}

	// Get
	got, err := s.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{Name: created.Name})
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != "custom.googleapis.com/test_metric" {
		t.Errorf("got type %q", got.Type)
	}

	// Delete
	_, err = s.DeleteMetricDescriptor(ctx, &monitoringpb.DeleteMetricDescriptorRequest{Name: created.Name})
	if err != nil {
		t.Fatal(err)
	}

	// Get after delete
	_, err = s.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{Name: created.Name})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

func TestCreateMetricDescriptorValidation(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	// Missing type prefix
	_, err := s.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
		Name: "projects/p",
		MetricDescriptor: &metric.MetricDescriptor{
			Type:       "compute.googleapis.com/invalid",
			MetricKind: metric.MetricDescriptor_GAUGE,
			ValueType:  metric.MetricDescriptor_DOUBLE,
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for bad prefix, got %v", err)
	}

	// Missing metric descriptor
	_, err = s.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
		Name: "projects/p",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for nil descriptor, got %v", err)
	}
}

func TestListMetricDescriptorsPagination(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	for i := 0; i < 25; i++ {
		s.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
			Name: "projects/p",
			MetricDescriptor: &metric.MetricDescriptor{
				Type:       fmt.Sprintf("custom.googleapis.com/m%02d", i),
				MetricKind: metric.MetricDescriptor_GAUGE,
				ValueType:  metric.MetricDescriptor_INT64,
			},
		})
	}

	// First page
	resp, err := s.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{
		Name:     "projects/p",
		PageSize: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.MetricDescriptors) != 10 {
		t.Errorf("got %d, want 10", len(resp.MetricDescriptors))
	}
	if resp.NextPageToken == "" {
		t.Error("expected next page token")
	}

	// Second page
	resp2, err := s.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{
		Name:      "projects/p",
		PageSize:  10,
		PageToken: resp.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp2.MetricDescriptors) != 10 {
		t.Errorf("got %d, want 10", len(resp2.MetricDescriptors))
	}

	// Third page (last 5)
	resp3, err := s.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{
		Name:      "projects/p",
		PageSize:  10,
		PageToken: resp2.NextPageToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp3.MetricDescriptors) != 5 {
		t.Errorf("got %d, want 5", len(resp3.MetricDescriptors))
	}
	if resp3.NextPageToken != "" {
		t.Error("expected empty next page token on last page")
	}
}

func TestCreateTimeSeries(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	_, err := s.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/p",
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric:     &metric.Metric{Type: "custom.googleapis.com/cpu", Labels: map[string]string{"env": "prod"}},
				Resource:   &monitoredres.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": "p"}},
				MetricKind: metric.MetricDescriptor_GAUGE,
				ValueType:  metric.MetricDescriptor_DOUBLE,
				Points: []*monitoringpb.Point{
					{
						Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
						Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 42.0}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify data can be read back.
	resp, err := s.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:   "projects/p",
		Filter: `metric.type = "custom.googleapis.com/cpu"`,
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(now.Add(-time.Hour)),
			EndTime:   timestamppb.New(now.Add(time.Hour)),
		},
		View: monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.TimeSeries) != 1 {
		t.Fatalf("got %d series, want 1", len(resp.TimeSeries))
	}
	if len(resp.TimeSeries[0].Points) != 1 {
		t.Fatalf("got %d points, want 1", len(resp.TimeSeries[0].Points))
	}
	if resp.TimeSeries[0].Points[0].GetValue().GetDoubleValue() != 42.0 {
		t.Errorf("got %v, want 42.0", resp.TimeSeries[0].Points[0].GetValue().GetDoubleValue())
	}
}

func TestCreateTimeSeriesAutoCreatesDescriptor(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	// Write a time series for a metric that has no descriptor.
	_, err := s.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/p",
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric:   &metric.Metric{Type: "custom.googleapis.com/auto_created", Labels: map[string]string{"env": "prod"}},
				Resource: &monitoredres.MonitoredResource{Type: "global"},
				Points: []*monitoringpb.Point{
					{
						Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.Now()},
						Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: 100}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify descriptor was auto-created.
	md, err := s.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: "projects/p/metricDescriptors/custom.googleapis.com/auto_created",
	})
	if err != nil {
		t.Fatalf("auto-created descriptor not found: %v", err)
	}
	if md.MetricKind != metric.MetricDescriptor_GAUGE {
		t.Errorf("expected GAUGE, got %v", md.MetricKind)
	}
	if md.ValueType != metric.MetricDescriptor_INT64 {
		t.Errorf("expected INT64, got %v", md.ValueType)
	}
}

func TestCreateTimeSeriesValidation(t *testing.T) {
	s := newTestServer()
	ctx := context.Background()

	// Too many time series
	ts := make([]*monitoringpb.TimeSeries, 201)
	for i := range ts {
		ts[i] = &monitoringpb.TimeSeries{
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
	_, err := s.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name:       "projects/p",
		TimeSeries: ts,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for >200 time series, got %v", err)
	}

	// Missing filter on ListTimeSeries
	_, err = s.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name: "projects/p",
		Interval: &monitoringpb.TimeInterval{
			EndTime: timestamppb.Now(),
		},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for missing filter, got %v", err)
	}

	// Missing interval on ListTimeSeries
	_, err = s.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:   "projects/p",
		Filter: `metric.type = "foo"`,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for missing interval, got %v", err)
	}
}

func TestPaginate(t *testing.T) {
	tests := []struct {
		total     int
		pageSize  int32
		pageToken string
		wantLen   int
		wantNext  bool
	}{
		{10, 5, "", 5, true},
		{10, 5, "NQ==", 5, false}, // offset=5
		{10, 20, "", 10, false},
		{0, 5, "", 0, false},
		{10, 3, "", 3, true},
	}

	for _, tt := range tests {
		indices, next := paginate(tt.total, tt.pageSize, tt.pageToken)
		if len(indices) != tt.wantLen {
			t.Errorf("paginate(%d, %d, %q): got %d items, want %d", tt.total, tt.pageSize, tt.pageToken, len(indices), tt.wantLen)
		}
		hasNext := next != ""
		if hasNext != tt.wantNext {
			t.Errorf("paginate(%d, %d, %q): hasNext=%v, want %v", tt.total, tt.pageSize, tt.pageToken, hasNext, tt.wantNext)
		}
	}
}
