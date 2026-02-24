package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/api/label"
	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
)

func TestCreateGetDeleteMetricDescriptor(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	md := &metric.MetricDescriptor{
		Type:        "custom.googleapis.com/test_metric",
		MetricKind:  metric.MetricDescriptor_GAUGE,
		ValueType:   metric.MetricDescriptor_DOUBLE,
		Description: "A test metric",
		Labels: []*label.LabelDescriptor{
			{Key: "env", ValueType: label.LabelDescriptor_STRING},
		},
	}

	// Create
	created, err := s.CreateMetricDescriptor(ctx, "test-project", md)
	if err != nil {
		t.Fatalf("CreateMetricDescriptor: %v", err)
	}
	if created.Name != "projects/test-project/metricDescriptors/custom.googleapis.com/test_metric" {
		t.Errorf("unexpected name: %s", created.Name)
	}
	if created.MetricKind != metric.MetricDescriptor_GAUGE {
		t.Errorf("unexpected metric kind: %v", created.MetricKind)
	}

	// Get
	got, err := s.GetMetricDescriptor(ctx, created.Name)
	if err != nil {
		t.Fatalf("GetMetricDescriptor: %v", err)
	}
	if got.Type != md.Type {
		t.Errorf("got type %q, want %q", got.Type, md.Type)
	}

	// Duplicate create
	_, err = s.CreateMetricDescriptor(ctx, "test-project", md)
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("expected AlreadyExists, got %v", err)
	}

	// Delete
	err = s.DeleteMetricDescriptor(ctx, created.Name)
	if err != nil {
		t.Fatalf("DeleteMetricDescriptor: %v", err)
	}

	// Get after delete
	_, err = s.GetMetricDescriptor(ctx, created.Name)
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound after delete, got %v", err)
	}

	// Delete non-existent
	err = s.DeleteMetricDescriptor(ctx, created.Name)
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound on second delete, got %v", err)
	}
}

func TestListMetricDescriptors(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		md := &metric.MetricDescriptor{
			Type:       fmt.Sprintf("custom.googleapis.com/metric_%d", i),
			MetricKind: metric.MetricDescriptor_GAUGE,
			ValueType:  metric.MetricDescriptor_INT64,
		}
		if _, err := s.CreateMetricDescriptor(ctx, "proj", md); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}

	// Create one in a different project.
	other := &metric.MetricDescriptor{
		Type:       "custom.googleapis.com/other",
		MetricKind: metric.MetricDescriptor_GAUGE,
		ValueType:  metric.MetricDescriptor_INT64,
	}
	if _, err := s.CreateMetricDescriptor(ctx, "other-proj", other); err != nil {
		t.Fatal(err)
	}

	list, err := s.ListMetricDescriptors(ctx, "proj", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 5 {
		t.Errorf("got %d descriptors, want 5", len(list))
	}
}

func TestMonitoredResourceDescriptors(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	// List should return seeded descriptors.
	list, err := s.ListMonitoredResourceDescriptors(ctx, "proj", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) == 0 {
		t.Fatal("expected seeded monitored resource descriptors, got none")
	}

	// Get "global".
	got, err := s.GetMonitoredResourceDescriptor(ctx, "projects/proj/monitoredResourceDescriptors/global")
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != "global" {
		t.Errorf("got type %q, want global", got.Type)
	}

	// Get non-existent.
	_, err = s.GetMonitoredResourceDescriptor(ctx, "projects/proj/monitoredResourceDescriptors/nonexistent")
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound, got %v", err)
	}
}

func makePoint(t time.Time, val float64) *monitoringpb.Point {
	return &monitoringpb.Point{
		Interval: &monitoringpb.TimeInterval{
			EndTime: timestamppb.New(t),
		},
		Value: &monitoringpb.TypedValue{
			Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: val},
		},
	}
}

func TestCreateAndListTimeSeries(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ts := []*monitoringpb.TimeSeries{
		{
			Metric:     &metric.Metric{Type: "custom.googleapis.com/cpu", Labels: map[string]string{"env": "prod"}},
			Resource:   &monitoredres.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": "proj"}},
			MetricKind: metric.MetricDescriptor_GAUGE,
			ValueType:  metric.MetricDescriptor_DOUBLE,
			Points:     []*monitoringpb.Point{makePoint(now, 42.0)},
		},
	}

	if err := s.CreateTimeSeries(ctx, "proj", ts); err != nil {
		t.Fatalf("CreateTimeSeries: %v", err)
	}

	// Write another point (later in time).
	ts[0].Points = []*monitoringpb.Point{makePoint(now.Add(time.Minute), 43.0)}
	if err := s.CreateTimeSeries(ctx, "proj", ts); err != nil {
		t.Fatalf("CreateTimeSeries second point: %v", err)
	}

	// List with matching filter.
	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(now.Add(-time.Hour)),
		EndTime:   timestamppb.New(now.Add(time.Hour)),
	}
	result, err := s.ListTimeSeries(ctx, "proj", `metric.type = "custom.googleapis.com/cpu"`, interval, monitoringpb.ListTimeSeriesRequest_FULL)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("got %d series, want 1", len(result))
	}
	if len(result[0].Points) != 2 {
		t.Errorf("got %d points, want 2", len(result[0].Points))
	}
	// Points should be in reverse time order.
	if result[0].Points[0].GetValue().GetDoubleValue() != 43.0 {
		t.Errorf("first point should be newest (43.0), got %v", result[0].Points[0].GetValue().GetDoubleValue())
	}

	// HEADERS view should have no points.
	headers, err := s.ListTimeSeries(ctx, "proj", `metric.type = "custom.googleapis.com/cpu"`, interval, monitoringpb.ListTimeSeriesRequest_HEADERS)
	if err != nil {
		t.Fatal(err)
	}
	if len(headers) != 1 {
		t.Fatalf("got %d series, want 1", len(headers))
	}
	if len(headers[0].Points) != 0 {
		t.Errorf("HEADERS view should return no points, got %d", len(headers[0].Points))
	}

	// Non-matching filter.
	noMatch, err := s.ListTimeSeries(ctx, "proj", `metric.type = "custom.googleapis.com/other"`, interval, monitoringpb.ListTimeSeriesRequest_FULL)
	if err != nil {
		t.Fatal(err)
	}
	if len(noMatch) != 0 {
		t.Errorf("expected 0 series for non-matching filter, got %d", len(noMatch))
	}
}

func TestCreateTimeSeriesChronologicalOrdering(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ts := []*monitoringpb.TimeSeries{
		{
			Metric:   &metric.Metric{Type: "custom.googleapis.com/test"},
			Resource: &monitoredres.MonitoredResource{Type: "global"},
			Points:   []*monitoringpb.Point{makePoint(now, 1.0)},
		},
	}

	if err := s.CreateTimeSeries(ctx, "proj", ts); err != nil {
		t.Fatal(err)
	}

	// Write a point at the same time (not after) — should fail.
	ts[0].Points = []*monitoringpb.Point{makePoint(now, 2.0)}
	err := s.CreateTimeSeries(ctx, "proj", ts)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for non-chronological point, got %v", err)
	}

	// Write a point earlier — should fail.
	ts[0].Points = []*monitoringpb.Point{makePoint(now.Add(-time.Minute), 3.0)}
	err = s.CreateTimeSeries(ctx, "proj", ts)
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument for earlier point, got %v", err)
	}
}

func TestTimeSeriesTimeRangeFiltering(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Write 5 points at 1-minute intervals.
	for i := 0; i < 5; i++ {
		ts := []*monitoringpb.TimeSeries{
			{
				Metric:   &metric.Metric{Type: "custom.googleapis.com/series"},
				Resource: &monitoredres.MonitoredResource{Type: "global"},
				Points:   []*monitoringpb.Point{makePoint(base.Add(time.Duration(i)*time.Minute), float64(i))},
			},
		}
		if err := s.CreateTimeSeries(ctx, "proj", ts); err != nil {
			t.Fatal(err)
		}
	}

	// Query for points in [minute 1, minute 3].
	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(base.Add(time.Minute)),
		EndTime:   timestamppb.New(base.Add(3 * time.Minute)),
	}
	result, err := s.ListTimeSeries(ctx, "proj", `metric.type = "custom.googleapis.com/series"`, interval, monitoringpb.ListTimeSeriesRequest_FULL)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Fatalf("got %d series, want 1", len(result))
	}
	if len(result[0].Points) != 3 {
		t.Errorf("got %d points, want 3 (minutes 1,2,3)", len(result[0].Points))
	}
}

func TestReset(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	md := &metric.MetricDescriptor{
		Type:       "custom.googleapis.com/reset_test",
		MetricKind: metric.MetricDescriptor_GAUGE,
		ValueType:  metric.MetricDescriptor_DOUBLE,
	}
	if _, err := s.CreateMetricDescriptor(ctx, "proj", md); err != nil {
		t.Fatal(err)
	}

	ts := []*monitoringpb.TimeSeries{
		{
			Metric:   &metric.Metric{Type: "custom.googleapis.com/reset_test"},
			Resource: &monitoredres.MonitoredResource{Type: "global"},
			Points:   []*monitoringpb.Point{makePoint(time.Now(), 1.0)},
		},
	}
	if err := s.CreateTimeSeries(ctx, "proj", ts); err != nil {
		t.Fatal(err)
	}

	s.Reset()

	// Metric descriptors should be gone.
	_, err := s.GetMetricDescriptor(ctx, "projects/proj/metricDescriptors/custom.googleapis.com/reset_test")
	if status.Code(err) != codes.NotFound {
		t.Error("expected NotFound after reset")
	}

	// Monitored resource descriptors should still exist.
	list, err := s.ListMonitoredResourceDescriptors(ctx, "proj", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(list) == 0 {
		t.Error("monitored resource descriptors should survive reset")
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Concurrent writes to different series (same project).
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ts := []*monitoringpb.TimeSeries{
				{
					Metric:   &metric.Metric{Type: fmt.Sprintf("custom.googleapis.com/concurrent_%d", i)},
					Resource: &monitoredres.MonitoredResource{Type: "global"},
					Points:   []*monitoringpb.Point{makePoint(base.Add(time.Duration(i)*time.Second), float64(i))},
				},
			}
			_ = s.CreateTimeSeries(ctx, "proj", ts)
		}(i)
	}

	// Concurrent reads while writes are happening.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			interval := &monitoringpb.TimeInterval{
				StartTime: timestamppb.New(base.Add(-time.Hour)),
				EndTime:   timestamppb.New(base.Add(time.Hour)),
			}
			_, _ = s.ListTimeSeries(ctx, "proj", "", interval, monitoringpb.ListTimeSeriesRequest_FULL)
		}()
	}

	wg.Wait()
}

func TestState(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	state := s.State()
	if state["metric_descriptors"].(int) != 0 {
		t.Error("expected 0 metric descriptors initially")
	}

	md := &metric.MetricDescriptor{
		Type:       "custom.googleapis.com/state_test",
		MetricKind: metric.MetricDescriptor_GAUGE,
		ValueType:  metric.MetricDescriptor_DOUBLE,
	}
	s.CreateMetricDescriptor(ctx, "proj", md)

	ts := []*monitoringpb.TimeSeries{
		{
			Metric:   &metric.Metric{Type: "custom.googleapis.com/state_test"},
			Resource: &monitoredres.MonitoredResource{Type: "global"},
			Points:   []*monitoringpb.Point{makePoint(time.Now(), 1.0)},
		},
	}
	s.CreateTimeSeries(ctx, "proj", ts)

	state = s.State()
	if state["metric_descriptors"].(int) != 1 {
		t.Errorf("expected 1 metric descriptor, got %v", state["metric_descriptors"])
	}
	projects := state["projects"].(map[string]interface{})
	projState := projects["proj"].(map[string]interface{})
	if projState["time_series_count"].(int) != 1 {
		t.Errorf("expected 1 time series, got %v", projState["time_series_count"])
	}
}

func TestMultipleSeriesDifferentLabels(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Write two series with the same metric type but different labels.
	ts := []*monitoringpb.TimeSeries{
		{
			Metric:   &metric.Metric{Type: "custom.googleapis.com/cpu", Labels: map[string]string{"env": "prod"}},
			Resource: &monitoredres.MonitoredResource{Type: "global"},
			Points:   []*monitoringpb.Point{makePoint(now, 1.0)},
		},
		{
			Metric:   &metric.Metric{Type: "custom.googleapis.com/cpu", Labels: map[string]string{"env": "staging"}},
			Resource: &monitoredres.MonitoredResource{Type: "global"},
			Points:   []*monitoringpb.Point{makePoint(now, 2.0)},
		},
	}

	if err := s.CreateTimeSeries(ctx, "proj", ts); err != nil {
		t.Fatal(err)
	}

	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(now.Add(-time.Hour)),
		EndTime:   timestamppb.New(now.Add(time.Hour)),
	}
	result, err := s.ListTimeSeries(ctx, "proj", `metric.type = "custom.googleapis.com/cpu"`, interval, monitoringpb.ListTimeSeriesRequest_FULL)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 2 {
		t.Errorf("got %d series, want 2", len(result))
	}
}

func TestListTimeSeriesComplexFilter(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	ts := []*monitoringpb.TimeSeries{
		{
			Metric:   &metric.Metric{Type: "custom.googleapis.com/cpu", Labels: map[string]string{"env": "prod"}},
			Resource: &monitoredres.MonitoredResource{Type: "gce_instance", Labels: map[string]string{"zone": "us-east1-b"}},
			Points:   []*monitoringpb.Point{makePoint(now, 1.0)},
		},
		{
			Metric:   &metric.Metric{Type: "custom.googleapis.com/cpu", Labels: map[string]string{"env": "staging"}},
			Resource: &monitoredres.MonitoredResource{Type: "gce_instance", Labels: map[string]string{"zone": "us-west1-a"}},
			Points:   []*monitoringpb.Point{makePoint(now, 2.0)},
		},
		{
			Metric:   &metric.Metric{Type: "custom.googleapis.com/memory", Labels: map[string]string{"env": "prod"}},
			Resource: &monitoredres.MonitoredResource{Type: "gce_instance", Labels: map[string]string{"zone": "us-east1-b"}},
			Points:   []*monitoringpb.Point{makePoint(now, 3.0)},
		},
	}
	if err := s.CreateTimeSeries(ctx, "proj", ts); err != nil {
		t.Fatal(err)
	}

	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(now.Add(-time.Hour)),
		EndTime:   timestamppb.New(now.Add(time.Hour)),
	}

	tests := []struct {
		name   string
		filter string
		want   int
	}{
		{"metric type AND label", `metric.type = "custom.googleapis.com/cpu" AND metric.labels.env = "prod"`, 1},
		{"resource label filter", `resource.labels.zone = "us-west1-a"`, 1},
		{"OR filter", `metric.labels.env = "prod" OR metric.labels.env = "staging"`, 3},
		{"NOT filter", `metric.type = "custom.googleapis.com/cpu" AND NOT metric.labels.env = "staging"`, 1},
		{"starts_with", `metric.type = starts_with("custom.googleapis.com/c")`, 2},
		{"resource type", `resource.type = "gce_instance"`, 3},
		{"no match", `metric.labels.env = "dev"`, 0},
		{"empty filter returns all", ``, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := s.ListTimeSeries(ctx, "proj", tt.filter, interval, monitoringpb.ListTimeSeriesRequest_FULL)
			if err != nil {
				t.Fatal(err)
			}
			if len(result) != tt.want {
				t.Errorf("filter %q: got %d series, want %d", tt.filter, len(result), tt.want)
			}
		})
	}
}

func TestListMetricDescriptorsFilter(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()

	types := []string{
		"custom.googleapis.com/cpu",
		"custom.googleapis.com/memory",
		"compute.googleapis.com/instance/uptime",
	}
	for _, typ := range types {
		md := &metric.MetricDescriptor{
			Type:       typ,
			MetricKind: metric.MetricDescriptor_GAUGE,
			ValueType:  metric.MetricDescriptor_DOUBLE,
		}
		if _, err := s.CreateMetricDescriptor(ctx, "proj", md); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name   string
		filter string
		want   int
	}{
		{"exact match", `metric.type = "custom.googleapis.com/cpu"`, 1},
		{"starts_with", `metric.type = starts_with("custom.googleapis.com/")`, 2},
		{"no filter", ``, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := s.ListMetricDescriptors(ctx, "proj", tt.filter)
			if err != nil {
				t.Fatal(err)
			}
			if len(result) != tt.want {
				t.Errorf("filter %q: got %d descriptors, want %d", tt.filter, len(result), tt.want)
			}
		})
	}
}
