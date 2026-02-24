package promql

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/timestamppb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
)

func seedStore(t *testing.T, s store.Store) {
	t.Helper()
	ctx := context.Background()

	// Create a metric descriptor.
	md := &metric.MetricDescriptor{
		Type:       "custom.googleapis.com/cpu_usage",
		MetricKind: metric.MetricDescriptor_GAUGE,
		ValueType:  metric.MetricDescriptor_DOUBLE,
	}
	if _, err := s.CreateMetricDescriptor(ctx, "test-project", md); err != nil {
		t.Fatal(err)
	}

	// Write time series with 3 points.
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	ts := []*monitoringpb.TimeSeries{
		{
			Metric: &metric.Metric{
				Type:   "custom.googleapis.com/cpu_usage",
				Labels: map[string]string{"env": "prod"},
			},
			Resource: &monitoredres.MonitoredResource{
				Type:   "gce_instance",
				Labels: map[string]string{"instance_id": "123", "zone": "us-east1-b"},
			},
			Points: []*monitoringpb.Point{
				{
					Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
					Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 0.5}},
				},
			},
		},
	}
	if err := s.CreateTimeSeries(ctx, "test-project", ts); err != nil {
		t.Fatal(err)
	}

	ts2 := []*monitoringpb.TimeSeries{
		{
			Metric: &metric.Metric{
				Type:   "custom.googleapis.com/cpu_usage",
				Labels: map[string]string{"env": "prod"},
			},
			Resource: &monitoredres.MonitoredResource{
				Type:   "gce_instance",
				Labels: map[string]string{"instance_id": "123", "zone": "us-east1-b"},
			},
			Points: []*monitoringpb.Point{
				{
					Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now.Add(time.Minute))},
					Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 0.7}},
				},
			},
		},
	}
	if err := s.CreateTimeSeries(ctx, "test-project", ts2); err != nil {
		t.Fatal(err)
	}

	ts3 := []*monitoringpb.TimeSeries{
		{
			Metric: &metric.Metric{
				Type:   "custom.googleapis.com/cpu_usage",
				Labels: map[string]string{"env": "prod"},
			},
			Resource: &monitoredres.MonitoredResource{
				Type:   "gce_instance",
				Labels: map[string]string{"instance_id": "123", "zone": "us-east1-b"},
			},
			Points: []*monitoringpb.Point{
				{
					Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now.Add(2 * time.Minute))},
					Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 0.9}},
				},
			},
		},
	}
	if err := s.CreateTimeSeries(ctx, "test-project", ts3); err != nil {
		t.Fatal(err)
	}

	// Write a second series with different labels.
	ts4 := []*monitoringpb.TimeSeries{
		{
			Metric: &metric.Metric{
				Type:   "custom.googleapis.com/cpu_usage",
				Labels: map[string]string{"env": "staging"},
			},
			Resource: &monitoredres.MonitoredResource{
				Type:   "gce_instance",
				Labels: map[string]string{"instance_id": "456", "zone": "us-west1-a"},
			},
			Points: []*monitoringpb.Point{
				{
					Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
					Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: 42}},
				},
			},
		},
	}
	if err := s.CreateTimeSeries(ctx, "test-project", ts4); err != nil {
		t.Fatal(err)
	}
}

func TestMetricTypeToPromName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"custom.googleapis.com/my_metric", "custom_googleapis_com:my_metric"},
		{"compute.googleapis.com/instance/cpu/utilization", "compute_googleapis_com:instance_cpu_utilization"},
		{"simple_metric", "simple_metric"},
		{"domain.com/path", "domain_com:path"},
	}
	for _, tt := range tests {
		got := MetricTypeToPromName(tt.input)
		if got != tt.want {
			t.Errorf("MetricTypeToPromName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestPromNameToMetricType(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"custom_googleapis_com:my_metric", "custom.googleapis.com/my/metric"},
		{"simple_metric", "simple.metric"},
		{"domain_com:path", "domain.com/path"},
	}
	for _, tt := range tests {
		got := PromNameToMetricType(tt.input)
		if got != tt.want {
			t.Errorf("PromNameToMetricType(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestGCPToPromLabels(t *testing.T) {
	ts := &monitoringpb.TimeSeries{
		Metric: &metric.Metric{
			Type:   "custom.googleapis.com/cpu_usage",
			Labels: map[string]string{"env": "prod", "region": "us"},
		},
		Resource: &monitoredres.MonitoredResource{
			Type:   "gce_instance",
			Labels: map[string]string{"instance_id": "123", "zone": "us-east1-b"},
		},
	}

	lset := GCPToPromLabels(ts)

	expected := map[string]string{
		"__name__":            "custom_googleapis_com:cpu_usage",
		"env":                 "prod",
		"region":              "us",
		"resource_type":       "gce_instance",
		"resource_instance_id": "123",
		"resource_zone":       "us-east1-b",
	}
	for k, v := range expected {
		got := lset.Get(k)
		if got != v {
			t.Errorf("label %q = %q, want %q", k, got, v)
		}
	}
}

func TestSelect(t *testing.T) {
	s := store.NewMemoryStore()
	seedStore(t, s)

	q := &StoreQueryable{Store: s, Project: "test-project"}
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	querier, err := q.Querier(now.Add(-time.Hour).UnixMilli(), now.Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	defer querier.Close()

	// Select all series.
	ss := querier.Select(context.Background(), true, nil)
	count := 0
	for ss.Next() {
		count++
		series := ss.At()
		_ = series.Labels()

		it := series.Iterator(nil)
		sampleCount := 0
		for it.Next() != chunkenc.ValNone {
			sampleCount++
		}
		if it.Err() != nil {
			t.Fatal(it.Err())
		}
		// First series (prod) has 3 points, second (staging) has 1.
		if sampleCount != 3 && sampleCount != 1 {
			t.Errorf("unexpected sample count: %d", sampleCount)
		}
	}
	if ss.Err() != nil {
		t.Fatal(ss.Err())
	}
	if count != 2 {
		t.Errorf("got %d series, want 2", count)
	}
}

func TestSelectWithMatcher(t *testing.T) {
	s := store.NewMemoryStore()
	seedStore(t, s)

	q := &StoreQueryable{Store: s, Project: "test-project"}
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	querier, err := q.Querier(now.Add(-time.Hour).UnixMilli(), now.Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	defer querier.Close()

	// Select only env="staging" series.
	matcher := labels.MustNewMatcher(labels.MatchEqual, "env", "staging")
	ss := querier.Select(context.Background(), false, nil, matcher)
	count := 0
	for ss.Next() {
		count++
		series := ss.At()
		if series.Labels().Get("env") != "staging" {
			t.Errorf("expected env=staging, got %s", series.Labels().Get("env"))
		}
		it := series.Iterator(nil)
		sampleCount := 0
		for it.Next() != chunkenc.ValNone {
			sampleCount++
		}
		if sampleCount != 1 {
			t.Errorf("expected 1 sample, got %d", sampleCount)
		}
	}
	if count != 1 {
		t.Errorf("got %d series, want 1", count)
	}
}

func TestSelectTimeRangeFiltering(t *testing.T) {
	s := store.NewMemoryStore()
	seedStore(t, s)

	q := &StoreQueryable{Store: s, Project: "test-project"}
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	// Query only the first minute — should get 2 points for prod series
	// (t=0 and t=+1min), plus 1 point for staging.
	querier, err := q.Querier(now.UnixMilli(), now.Add(time.Minute).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	defer querier.Close()

	matcher := labels.MustNewMatcher(labels.MatchEqual, "env", "prod")
	ss := querier.Select(context.Background(), false, nil, matcher)
	if !ss.Next() {
		t.Fatal("expected at least one series")
	}

	it := ss.At().Iterator(nil)
	sampleCount := 0
	for it.Next() != chunkenc.ValNone {
		sampleCount++
	}
	if sampleCount != 2 {
		t.Errorf("expected 2 samples in first minute, got %d", sampleCount)
	}
}

func TestSelectEmptyProject(t *testing.T) {
	s := store.NewMemoryStore()

	q := &StoreQueryable{Store: s, Project: "empty-project"}
	querier, err := q.Querier(0, time.Now().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	defer querier.Close()

	ss := querier.Select(context.Background(), false, nil)
	if ss.Next() {
		t.Error("expected no series for empty project")
	}
}

func TestLabelValues(t *testing.T) {
	s := store.NewMemoryStore()
	seedStore(t, s)

	q := &StoreQueryable{Store: s, Project: "test-project"}
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	querier, err := q.Querier(now.Add(-time.Hour).UnixMilli(), now.Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	defer querier.Close()

	values, _, err := querier.LabelValues(context.Background(), "env", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 env values, got %d: %v", len(values), values)
	}
	// Values should be sorted.
	if values[0] != "prod" || values[1] != "staging" {
		t.Errorf("unexpected values: %v", values)
	}
}

func TestLabelValuesWithMatcher(t *testing.T) {
	s := store.NewMemoryStore()
	seedStore(t, s)

	q := &StoreQueryable{Store: s, Project: "test-project"}
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	querier, err := q.Querier(now.Add(-time.Hour).UnixMilli(), now.Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	defer querier.Close()

	matcher := labels.MustNewMatcher(labels.MatchEqual, "resource_zone", "us-east1-b")
	values, _, err := querier.LabelValues(context.Background(), "env", nil, matcher)
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0] != "prod" {
		t.Errorf("expected [prod], got %v", values)
	}
}

func TestLabelNames(t *testing.T) {
	s := store.NewMemoryStore()
	seedStore(t, s)

	q := &StoreQueryable{Store: s, Project: "test-project"}
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	querier, err := q.Querier(now.Add(-time.Hour).UnixMilli(), now.Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	defer querier.Close()

	names, _, err := querier.LabelNames(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	expected := map[string]bool{
		"__name__":             true,
		"env":                  true,
		"resource_type":        true,
		"resource_instance_id": true,
		"resource_zone":        true,
	}
	for _, n := range names {
		delete(expected, n)
	}
	if len(expected) != 0 {
		t.Errorf("missing label names: %v", expected)
	}
}

func TestSampleIteratorSeek(t *testing.T) {
	samples := []sample{
		{t: 1000, v: 1.0},
		{t: 2000, v: 2.0},
		{t: 3000, v: 3.0},
		{t: 4000, v: 4.0},
		{t: 5000, v: 5.0},
	}
	it := &sampleIterator{samples: samples, idx: -1}

	// Seek to t=3000.
	if vt := it.Seek(3000); vt != chunkenc.ValFloat {
		t.Fatalf("Seek(3000) = %v, want ValFloat", vt)
	}
	ts, v := it.At()
	if ts != 3000 || v != 3.0 {
		t.Errorf("At() = (%d, %f), want (3000, 3.0)", ts, v)
	}

	// Seek to same position — should be no-op.
	if vt := it.Seek(3000); vt != chunkenc.ValFloat {
		t.Fatalf("Seek(3000) again = %v, want ValFloat", vt)
	}
	ts, v = it.At()
	if ts != 3000 || v != 3.0 {
		t.Errorf("At() after re-seek = (%d, %f), want (3000, 3.0)", ts, v)
	}

	// Seek to earlier value — should stay at current position.
	if vt := it.Seek(1000); vt != chunkenc.ValFloat {
		t.Fatalf("Seek(1000) = %v, want ValFloat", vt)
	}
	ts, _ = it.At()
	if ts != 3000 {
		t.Errorf("Seek to earlier should not move back, got t=%d", ts)
	}

	// Seek past all samples.
	if vt := it.Seek(9999); vt != chunkenc.ValNone {
		t.Fatalf("Seek(9999) = %v, want ValNone", vt)
	}
}

func TestSampleIteratorNextAndAt(t *testing.T) {
	samples := []sample{
		{t: 100, v: 1.0},
		{t: 200, v: 2.0},
		{t: 300, v: 3.0},
	}
	it := &sampleIterator{samples: samples, idx: -1}

	for i, want := range samples {
		if vt := it.Next(); vt != chunkenc.ValFloat {
			t.Fatalf("sample %d: Next() = %v, want ValFloat", i, vt)
		}
		ts, v := it.At()
		if ts != want.t || v != want.v {
			t.Errorf("sample %d: At() = (%d, %f), want (%d, %f)", i, ts, v, want.t, want.v)
		}
		if it.AtT() != want.t {
			t.Errorf("sample %d: AtT() = %d, want %d", i, it.AtT(), want.t)
		}
	}

	// After all samples, Next returns ValNone.
	if vt := it.Next(); vt != chunkenc.ValNone {
		t.Errorf("Next() after exhaustion = %v, want ValNone", vt)
	}
}

func TestExtractFloat64(t *testing.T) {
	tests := []struct {
		name string
		val  *monitoringpb.TypedValue
		want float64
	}{
		{
			name: "double",
			val:  &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 3.14}},
			want: 3.14,
		},
		{
			name: "int64",
			val:  &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: 42}},
			want: 42.0,
		},
		{
			name: "bool_true",
			val:  &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_BoolValue{BoolValue: true}},
			want: 1.0,
		},
		{
			name: "bool_false",
			val:  &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_BoolValue{BoolValue: false}},
			want: 0.0,
		},
		{
			name: "nil",
			val:  &monitoringpb.TypedValue{},
			want: 0.0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractFloat64(tt.val)
			if got != tt.want {
				t.Errorf("extractFloat64() = %f, want %f", got, tt.want)
			}
		})
	}
}

func TestSelectSorted(t *testing.T) {
	s := store.NewMemoryStore()
	seedStore(t, s)

	q := &StoreQueryable{Store: s, Project: "test-project"}
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	querier, err := q.Querier(now.Add(-time.Hour).UnixMilli(), now.Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	defer querier.Close()

	ss := querier.Select(context.Background(), true, nil)
	var prevLabels labels.Labels
	for ss.Next() {
		cur := ss.At().Labels()
		if prevLabels.Len() > 0 && labels.Compare(prevLabels, cur) >= 0 {
			t.Errorf("series not sorted: %v >= %v", prevLabels, cur)
		}
		prevLabels = cur
	}
}
