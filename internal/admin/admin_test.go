package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/timestamppb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
)

func seedData(t *testing.T, s store.Store) {
	t.Helper()
	ctx := context.Background()

	md := &metric.MetricDescriptor{
		Type:       "custom.googleapis.com/admin_test",
		MetricKind: metric.MetricDescriptor_GAUGE,
		ValueType:  metric.MetricDescriptor_DOUBLE,
	}
	if _, err := s.CreateMetricDescriptor(ctx, "proj", md); err != nil {
		t.Fatal(err)
	}

	ts := []*monitoringpb.TimeSeries{
		{
			Metric:   &metric.Metric{Type: "custom.googleapis.com/admin_test"},
			Resource: &monitoredres.MonitoredResource{Type: "global"},
			Points: []*monitoringpb.Point{
				{
					Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(time.Now())},
					Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
				},
			},
		},
	}
	if err := s.CreateTimeSeries(ctx, "proj", ts); err != nil {
		t.Fatal(err)
	}
}

func TestReset(t *testing.T) {
	s := store.NewMemoryStore()
	seedData(t, s)

	handler := NewHandler(s)

	// Verify state has data.
	state := s.State()
	if state["metric_descriptors"].(int) != 1 {
		t.Fatal("expected 1 metric descriptor before reset")
	}

	// POST /admin/reset
	req := httptest.NewRequest("POST", "/admin/reset", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("got status %d, want 204", w.Code)
	}

	// Verify data is gone.
	state = s.State()
	if state["metric_descriptors"].(int) != 0 {
		t.Error("expected 0 metric descriptors after reset")
	}
}

func TestState(t *testing.T) {
	s := store.NewMemoryStore()
	seedData(t, s)

	handler := NewHandler(s)

	req := httptest.NewRequest("GET", "/admin/state", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("got content-type %q, want application/json", w.Header().Get("Content-Type"))
	}

	var state map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	if state["metric_descriptors"].(float64) != 1 {
		t.Errorf("expected 1 metric descriptor, got %v", state["metric_descriptors"])
	}
}
