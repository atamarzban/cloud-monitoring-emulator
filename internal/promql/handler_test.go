package promql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/timestamppb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
)

const testProject = "test-project"

func basePath(project string) string {
	return fmt.Sprintf("/v1/projects/%s/location/global/prometheus/api/v1", project)
}

func seedHandlerStore(t *testing.T, s store.Store) time.Time {
	t.Helper()
	ctx := context.Background()

	md := &metric.MetricDescriptor{
		Type:       "custom.googleapis.com/cpu_usage",
		MetricKind: metric.MetricDescriptor_GAUGE,
		ValueType:  metric.MetricDescriptor_DOUBLE,
	}
	if _, err := s.CreateMetricDescriptor(ctx, testProject, md); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)

	for i := 0; i < 3; i++ {
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
						Interval: &monitoringpb.TimeInterval{
							EndTime: timestamppb.New(now.Add(time.Duration(i) * time.Minute)),
						},
						Value: &monitoringpb.TypedValue{
							Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: float64(i+1) * 0.5},
						},
					},
				},
			},
		}
		if err := s.CreateTimeSeries(ctx, testProject, ts); err != nil {
			t.Fatal(err)
		}
	}
	return now
}

func TestParsePromPath(t *testing.T) {
	tests := []struct {
		path     string
		project  string
		endpoint string
		ok       bool
	}{
		{"/v1/projects/my-proj/location/global/prometheus/api/v1/query", "my-proj", "query", true},
		{"/v1/projects/my-proj/location/global/prometheus/api/v1/query_range", "my-proj", "query_range", true},
		{"/v1/projects/my-proj/location/us-east1/prometheus/api/v1/label/env/values", "my-proj", "label/env/values", true},
		{"/v1/projects/my-proj/location/global/prometheus/api/v1/series", "my-proj", "series", true},
		{"/bad/path", "", "", false},
		{"/v1/projects/p/location/l/bad/api/v1/query", "", "", false},
	}
	for _, tt := range tests {
		project, endpoint, ok := parsePromPath(tt.path)
		if ok != tt.ok || project != tt.project || endpoint != tt.endpoint {
			t.Errorf("parsePromPath(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tt.path, project, endpoint, ok, tt.project, tt.endpoint, tt.ok)
		}
	}
}

func TestParseLabelValuesEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		name     string
		ok       bool
	}{
		{"label/env/values", "env", true},
		{"label/__name__/values", "__name__", true},
		{"label//values", "", false},
		{"query", "", false},
	}
	for _, tt := range tests {
		name, ok := parseLabelValuesEndpoint(tt.endpoint)
		if ok != tt.ok || name != tt.name {
			t.Errorf("parseLabelValuesEndpoint(%q) = (%q, %v), want (%q, %v)",
				tt.endpoint, name, ok, tt.name, tt.ok)
		}
	}
}

func TestInstantQuery(t *testing.T) {
	s := store.NewMemoryStore()
	now := seedHandlerStore(t, s)
	handler := NewHandler(s)

	// Query for the metric at the last data point.
	queryTime := fmt.Sprintf("%d", now.Add(2*time.Minute).Unix())
	req := httptest.NewRequest("GET",
		basePath(testProject)+"/query?query=custom_googleapis_com:cpu_usage&time="+queryTime, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200. body: %s", w.Code, w.Body.String())
	}

	var resp apiResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "success" {
		t.Fatalf("response status = %q, want success. error: %s", resp.Status, resp.Error)
	}

	data, ok := resp.Data.(map[string]interface{})
	if !ok {
		t.Fatal("data is not a map")
	}
	if data["resultType"] != "vector" {
		t.Errorf("resultType = %q, want vector", data["resultType"])
	}
}

func TestInstantQueryMissingParam(t *testing.T) {
	s := store.NewMemoryStore()
	handler := NewHandler(s)

	req := httptest.NewRequest("GET", basePath(testProject)+"/query", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want 400", w.Code)
	}
}

func TestRangeQuery(t *testing.T) {
	s := store.NewMemoryStore()
	now := seedHandlerStore(t, s)
	handler := NewHandler(s)

	params := url.Values{
		"query": {"custom_googleapis_com:cpu_usage"},
		"start": {fmt.Sprintf("%d", now.Add(-time.Minute).Unix())},
		"end":   {fmt.Sprintf("%d", now.Add(3*time.Minute).Unix())},
		"step":  {"60"},
	}
	req := httptest.NewRequest("GET",
		basePath(testProject)+"/query_range?"+params.Encode(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200. body: %s", w.Code, w.Body.String())
	}

	var resp apiResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "success" {
		t.Fatalf("response status = %q, want success. error: %s", resp.Status, resp.Error)
	}

	data, ok := resp.Data.(map[string]interface{})
	if !ok {
		t.Fatal("data is not a map")
	}
	if data["resultType"] != "matrix" {
		t.Errorf("resultType = %q, want matrix", data["resultType"])
	}
}

func TestRangeQueryMissingStep(t *testing.T) {
	s := store.NewMemoryStore()
	handler := NewHandler(s)

	params := url.Values{
		"query": {"up"},
		"start": {"1609459200"},
		"end":   {"1609462800"},
	}
	req := httptest.NewRequest("GET",
		basePath(testProject)+"/query_range?"+params.Encode(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want 400", w.Code)
	}
}

func TestSeries(t *testing.T) {
	s := store.NewMemoryStore()
	now := seedHandlerStore(t, s)
	handler := NewHandler(s)

	params := url.Values{
		"match[]": {`{__name__="custom_googleapis_com:cpu_usage"}`},
		"start":   {fmt.Sprintf("%d", now.Add(-time.Hour).Unix())},
		"end":     {fmt.Sprintf("%d", now.Add(time.Hour).Unix())},
	}
	req := httptest.NewRequest("GET",
		basePath(testProject)+"/series?"+params.Encode(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200. body: %s", w.Code, w.Body.String())
	}

	var resp apiResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "success" {
		t.Fatalf("response status = %q, want success", resp.Status)
	}

	data, ok := resp.Data.([]interface{})
	if !ok {
		t.Fatal("data is not an array")
	}
	if len(data) != 1 {
		t.Errorf("got %d series, want 1", len(data))
	}
}

func TestSeriesMissingMatch(t *testing.T) {
	s := store.NewMemoryStore()
	handler := NewHandler(s)

	req := httptest.NewRequest("GET", basePath(testProject)+"/series", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want 400", w.Code)
	}
}

func TestHandlerLabelNames(t *testing.T) {
	s := store.NewMemoryStore()
	now := seedHandlerStore(t, s)
	handler := NewHandler(s)

	params := url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-time.Hour).Unix())},
		"end":   {fmt.Sprintf("%d", now.Add(time.Hour).Unix())},
	}
	req := httptest.NewRequest("GET",
		basePath(testProject)+"/labels?"+params.Encode(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200. body: %s", w.Code, w.Body.String())
	}

	var resp apiResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "success" {
		t.Fatalf("response status = %q, want success", resp.Status)
	}

	data, ok := resp.Data.([]interface{})
	if !ok {
		t.Fatal("data is not an array")
	}
	// Should include at least __name__, env, resource_type, resource_instance_id, resource_zone.
	if len(data) < 5 {
		t.Errorf("got %d label names, expected at least 5: %v", len(data), data)
	}
}

func TestHandlerLabelValues(t *testing.T) {
	s := store.NewMemoryStore()
	now := seedHandlerStore(t, s)
	handler := NewHandler(s)

	params := url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-time.Hour).Unix())},
		"end":   {fmt.Sprintf("%d", now.Add(time.Hour).Unix())},
	}
	req := httptest.NewRequest("GET",
		basePath(testProject)+"/label/env/values?"+params.Encode(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200. body: %s", w.Code, w.Body.String())
	}

	var resp apiResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status != "success" {
		t.Fatalf("response status = %q, want success", resp.Status)
	}

	data, ok := resp.Data.([]interface{})
	if !ok {
		t.Fatal("data is not an array")
	}
	if len(data) != 1 || data[0] != "prod" {
		t.Errorf("expected [prod], got %v", data)
	}
}

func TestQueryExemplars(t *testing.T) {
	s := store.NewMemoryStore()
	handler := NewHandler(s)

	req := httptest.NewRequest("GET", basePath(testProject)+"/query_exemplars", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}

	var resp apiResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Status != "success" {
		t.Errorf("status = %q, want success", resp.Status)
	}
}

func TestMetadata(t *testing.T) {
	s := store.NewMemoryStore()
	handler := NewHandler(s)

	req := httptest.NewRequest("GET", basePath(testProject)+"/metadata", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want 200", w.Code)
	}

	var resp apiResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Status != "success" {
		t.Errorf("status = %q, want success", resp.Status)
	}
}

func TestInvalidPath(t *testing.T) {
	s := store.NewMemoryStore()
	handler := NewHandler(s)

	req := httptest.NewRequest("GET", "/bad/path", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("got status %d, want 404", w.Code)
	}
}

func TestUnknownEndpoint(t *testing.T) {
	s := store.NewMemoryStore()
	handler := NewHandler(s)

	req := httptest.NewRequest("GET", basePath(testProject)+"/unknown", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("got status %d, want 404", w.Code)
	}
}

func TestPostQuery(t *testing.T) {
	s := store.NewMemoryStore()
	now := seedHandlerStore(t, s)
	handler := NewHandler(s)

	form := url.Values{
		"query": {"custom_googleapis_com:cpu_usage"},
		"time":  {fmt.Sprintf("%d", now.Add(2*time.Minute).Unix())},
	}
	req := httptest.NewRequest("POST", basePath(testProject)+"/query",
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200. body: %s", w.Code, w.Body.String())
	}

	var resp apiResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Status != "success" {
		t.Errorf("status = %q, want success. error: %s", resp.Status, resp.Error)
	}
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		input string
		err   bool
	}{
		{"1609459200", false},         // Unix timestamp
		{"1609459200.123", false},     // Unix with fractional
		{"2021-01-01T00:00:00Z", false}, // RFC3339
		{"", false},                   // Empty → now
		{"not-a-time", true},
	}
	for _, tt := range tests {
		_, err := parseTime(tt.input)
		if (err != nil) != tt.err {
			t.Errorf("parseTime(%q) error = %v, wantErr = %v", tt.input, err, tt.err)
		}
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		input string
		want  time.Duration
		err   bool
	}{
		{"60", 60 * time.Second, false},
		{"15s", 15 * time.Second, false},
		{"1m", time.Minute, false},
		{"0.5", 500 * time.Millisecond, false},
		{"", 0, true},
		{"abc", 0, true},
	}
	for _, tt := range tests {
		got, err := parseDuration(tt.input)
		if (err != nil) != tt.err {
			t.Errorf("parseDuration(%q) error = %v, wantErr = %v", tt.input, err, tt.err)
			continue
		}
		if err == nil && got != tt.want {
			t.Errorf("parseDuration(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestFormatFloat(t *testing.T) {
	tests := []struct {
		input float64
		want  string
	}{
		{1.5, "1.5"},
		{42.0, "42"},
		{0.0, "0"},
	}
	for _, tt := range tests {
		got := formatFloat(tt.input)
		if got != tt.want {
			t.Errorf("formatFloat(%f) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestInvalidQueryExpr(t *testing.T) {
	s := store.NewMemoryStore()
	handler := NewHandler(s)

	req := httptest.NewRequest("GET",
		basePath(testProject)+"/query?query=invalid{{{", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("got status %d, want 400. body: %s", w.Code, w.Body.String())
	}
}
