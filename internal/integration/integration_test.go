package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/soheilhy/cmux"
	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/admin"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/promql"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/server"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
)

// testServer starts a full emulator server on a random port and returns
// the port and a cleanup function.
func testServer(t *testing.T) (int, func()) {
	t.Helper()

	s := store.NewMemoryStore()

	grpcServer := grpc.NewServer()
	metricSvc := server.NewMetricServiceServer(s)
	alertSvc := server.NewAlertPolicyServiceServer(s)
	monitoringpb.RegisterMetricServiceServer(grpcServer, metricSvc)
	monitoringpb.RegisterAlertPolicyServiceServer(grpcServer, alertSvc)

	gwMux := runtime.NewServeMux()
	if err := monitoringpb.RegisterMetricServiceHandlerServer(context.Background(), gwMux, metricSvc); err != nil {
		t.Fatal(err)
	}
	if err := monitoringpb.RegisterAlertPolicyServiceHandlerServer(context.Background(), gwMux, alertSvc); err != nil {
		t.Fatal(err)
	}

	httpMux := http.NewServeMux()
	httpMux.Handle("/v3/", gwMux)
	httpMux.Handle("/v1/", promql.NewHandler(s))
	httpMux.Handle("/admin/", admin.NewHandler(s))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := lis.Addr().(*net.TCPAddr).Port

	m := cmux.New(lis)
	grpcLis := m.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
	httpLis := m.Match(cmux.Any())

	go grpcServer.Serve(grpcLis)
	go http.Serve(httpLis, httpMux)
	go m.Serve()

	cleanup := func() {
		grpcServer.GracefulStop()
		lis.Close()
	}
	return port, cleanup
}

func grpcConn(t *testing.T, port int) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		fmt.Sprintf("127.0.0.1:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func grpcClient(t *testing.T, port int) monitoringpb.MetricServiceClient {
	return monitoringpb.NewMetricServiceClient(grpcConn(t, port))
}

func TestGRPCWriteRead(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/integration-test"

	// Create a metric descriptor.
	md, err := client.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
		Name: project,
		MetricDescriptor: &metric.MetricDescriptor{
			Type:       "custom.googleapis.com/test_metric",
			MetricKind: metric.MetricDescriptor_GAUGE,
			ValueType:  metric.MetricDescriptor_DOUBLE,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if md.GetType() != "custom.googleapis.com/test_metric" {
		t.Errorf("metric type = %q", md.GetType())
	}

	// Write time series.
	now := time.Now()
	_, err = client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: project,
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric:   &metric.Metric{Type: "custom.googleapis.com/test_metric", Labels: map[string]string{"env": "test"}},
				Resource: &monitoredres.MonitoredResource{Type: "global"},
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

	// Read time series back.
	resp, err := client.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:   project,
		Filter: `metric.type = "custom.googleapis.com/test_metric"`,
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(now.Add(-time.Hour)),
			EndTime:   timestamppb.New(now.Add(time.Hour)),
		},
		View: monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(resp.GetTimeSeries()) != 1 {
		t.Fatalf("got %d time series, want 1", len(resp.GetTimeSeries()))
	}

	ts := resp.GetTimeSeries()[0]
	if ts.GetMetric().GetType() != "custom.googleapis.com/test_metric" {
		t.Errorf("metric type = %q", ts.GetMetric().GetType())
	}
	if len(ts.GetPoints()) != 1 {
		t.Fatalf("got %d points, want 1", len(ts.GetPoints()))
	}
	if ts.GetPoints()[0].GetValue().GetDoubleValue() != 42.0 {
		t.Errorf("value = %f, want 42.0", ts.GetPoints()[0].GetValue().GetDoubleValue())
	}
}

func TestGRPCWritePromQLRead(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/promql-test"
	projectID := "promql-test"

	// Write time series via gRPC.
	now := time.Now().Truncate(time.Second)
	_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: project,
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric:   &metric.Metric{Type: "custom.googleapis.com/latency", Labels: map[string]string{"service": "web"}},
				Resource: &monitoredres.MonitoredResource{Type: "global"},
				Points: []*monitoringpb.Point{
					{
						Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
						Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 150.0}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Query via Prometheus HTTP API.
	promURL := fmt.Sprintf("http://127.0.0.1:%d/v1/projects/%s/location/global/prometheus/api/v1/query", port, projectID)
	params := url.Values{
		"query": {"custom_googleapis_com:latency"},
		"time":  {fmt.Sprintf("%d", now.Unix())},
	}

	resp, err := http.Get(promURL + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("PromQL query status %d: %s", resp.StatusCode, body)
	}

	var apiResp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string        `json:"resultType"`
			Result     []interface{} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		t.Fatal(err)
	}
	if apiResp.Status != "success" {
		t.Fatalf("PromQL status = %q", apiResp.Status)
	}
	if apiResp.Data.ResultType != "vector" {
		t.Errorf("resultType = %q, want vector", apiResp.Data.ResultType)
	}
	if len(apiResp.Data.Result) != 1 {
		t.Errorf("got %d results, want 1", len(apiResp.Data.Result))
	}
}

func TestRESTWriteRead(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	project := "rest-test"

	// Create metric descriptor via REST.
	mdJSON := `{
		"type": "custom.googleapis.com/rest_metric",
		"metricKind": "GAUGE",
		"valueType": "DOUBLE"
	}`
	resp, err := http.Post(
		baseURL+"/v3/projects/"+project+"/metricDescriptors",
		"application/json",
		jsonReader(mdJSON),
	)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create metric descriptor status %d", resp.StatusCode)
	}

	// Write time series via REST.
	now := time.Now()
	tsJSON := fmt.Sprintf(`{
		"timeSeries": [{
			"metric": {"type": "custom.googleapis.com/rest_metric"},
			"resource": {"type": "global"},
			"points": [{
				"interval": {"endTime": "%s"},
				"value": {"doubleValue": 99.9}
			}]
		}]
	}`, now.Format(time.RFC3339Nano))

	resp, err = http.Post(
		baseURL+"/v3/projects/"+project+"/timeSeries",
		"application/json",
		jsonReader(tsJSON),
	)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create time series status %d", resp.StatusCode)
	}

	// Read back via REST.
	params := url.Values{
		"filter":             {`metric.type = "custom.googleapis.com/rest_metric"`},
		"interval.startTime": {now.Add(-time.Hour).Format(time.RFC3339Nano)},
		"interval.endTime":   {now.Add(time.Hour).Format(time.RFC3339Nano)},
		"view":               {"FULL"},
	}
	resp, err = http.Get(baseURL + "/v3/projects/" + project + "/timeSeries?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("list time series status %d: %s", resp.StatusCode, body)
	}

	var listResp struct {
		TimeSeries []struct {
			Metric struct {
				Type string `json:"type"`
			} `json:"metric"`
		} `json:"timeSeries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&listResp); err != nil {
		t.Fatal(err)
	}
	if len(listResp.TimeSeries) != 1 {
		t.Fatalf("got %d time series, want 1", len(listResp.TimeSeries))
	}
	if listResp.TimeSeries[0].Metric.Type != "custom.googleapis.com/rest_metric" {
		t.Errorf("metric type = %q", listResp.TimeSeries[0].Metric.Type)
	}
}

func TestAdminReset(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/reset-test"
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	// Write some data.
	_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: project,
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric:   &metric.Metric{Type: "custom.googleapis.com/to_reset"},
				Resource: &monitoredres.MonitoredResource{Type: "global"},
				Points: []*monitoringpb.Point{
					{
						Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(time.Now())},
						Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify data exists via admin state.
	resp, err := http.Get(baseURL + "/admin/state")
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&state)
	resp.Body.Close()

	projects, ok := state["projects"].(map[string]interface{})
	if !ok || len(projects) == 0 {
		t.Fatal("expected project data in state")
	}

	// Reset.
	req, _ := http.NewRequest("POST", baseURL+"/admin/reset", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("reset status %d, want 204", resp.StatusCode)
	}

	// Verify data is gone.
	resp, err = http.Get(baseURL + "/admin/state")
	if err != nil {
		t.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&state)
	resp.Body.Close()

	projects, _ = state["projects"].(map[string]interface{})
	if len(projects) != 0 {
		t.Errorf("expected empty projects after reset, got %v", projects)
	}
}

func TestValidationErrors(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()

	// CreateTimeSeries with missing required fields.
	_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/validation-test",
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				// Missing metric, resource, and points.
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for missing fields")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}

	// CreateTimeSeries with too many time series (>200).
	tsList := make([]*monitoringpb.TimeSeries, 201)
	for i := range tsList {
		tsList[i] = &monitoringpb.TimeSeries{
			Metric:   &metric.Metric{Type: fmt.Sprintf("custom.googleapis.com/m%d", i)},
			Resource: &monitoredres.MonitoredResource{Type: "global"},
			Points: []*monitoringpb.Point{
				{
					Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(time.Now())},
					Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
				},
			},
		}
	}
	_, err = client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name:       "projects/validation-test",
		TimeSeries: tsList,
	})
	if err == nil {
		t.Fatal("expected error for too many time series")
	}
	st, _ = status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}

	// GetMetricDescriptor for non-existent descriptor.
	_, err = client.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: "projects/validation-test/metricDescriptors/custom.googleapis.com/nonexistent",
	})
	if err == nil {
		t.Fatal("expected error for non-existent descriptor")
	}
	st, _ = status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", st.Code())
	}
}

func TestConcurrentAccess(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/concurrent-test"

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	// 10 concurrent writers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
				Name: project,
				TimeSeries: []*monitoringpb.TimeSeries{
					{
						Metric: &metric.Metric{
							Type:   fmt.Sprintf("custom.googleapis.com/concurrent_%d", i),
							Labels: map[string]string{"worker": fmt.Sprintf("%d", i)},
						},
						Resource: &monitoredres.MonitoredResource{Type: "global"},
						Points: []*monitoringpb.Point{
							{
								Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(time.Now())},
								Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: float64(i)}},
							},
						},
					},
				},
			})
			if err != nil {
				errs <- fmt.Errorf("writer %d: %w", i, err)
			}
		}(i)
	}

	// 10 concurrent readers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := client.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
				Name:   project,
				Filter: fmt.Sprintf(`metric.type = "custom.googleapis.com/concurrent_%d"`, i),
				Interval: &monitoringpb.TimeInterval{
					StartTime: timestamppb.New(time.Now().Add(-time.Hour)),
					EndTime:   timestamppb.New(time.Now().Add(time.Hour)),
				},
				View: monitoringpb.ListTimeSeriesRequest_FULL,
			})
			if err != nil {
				errs <- fmt.Errorf("reader %d: %w", i, err)
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func TestAlertPolicyCRUD(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	conn := grpcConn(t, port)
	client := monitoringpb.NewAlertPolicyServiceClient(conn)
	ctx := context.Background()
	project := "projects/alert-test"

	// Create an alert policy.
	created, err := client.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: project,
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "High CPU",
			Conditions: []*monitoringpb.AlertPolicy_Condition{
				{
					DisplayName: "CPU > 90%",
					Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{
						ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
							Filter: `metric.type = "compute.googleapis.com/instance/cpu/utilization"`,
						},
					},
				},
			},
			Combiner: monitoringpb.AlertPolicy_OR,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.GetDisplayName() != "High CPU" {
		t.Errorf("display_name = %q, want %q", created.GetDisplayName(), "High CPU")
	}
	if created.GetName() == "" {
		t.Fatal("created policy has empty name")
	}

	// Get the policy back.
	got, err := client.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{
		Name: created.GetName(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetDisplayName() != "High CPU" {
		t.Errorf("display_name = %q", got.GetDisplayName())
	}

	// List policies.
	listResp, err := client.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{
		Name: project,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.GetAlertPolicies()) != 1 {
		t.Fatalf("got %d policies, want 1", len(listResp.GetAlertPolicies()))
	}

	// Update the policy.
	created.DisplayName = "Critical CPU"
	updated, err := client.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
		AlertPolicy: created,
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.GetDisplayName() != "Critical CPU" {
		t.Errorf("updated display_name = %q, want %q", updated.GetDisplayName(), "Critical CPU")
	}

	// Delete the policy.
	_, err = client.DeleteAlertPolicy(ctx, &monitoringpb.DeleteAlertPolicyRequest{
		Name: created.GetName(),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify deletion.
	_, err = client.GetAlertPolicy(ctx, &monitoringpb.GetAlertPolicyRequest{
		Name: created.GetName(),
	})
	if err == nil {
		t.Fatal("expected error after delete")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", st.Code())
	}
}

// jsonReader wraps a JSON string in an io.Reader.
func jsonReader(s string) io.Reader {
	return io.NopCloser(jsonReaderString(s))
}

type jsonReaderString string

func (s jsonReaderString) Read(p []byte) (int, error) {
	n := copy(p, string(s))
	if n < len(string(s)) {
		return n, nil
	}
	return n, io.EOF
}
