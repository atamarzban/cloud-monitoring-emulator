package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
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
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/admin"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/promql"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/server"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/token"
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
	httpMux.Handle("/token", token.NewHandler())

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

// --- Task 15: MetricDescriptor lifecycle ---

func TestMetricDescriptorLifecycle(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/md-lifecycle"

	// Create two descriptors.
	for _, mt := range []string{"custom.googleapis.com/cpu_usage", "custom.googleapis.com/mem_usage"} {
		_, err := client.CreateMetricDescriptor(ctx, &monitoringpb.CreateMetricDescriptorRequest{
			Name: project,
			MetricDescriptor: &metric.MetricDescriptor{
				Type:       mt,
				MetricKind: metric.MetricDescriptor_GAUGE,
				ValueType:  metric.MetricDescriptor_DOUBLE,
			},
		})
		if err != nil {
			t.Fatalf("create %s: %v", mt, err)
		}
	}

	// Get one back.
	got, err := client.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: project + "/metricDescriptors/custom.googleapis.com/cpu_usage",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetType() != "custom.googleapis.com/cpu_usage" {
		t.Errorf("type = %q", got.GetType())
	}

	// List — should contain both.
	listResp, err := client.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{
		Name: project,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.GetMetricDescriptors()) != 2 {
		t.Fatalf("got %d descriptors, want 2", len(listResp.GetMetricDescriptors()))
	}

	// List with filter.
	listResp, err = client.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{
		Name:   project,
		Filter: `metric.type = "custom.googleapis.com/cpu_usage"`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.GetMetricDescriptors()) != 1 {
		t.Fatalf("filtered: got %d descriptors, want 1", len(listResp.GetMetricDescriptors()))
	}

	// Delete one.
	_, err = client.DeleteMetricDescriptor(ctx, &monitoringpb.DeleteMetricDescriptorRequest{
		Name: project + "/metricDescriptors/custom.googleapis.com/cpu_usage",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify it's gone.
	_, err = client.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: project + "/metricDescriptors/custom.googleapis.com/cpu_usage",
	})
	if err == nil {
		t.Fatal("expected NotFound after delete")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", st.Code())
	}

	// List should now have 1.
	listResp, err = client.ListMetricDescriptors(ctx, &monitoringpb.ListMetricDescriptorsRequest{
		Name: project,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.GetMetricDescriptors()) != 1 {
		t.Fatalf("after delete: got %d descriptors, want 1", len(listResp.GetMetricDescriptors()))
	}
}

// --- Task 16: MonitoredResourceDescriptor ---

func TestMonitoredResourceDescriptors(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/mrd-test"

	// List should return seeded types.
	listResp, err := client.ListMonitoredResourceDescriptors(ctx, &monitoringpb.ListMonitoredResourceDescriptorsRequest{
		Name: project,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.GetResourceDescriptors()) < 5 {
		t.Fatalf("got %d resource descriptors, want at least 5 seeded types", len(listResp.GetResourceDescriptors()))
	}

	// Check known types exist.
	types := map[string]bool{}
	for _, rd := range listResp.GetResourceDescriptors() {
		types[rd.GetType()] = true
	}
	for _, want := range []string{"global", "gce_instance", "k8s_container", "k8s_pod"} {
		if !types[want] {
			t.Errorf("missing seeded type %q", want)
		}
	}

	// Get a specific one.
	got, err := client.GetMonitoredResourceDescriptor(ctx, &monitoringpb.GetMonitoredResourceDescriptorRequest{
		Name: project + "/monitoredResourceDescriptors/gce_instance",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetType() != "gce_instance" {
		t.Errorf("type = %q, want gce_instance", got.GetType())
	}

	// Get non-existent returns NotFound.
	_, err = client.GetMonitoredResourceDescriptor(ctx, &monitoringpb.GetMonitoredResourceDescriptorRequest{
		Name: project + "/monitoredResourceDescriptors/nonexistent_type",
	})
	if err == nil {
		t.Fatal("expected NotFound")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", st.Code())
	}
}

// --- Task 17: TimeSeries advanced ---

func TestTimeSeriesAutoDescriptorCreation(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/auto-desc"

	// Write time series without creating a descriptor first.
	now := time.Now()
	_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: project,
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric:   &metric.Metric{Type: "custom.googleapis.com/auto_metric", Labels: map[string]string{"env": "prod"}},
				Resource: &monitoredres.MonitoredResource{Type: "global"},
				Points: []*monitoringpb.Point{
					{
						Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
						Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify the descriptor was auto-created.
	md, err := client.GetMetricDescriptor(ctx, &monitoringpb.GetMetricDescriptorRequest{
		Name: project + "/metricDescriptors/custom.googleapis.com/auto_metric",
	})
	if err != nil {
		t.Fatal("auto-created descriptor not found:", err)
	}
	if md.GetMetricKind() != metric.MetricDescriptor_GAUGE {
		t.Errorf("auto-created kind = %v, want GAUGE", md.GetMetricKind())
	}
}

func TestTimeSeriesMultiplePoints(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/multi-point"

	now := time.Now()
	metricType := "custom.googleapis.com/multi_point"

	// Write two points at different times to the same series.
	for i, offset := range []time.Duration{-2 * time.Minute, -1 * time.Minute} {
		_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
			Name: project,
			TimeSeries: []*monitoringpb.TimeSeries{
				{
					Metric:   &metric.Metric{Type: metricType},
					Resource: &monitoredres.MonitoredResource{Type: "global"},
					Points: []*monitoringpb.Point{
						{
							Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now.Add(offset))},
							Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: float64(i + 1)}},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Read back — should have 2 points on 1 series.
	resp, err := client.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:   project,
		Filter: fmt.Sprintf(`metric.type = "%s"`, metricType),
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
		t.Fatalf("got %d series, want 1", len(resp.GetTimeSeries()))
	}
	if len(resp.GetTimeSeries()[0].GetPoints()) != 2 {
		t.Fatalf("got %d points, want 2", len(resp.GetTimeSeries()[0].GetPoints()))
	}
}

func TestTimeSeriesFilterWithLabels(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/filter-labels"

	now := time.Now()
	metricType := "custom.googleapis.com/labeled"

	// Write two series with different labels.
	for _, env := range []string{"prod", "staging"} {
		_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
			Name: project,
			TimeSeries: []*monitoringpb.TimeSeries{
				{
					Metric:   &metric.Metric{Type: metricType, Labels: map[string]string{"env": env}},
					Resource: &monitoredres.MonitoredResource{Type: "global"},
					Points: []*monitoringpb.Point{
						{
							Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
							Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(now.Add(-time.Hour)),
		EndTime:   timestamppb.New(now.Add(time.Hour)),
	}

	// Filter by metric type AND label.
	resp, err := client.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     project,
		Filter:   fmt.Sprintf(`metric.type = "%s" AND metric.labels.env = "prod"`, metricType),
		Interval: interval,
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetTimeSeries()) != 1 {
		t.Fatalf("AND filter: got %d series, want 1", len(resp.GetTimeSeries()))
	}
	if resp.GetTimeSeries()[0].GetMetric().GetLabels()["env"] != "prod" {
		t.Errorf("env = %q, want prod", resp.GetTimeSeries()[0].GetMetric().GetLabels()["env"])
	}

	// No filter match.
	resp, err = client.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     project,
		Filter:   fmt.Sprintf(`metric.type = "%s" AND metric.labels.env = "dev"`, metricType),
		Interval: interval,
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetTimeSeries()) != 0 {
		t.Fatalf("no-match filter: got %d series, want 0", len(resp.GetTimeSeries()))
	}
}

func TestTimeSeriesHeadersView(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/view-test"

	now := time.Now()
	_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: project,
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric:   &metric.Metric{Type: "custom.googleapis.com/view_metric"},
				Resource: &monitoredres.MonitoredResource{Type: "global"},
				Points: []*monitoringpb.Point{
					{
						Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
						Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 5.0}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(now.Add(-time.Hour)),
		EndTime:   timestamppb.New(now.Add(time.Hour)),
	}

	// HEADERS view should omit points.
	resp, err := client.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     project,
		Filter:   `metric.type = "custom.googleapis.com/view_metric"`,
		Interval: interval,
		View:     monitoringpb.ListTimeSeriesRequest_HEADERS,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetTimeSeries()) != 1 {
		t.Fatalf("got %d series, want 1", len(resp.GetTimeSeries()))
	}
	if len(resp.GetTimeSeries()[0].GetPoints()) != 0 {
		t.Errorf("HEADERS view: got %d points, want 0", len(resp.GetTimeSeries()[0].GetPoints()))
	}

	// FULL view should include points.
	resp, err = client.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     project,
		Filter:   `metric.type = "custom.googleapis.com/view_metric"`,
		Interval: interval,
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetTimeSeries()[0].GetPoints()) != 1 {
		t.Errorf("FULL view: got %d points, want 1", len(resp.GetTimeSeries()[0].GetPoints()))
	}
}

func TestTimeSeriesProjectIsolation(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	now := time.Now()

	// Write to project A.
	_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/project-a",
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric:   &metric.Metric{Type: "custom.googleapis.com/isolated"},
				Resource: &monitoredres.MonitoredResource{Type: "global"},
				Points: []*monitoringpb.Point{
					{
						Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
						Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// List from project B — should be empty.
	resp, err := client.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:   "projects/project-b",
		Filter: `metric.type = "custom.googleapis.com/isolated"`,
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(now.Add(-time.Hour)),
			EndTime:   timestamppb.New(now.Add(time.Hour)),
		},
		View: monitoringpb.ListTimeSeriesRequest_FULL,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetTimeSeries()) != 0 {
		t.Fatalf("project isolation: got %d series from project-b, want 0", len(resp.GetTimeSeries()))
	}
}

func TestTimeSeriesPagination(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/pagination"
	now := time.Now()

	// Write 3 distinct series.
	for i := 0; i < 3; i++ {
		_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
			Name: project,
			TimeSeries: []*monitoringpb.TimeSeries{
				{
					Metric:   &metric.Metric{Type: fmt.Sprintf("custom.googleapis.com/page_%d", i)},
					Resource: &monitoredres.MonitoredResource{Type: "global"},
					Points: []*monitoringpb.Point{
						{
							Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
							Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: float64(i)}},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(now.Add(-time.Hour)),
		EndTime:   timestamppb.New(now.Add(time.Hour)),
	}

	// Page through with page_size=2.
	resp, err := client.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:     project,
		Filter:   `metric.type = starts_with("custom.googleapis.com/page_")`,
		Interval: interval,
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
		PageSize: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetTimeSeries()) != 2 {
		t.Fatalf("page 1: got %d series, want 2", len(resp.GetTimeSeries()))
	}
	if resp.GetNextPageToken() == "" {
		t.Fatal("page 1: expected next_page_token")
	}

	// Get second page.
	resp, err = client.ListTimeSeries(ctx, &monitoringpb.ListTimeSeriesRequest{
		Name:      project,
		Filter:    `metric.type = starts_with("custom.googleapis.com/page_")`,
		Interval:  interval,
		View:      monitoringpb.ListTimeSeriesRequest_FULL,
		PageSize:  2,
		PageToken: resp.GetNextPageToken(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetTimeSeries()) != 1 {
		t.Fatalf("page 2: got %d series, want 1", len(resp.GetTimeSeries()))
	}
	if resp.GetNextPageToken() != "" {
		t.Errorf("page 2: expected empty next_page_token, got %q", resp.GetNextPageToken())
	}
}

// --- Task 18: PromQL range query and metadata ---

func TestPromQLRangeQuery(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/promql-range"
	projectID := "promql-range"

	now := time.Now().Truncate(time.Second)

	// Write two points at different times.
	for _, offset := range []time.Duration{-2 * time.Minute, -1 * time.Minute} {
		_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
			Name: project,
			TimeSeries: []*monitoringpb.TimeSeries{
				{
					Metric:   &metric.Metric{Type: "custom.googleapis.com/range_metric"},
					Resource: &monitoredres.MonitoredResource{Type: "global"},
					Points: []*monitoringpb.Point{
						{
							Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now.Add(offset))},
							Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 100.0}},
						},
					},
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Range query.
	promURL := fmt.Sprintf("http://127.0.0.1:%d/v1/projects/%s/location/global/prometheus/api/v1/query_range", port, projectID)
	params := url.Values{
		"query": {"custom_googleapis_com:range_metric"},
		"start": {fmt.Sprintf("%d", now.Add(-5*time.Minute).Unix())},
		"end":   {fmt.Sprintf("%d", now.Unix())},
		"step":  {"60"},
	}

	resp, err := http.Get(promURL + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("range query status %d: %s", resp.StatusCode, body)
	}

	var apiResp struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		t.Fatal(err)
	}
	if apiResp.Status != "success" {
		t.Fatalf("status = %q", apiResp.Status)
	}
	if apiResp.Data.ResultType != "matrix" {
		t.Errorf("resultType = %q, want matrix", apiResp.Data.ResultType)
	}
}

func TestPromQLSeriesEndpoint(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/promql-series"
	projectID := "promql-series"

	now := time.Now().Truncate(time.Second)
	_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: project,
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric:   &metric.Metric{Type: "custom.googleapis.com/series_test", Labels: map[string]string{"region": "us"}},
				Resource: &monitoredres.MonitoredResource{Type: "global"},
				Points: []*monitoringpb.Point{
					{
						Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
						Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	promURL := fmt.Sprintf("http://127.0.0.1:%d/v1/projects/%s/location/global/prometheus/api/v1/series", port, projectID)
	params := url.Values{
		"match[]": {`{__name__="custom_googleapis_com:series_test"}`},
		"start":   {fmt.Sprintf("%d", now.Add(-time.Hour).Unix())},
		"end":     {fmt.Sprintf("%d", now.Add(time.Hour).Unix())},
	}

	resp, err := http.Get(promURL + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("series status %d: %s", resp.StatusCode, body)
	}

	var apiResp struct {
		Status string              `json:"status"`
		Data   []map[string]string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		t.Fatal(err)
	}
	if apiResp.Status != "success" {
		t.Fatalf("status = %q", apiResp.Status)
	}
	if len(apiResp.Data) != 1 {
		t.Fatalf("got %d series, want 1", len(apiResp.Data))
	}
	if apiResp.Data[0]["__name__"] != "custom_googleapis_com:series_test" {
		t.Errorf("__name__ = %q", apiResp.Data[0]["__name__"])
	}
	if apiResp.Data[0]["region"] != "us" {
		t.Errorf("region = %q, want us", apiResp.Data[0]["region"])
	}
}

func TestPromQLLabelsEndpoint(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/promql-labels"
	projectID := "promql-labels"

	now := time.Now().Truncate(time.Second)
	_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: project,
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric:   &metric.Metric{Type: "custom.googleapis.com/label_test", Labels: map[string]string{"env": "prod"}},
				Resource: &monitoredres.MonitoredResource{Type: "global"},
				Points: []*monitoringpb.Point{
					{
						Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
						Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// /labels
	labelsParams := url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-time.Hour).Unix())},
		"end":   {fmt.Sprintf("%d", now.Add(time.Hour).Unix())},
	}
	labelsURL := fmt.Sprintf("http://127.0.0.1:%d/v1/projects/%s/location/global/prometheus/api/v1/labels", port, projectID)
	resp, err := http.Get(labelsURL + "?" + labelsParams.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var labelsResp struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&labelsResp); err != nil {
		t.Fatal(err)
	}
	if labelsResp.Status != "success" {
		t.Fatalf("status = %q", labelsResp.Status)
	}

	labelSet := map[string]bool{}
	for _, l := range labelsResp.Data {
		labelSet[l] = true
	}
	if !labelSet["__name__"] {
		t.Error("missing __name__ label")
	}
	if !labelSet["env"] {
		t.Error("missing env label")
	}
}

func TestPromQLLabelValuesEndpoint(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/promql-lv"
	projectID := "promql-lv"

	now := time.Now().Truncate(time.Second)
	_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: project,
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric:   &metric.Metric{Type: "custom.googleapis.com/lv_test"},
				Resource: &monitoredres.MonitoredResource{Type: "global"},
				Points: []*monitoringpb.Point{
					{
						Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(now)},
						Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 1.0}},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// /label/__name__/values
	valuesParams := url.Values{
		"start": {fmt.Sprintf("%d", now.Add(-time.Hour).Unix())},
		"end":   {fmt.Sprintf("%d", now.Add(time.Hour).Unix())},
	}
	valuesURL := fmt.Sprintf("http://127.0.0.1:%d/v1/projects/%s/location/global/prometheus/api/v1/label/__name__/values", port, projectID)
	resp, err := http.Get(valuesURL + "?" + valuesParams.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var valuesResp struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&valuesResp); err != nil {
		t.Fatal(err)
	}
	if valuesResp.Status != "success" {
		t.Fatalf("status = %q", valuesResp.Status)
	}

	found := false
	for _, v := range valuesResp.Data {
		if v == "custom_googleapis_com:lv_test" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("__name__ values = %v, want custom_googleapis_com:lv_test in list", valuesResp.Data)
	}
}

func TestPromQLLabelMapping(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	client := grpcClient(t, port)
	ctx := context.Background()
	project := "projects/promql-mapping"
	projectID := "promql-mapping"

	now := time.Now().Truncate(time.Second)
	_, err := client.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: project,
		TimeSeries: []*monitoringpb.TimeSeries{
			{
				Metric: &metric.Metric{
					Type:   "custom.googleapis.com/mapping_test",
					Labels: map[string]string{"service": "api", "version": "v2"},
				},
				Resource: &monitoredres.MonitoredResource{
					Type:   "gce_instance",
					Labels: map[string]string{"zone": "us-east1-b", "instance_id": "12345"},
				},
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

	// Query and verify label mapping.
	promURL := fmt.Sprintf("http://127.0.0.1:%d/v1/projects/%s/location/global/prometheus/api/v1/query", port, projectID)
	params := url.Values{
		"query": {"custom_googleapis_com:mapping_test"},
		"time":  {fmt.Sprintf("%d", now.Unix())},
	}

	resp, err := http.Get(promURL + "?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var apiResp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		t.Fatal(err)
	}
	if len(apiResp.Data.Result) != 1 {
		t.Fatalf("got %d results, want 1", len(apiResp.Data.Result))
	}

	m := apiResp.Data.Result[0].Metric
	// metric.labels.X → X
	if m["service"] != "api" {
		t.Errorf("service = %q, want api", m["service"])
	}
	if m["version"] != "v2" {
		t.Errorf("version = %q, want v2", m["version"])
	}
	// resource.type → resource_type
	if m["resource_type"] != "gce_instance" {
		t.Errorf("resource_type = %q, want gce_instance", m["resource_type"])
	}
	// resource.labels.X → resource_X
	if m["resource_zone"] != "us-east1-b" {
		t.Errorf("resource_zone = %q, want us-east1-b", m["resource_zone"])
	}
}

// --- Task 19: AlertPolicy REST and edge cases ---

func TestAlertPolicyREST(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	project := "rest-alert"

	// Create via REST.
	policyJSON := `{
		"displayName": "REST Alert",
		"combiner": "OR",
		"conditions": [{
			"displayName": "test condition",
			"conditionThreshold": {
				"filter": "metric.type = \"custom.googleapis.com/test\""
			}
		}]
	}`
	resp, err := http.Post(
		baseURL+"/v3/projects/"+project+"/alertPolicies",
		"application/json",
		jsonReader(policyJSON),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create status %d: %s", resp.StatusCode, body)
	}

	var created struct {
		Name        string `json:"name"`
		DisplayName string `json:"displayName"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created.DisplayName != "REST Alert" {
		t.Errorf("displayName = %q", created.DisplayName)
	}
	if created.Name == "" {
		t.Fatal("created policy has empty name")
	}

	// Get via REST.
	// Name is like "projects/rest-alert/alertPolicies/1", we need just the path after projects.
	policyPath := strings.TrimPrefix(created.Name, "projects/"+project+"/")
	resp2, err := http.Get(baseURL + "/v3/projects/" + project + "/" + policyPath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp2.Body)
		t.Fatalf("get status %d: %s", resp2.StatusCode, body)
	}

	var got struct {
		DisplayName string `json:"displayName"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.DisplayName != "REST Alert" {
		t.Errorf("get displayName = %q", got.DisplayName)
	}
}

func TestAlertPolicyFieldMaskUpdate(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	conn := grpcConn(t, port)
	client := monitoringpb.NewAlertPolicyServiceClient(conn)
	ctx := context.Background()
	project := "projects/mask-test"

	// Create a policy.
	created, err := client.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: project,
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "Original Name",
			Combiner:    monitoringpb.AlertPolicy_OR,
			UserLabels:  map[string]string{"team": "sre", "severity": "high"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Update only display_name via field mask.
	updated, err := client.UpdateAlertPolicy(ctx, &monitoringpb.UpdateAlertPolicyRequest{
		AlertPolicy: &monitoringpb.AlertPolicy{
			Name:        created.GetName(),
			DisplayName: "Updated Name",
		},
		UpdateMask: &fieldmaskpb.FieldMask{
			Paths: []string{"display_name"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// display_name should be updated.
	if updated.GetDisplayName() != "Updated Name" {
		t.Errorf("display_name = %q, want Updated Name", updated.GetDisplayName())
	}
	// user_labels should be preserved (not cleared).
	if updated.GetUserLabels()["team"] != "sre" {
		t.Errorf("user_labels[team] = %q, want sre (should be preserved)", updated.GetUserLabels()["team"])
	}
	if updated.GetUserLabels()["severity"] != "high" {
		t.Errorf("user_labels[severity] = %q, want high (should be preserved)", updated.GetUserLabels()["severity"])
	}
}

func TestAdminResetClearsAlertPolicies(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	conn := grpcConn(t, port)
	alertClient := monitoringpb.NewAlertPolicyServiceClient(conn)
	ctx := context.Background()
	project := "projects/reset-alerts"
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	// Create an alert policy.
	_, err := alertClient.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: project,
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "To Be Reset",
			Combiner:    monitoringpb.AlertPolicy_OR,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Verify it exists.
	listResp, err := alertClient.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{
		Name: project,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.GetAlertPolicies()) != 1 {
		t.Fatalf("before reset: got %d policies, want 1", len(listResp.GetAlertPolicies()))
	}

	// Reset via admin API.
	req, _ := http.NewRequest("POST", baseURL+"/admin/reset", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Verify policies are gone.
	listResp, err = alertClient.ListAlertPolicies(ctx, &monitoringpb.ListAlertPoliciesRequest{
		Name: project,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listResp.GetAlertPolicies()) != 0 {
		t.Errorf("after reset: got %d policies, want 0", len(listResp.GetAlertPolicies()))
	}
}

// --- Issue 19: OAuth2 token endpoint ---

func TestTokenEndpoint(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	// Valid JWT bearer token exchange.
	resp, err := http.PostForm(baseURL+"/token", url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"assertion":  {"eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ0ZXN0QHRlc3QuaWFtLmdzZXJ2aWNlYWNjb3VudC5jb20ifQ.fake-signature"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("token status %d: %s", resp.StatusCode, body)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		t.Fatal(err)
	}
	if tokenResp.AccessToken != "emulator-fake-token" {
		t.Errorf("access_token = %q, want emulator-fake-token", tokenResp.AccessToken)
	}
	if tokenResp.TokenType != "Bearer" {
		t.Errorf("token_type = %q, want Bearer", tokenResp.TokenType)
	}
	if tokenResp.ExpiresIn != 3600 {
		t.Errorf("expires_in = %d, want 3600", tokenResp.ExpiresIn)
	}
}

func TestTokenEndpointUnsupportedGrantType(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	resp, err := http.PostForm(baseURL+"/token", url.Values{
		"grant_type": {"client_credentials"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}

	var errResp struct {
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatal(err)
	}
	if errResp.Error != "unsupported_grant_type" {
		t.Errorf("error = %q, want unsupported_grant_type", errResp.Error)
	}
}

func TestTokenEndpointMethodNotAllowed(t *testing.T) {
	port, cleanup := testServer(t)
	defer cleanup()

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)

	resp, err := http.Get(baseURL + "/token")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		t.Error("GET /token should not return 200")
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
