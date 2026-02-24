package server

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"

	"google.golang.org/genproto/googleapis/api/label"
	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/validation"
)

const defaultPageSize = 1000

// MetricServiceServer implements the gRPC MetricService.
type MetricServiceServer struct {
	monitoringpb.UnimplementedMetricServiceServer
	store store.Store
}

func NewMetricServiceServer(s store.Store) *MetricServiceServer {
	return &MetricServiceServer{store: s}
}

func (s *MetricServiceServer) ListMonitoredResourceDescriptors(ctx context.Context, req *monitoringpb.ListMonitoredResourceDescriptorsRequest) (*monitoringpb.ListMonitoredResourceDescriptorsResponse, error) {
	project, err := validation.ParseProjectFromName(req.GetName())
	if err != nil {
		return nil, err
	}

	all, err := s.store.ListMonitoredResourceDescriptors(ctx, project, req.GetFilter())
	if err != nil {
		return nil, err
	}

	page, nextToken := paginate(len(all), req.GetPageSize(), req.GetPageToken())
	result := make([]*monitoredres.MonitoredResourceDescriptor, len(page))
	for i, idx := range page {
		result[i] = all[idx]
	}

	return &monitoringpb.ListMonitoredResourceDescriptorsResponse{
		ResourceDescriptors: result,
		NextPageToken:       nextToken,
	}, nil
}

func (s *MetricServiceServer) GetMonitoredResourceDescriptor(ctx context.Context, req *monitoringpb.GetMonitoredResourceDescriptorRequest) (*monitoredres.MonitoredResourceDescriptor, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	return s.store.GetMonitoredResourceDescriptor(ctx, req.GetName())
}

func (s *MetricServiceServer) ListMetricDescriptors(ctx context.Context, req *monitoringpb.ListMetricDescriptorsRequest) (*monitoringpb.ListMetricDescriptorsResponse, error) {
	project, err := validation.ParseProjectFromName(req.GetName())
	if err != nil {
		return nil, err
	}

	all, err := s.store.ListMetricDescriptors(ctx, project, req.GetFilter())
	if err != nil {
		return nil, err
	}

	page, nextToken := paginate(len(all), req.GetPageSize(), req.GetPageToken())
	result := make([]*metric.MetricDescriptor, len(page))
	for i, idx := range page {
		result[i] = all[idx]
	}

	return &monitoringpb.ListMetricDescriptorsResponse{
		MetricDescriptors: result,
		NextPageToken:     nextToken,
	}, nil
}

func (s *MetricServiceServer) GetMetricDescriptor(ctx context.Context, req *monitoringpb.GetMetricDescriptorRequest) (*metric.MetricDescriptor, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	return s.store.GetMetricDescriptor(ctx, req.GetName())
}

func (s *MetricServiceServer) CreateMetricDescriptor(ctx context.Context, req *monitoringpb.CreateMetricDescriptorRequest) (*metric.MetricDescriptor, error) {
	project, err := validation.ParseProjectFromName(req.GetName())
	if err != nil {
		return nil, err
	}
	md := req.GetMetricDescriptor()
	if md == nil {
		return nil, status.Error(codes.InvalidArgument, "metric_descriptor is required")
	}
	if err := validation.ValidateMetricDescriptor(md); err != nil {
		return nil, err
	}
	return s.store.CreateMetricDescriptor(ctx, project, md)
}

func (s *MetricServiceServer) DeleteMetricDescriptor(ctx context.Context, req *monitoringpb.DeleteMetricDescriptorRequest) (*emptypb.Empty, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if err := s.store.DeleteMetricDescriptor(ctx, req.GetName()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *MetricServiceServer) ListTimeSeries(ctx context.Context, req *monitoringpb.ListTimeSeriesRequest) (*monitoringpb.ListTimeSeriesResponse, error) {
	project, err := validation.ParseProjectFromName(req.GetName())
	if err != nil {
		return nil, err
	}
	if req.GetFilter() == "" {
		return nil, status.Error(codes.InvalidArgument, "filter is required")
	}
	if req.GetInterval() == nil || req.GetInterval().GetEndTime() == nil {
		return nil, status.Error(codes.InvalidArgument, "interval.end_time is required")
	}

	all, err := s.store.ListTimeSeries(ctx, project, req.GetFilter(), req.GetInterval(), req.GetView())
	if err != nil {
		return nil, err
	}

	page, nextToken := paginate(len(all), req.GetPageSize(), req.GetPageToken())
	result := make([]*monitoringpb.TimeSeries, len(page))
	for i, idx := range page {
		result[i] = all[idx]
	}

	return &monitoringpb.ListTimeSeriesResponse{
		TimeSeries:    result,
		NextPageToken: nextToken,
	}, nil
}

func (s *MetricServiceServer) CreateTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest) (*emptypb.Empty, error) {
	if err := validation.ValidateCreateTimeSeriesRequest(req); err != nil {
		return nil, err
	}
	project, _ := validation.ParseProjectFromName(req.GetName()) // already validated

	// Auto-create metric descriptors for custom metrics if they don't exist.
	for _, ts := range req.GetTimeSeries() {
		s.ensureMetricDescriptor(ctx, project, ts)
	}

	if err := s.store.CreateTimeSeries(ctx, project, req.GetTimeSeries()); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *MetricServiceServer) CreateServiceTimeSeries(ctx context.Context, req *monitoringpb.CreateTimeSeriesRequest) (*emptypb.Empty, error) {
	// Same implementation as CreateTimeSeries for the emulator.
	return s.CreateTimeSeries(ctx, req)
}

// ensureMetricDescriptor auto-creates a MetricDescriptor if one doesn't exist for a custom metric.
func (s *MetricServiceServer) ensureMetricDescriptor(ctx context.Context, project string, ts *monitoringpb.TimeSeries) {
	metricType := ts.GetMetric().GetType()
	name := fmt.Sprintf("projects/%s/metricDescriptors/%s", project, metricType)

	// Check if it already exists.
	if _, err := s.store.GetMetricDescriptor(ctx, name); err == nil {
		return
	}

	// Infer kind and type from the TimeSeries.
	kind := ts.GetMetricKind()
	if kind == metric.MetricDescriptor_METRIC_KIND_UNSPECIFIED {
		kind = metric.MetricDescriptor_GAUGE
	}

	valType := ts.GetValueType()
	if valType == metric.MetricDescriptor_VALUE_TYPE_UNSPECIFIED {
		valType = inferValueType(ts)
	}

	// Infer labels from the metric's labels.
	var labels []*label.LabelDescriptor
	for k := range ts.GetMetric().GetLabels() {
		labels = append(labels, &label.LabelDescriptor{
			Key:       k,
			ValueType: label.LabelDescriptor_STRING,
		})
	}

	md := &metric.MetricDescriptor{
		Type:       metricType,
		MetricKind: kind,
		ValueType:  valType,
		Labels:     labels,
	}

	// Best-effort; ignore errors (e.g., ALREADY_EXISTS from concurrent writes).
	s.store.CreateMetricDescriptor(ctx, project, md)
}

func inferValueType(ts *monitoringpb.TimeSeries) metric.MetricDescriptor_ValueType {
	if len(ts.GetPoints()) > 0 {
		v := ts.GetPoints()[0].GetValue()
		switch v.GetValue().(type) {
		case *monitoringpb.TypedValue_BoolValue:
			return metric.MetricDescriptor_BOOL
		case *monitoringpb.TypedValue_Int64Value:
			return metric.MetricDescriptor_INT64
		case *monitoringpb.TypedValue_DoubleValue:
			return metric.MetricDescriptor_DOUBLE
		case *monitoringpb.TypedValue_DistributionValue:
			return metric.MetricDescriptor_DISTRIBUTION
		case *monitoringpb.TypedValue_StringValue:
			return metric.MetricDescriptor_STRING
		}
	}
	return metric.MetricDescriptor_DOUBLE
}

// paginate returns a slice of indices for the current page and the next page token.
func paginate(total int, pageSize int32, pageToken string) (indices []int, nextToken string) {
	ps := int(pageSize)
	if ps <= 0 {
		ps = defaultPageSize
	}

	offset := 0
	if pageToken != "" {
		decoded, err := base64.StdEncoding.DecodeString(pageToken)
		if err == nil {
			offset, _ = strconv.Atoi(string(decoded))
		}
	}

	if offset >= total {
		return nil, ""
	}

	end := offset + ps
	if end > total {
		end = total
	}

	indices = make([]int, end-offset)
	for i := range indices {
		indices[i] = offset + i
	}

	if end < total {
		nextToken = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}
	return indices, nextToken
}
