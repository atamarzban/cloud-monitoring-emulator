package validation

import (
	"fmt"
	"strings"

	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
)

const MaxTimeSeriesPerRequest = 200

// ValidateCreateTimeSeriesRequest validates a CreateTimeSeries request.
func ValidateCreateTimeSeriesRequest(req *monitoringpb.CreateTimeSeriesRequest) error {
	if req.GetName() == "" {
		return status.Error(codes.InvalidArgument, "name is required")
	}
	if _, err := ParseProjectFromName(req.GetName()); err != nil {
		return err
	}
	if len(req.GetTimeSeries()) == 0 {
		return status.Error(codes.InvalidArgument, "timeSeries is required")
	}
	if len(req.GetTimeSeries()) > MaxTimeSeriesPerRequest {
		return status.Errorf(codes.InvalidArgument,
			"request contains %d time series, maximum is %d",
			len(req.GetTimeSeries()), MaxTimeSeriesPerRequest)
	}
	for i, ts := range req.GetTimeSeries() {
		if err := ValidateTimeSeries(ts); err != nil {
			return status.Errorf(codes.InvalidArgument, "timeSeries[%d]: %v", i, err)
		}
	}
	return nil
}

// ValidateTimeSeries validates a single TimeSeries entry in a CreateTimeSeries request.
func ValidateTimeSeries(ts *monitoringpb.TimeSeries) error {
	if ts.GetMetric() == nil {
		return fmt.Errorf("metric is required")
	}
	if ts.GetMetric().GetType() == "" {
		return fmt.Errorf("metric.type is required")
	}
	if ts.GetResource() == nil {
		return fmt.Errorf("resource is required")
	}
	if ts.GetResource().GetType() == "" {
		return fmt.Errorf("resource.type is required")
	}
	if len(ts.GetPoints()) != 1 {
		return fmt.Errorf("exactly one point is required, got %d", len(ts.GetPoints()))
	}
	p := ts.GetPoints()[0]
	if p.GetInterval() == nil || p.GetInterval().GetEndTime() == nil {
		return fmt.Errorf("point interval.end_time is required")
	}
	if p.GetValue() == nil {
		return fmt.Errorf("point value is required")
	}
	return nil
}

// ValidateMetricDescriptor validates a MetricDescriptor for creation.
func ValidateMetricDescriptor(md *metric.MetricDescriptor) error {
	if md.GetType() == "" {
		return status.Error(codes.InvalidArgument, "type is required")
	}
	if !strings.HasPrefix(md.GetType(), "custom.googleapis.com/") &&
		!strings.HasPrefix(md.GetType(), "external.googleapis.com/") {
		return status.Errorf(codes.InvalidArgument,
			"type must start with 'custom.googleapis.com/' or 'external.googleapis.com/', got %q",
			md.GetType())
	}
	if md.GetMetricKind() == metric.MetricDescriptor_METRIC_KIND_UNSPECIFIED {
		return status.Error(codes.InvalidArgument, "metricKind is required")
	}
	if md.GetValueType() == metric.MetricDescriptor_VALUE_TYPE_UNSPECIFIED {
		return status.Error(codes.InvalidArgument, "valueType is required")
	}
	return nil
}

// ParseProjectFromName extracts the project ID from a resource name like "projects/{project}".
func ParseProjectFromName(name string) (string, error) {
	if !strings.HasPrefix(name, "projects/") {
		return "", status.Errorf(codes.InvalidArgument, "name must start with 'projects/', got %q", name)
	}
	project := strings.TrimPrefix(name, "projects/")
	// Handle cases like "projects/my-proj/timeSeries" — take only the project segment.
	if idx := strings.IndexByte(project, '/'); idx >= 0 {
		project = project[:idx]
	}
	if project == "" {
		return "", status.Error(codes.InvalidArgument, "project ID is empty")
	}
	return project, nil
}

// ParseMetricDescriptorName extracts project and metric type from
// "projects/{project}/metricDescriptors/{type}".
func ParseMetricDescriptorName(name string) (project, metricType string, err error) {
	const prefix = "projects/"
	const segment = "/metricDescriptors/"

	if !strings.HasPrefix(name, prefix) {
		return "", "", status.Errorf(codes.InvalidArgument, "invalid metric descriptor name: %q", name)
	}
	rest := name[len(prefix):]
	idx := strings.Index(rest, segment)
	if idx < 0 {
		return "", "", status.Errorf(codes.InvalidArgument, "invalid metric descriptor name: %q", name)
	}
	project = rest[:idx]
	metricType = rest[idx+len(segment):]
	if project == "" || metricType == "" {
		return "", "", status.Errorf(codes.InvalidArgument, "invalid metric descriptor name: %q", name)
	}
	return project, metricType, nil
}

// ParseMonitoredResourceDescriptorName extracts project and resource type from
// "projects/{project}/monitoredResourceDescriptors/{type}".
func ParseMonitoredResourceDescriptorName(name string) (project, resourceType string, err error) {
	const prefix = "projects/"
	const segment = "/monitoredResourceDescriptors/"

	if !strings.HasPrefix(name, prefix) {
		return "", "", status.Errorf(codes.InvalidArgument, "invalid name: %q", name)
	}
	rest := name[len(prefix):]
	idx := strings.Index(rest, segment)
	if idx < 0 {
		return "", "", status.Errorf(codes.InvalidArgument, "invalid name: %q", name)
	}
	project = rest[:idx]
	resourceType = rest[idx+len(segment):]
	if project == "" || resourceType == "" {
		return "", "", status.Errorf(codes.InvalidArgument, "invalid name: %q", name)
	}
	return project, resourceType, nil
}
