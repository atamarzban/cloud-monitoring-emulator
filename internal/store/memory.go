package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"google.golang.org/genproto/googleapis/api/label"
	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/filter"
)

// storedTimeSeries holds a time series and its accumulated points.
type storedTimeSeries struct {
	metric     *metric.Metric
	resource   *monitoredres.MonitoredResource
	metricKind metric.MetricDescriptor_MetricKind
	valueType  metric.MetricDescriptor_ValueType
	unit       string
	points     []*monitoringpb.Point // sorted by endTime ascending
}

// seriesKey computes a unique identity for a time series.
func seriesKey(m *metric.Metric, r *monitoredres.MonitoredResource) string {
	var b strings.Builder
	b.WriteString(m.GetType())
	b.WriteByte('|')

	// Sort metric label keys for determinism.
	keys := make([]string, 0, len(m.GetLabels()))
	for k := range m.GetLabels() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(m.GetLabels()[k])
		b.WriteByte(',')
	}
	b.WriteByte('|')
	b.WriteString(r.GetType())
	b.WriteByte('|')

	// Sort resource label keys for determinism.
	keys = keys[:0]
	for k := range r.GetLabels() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(r.GetLabels()[k])
		b.WriteByte(',')
	}
	return b.String()
}

// MemoryStore implements Store with in-memory maps.
type MemoryStore struct {
	mu sync.RWMutex

	// metricDescriptors: map[fullName]*MetricDescriptor
	metricDescriptors map[string]*metric.MetricDescriptor

	// monitoredResourceDescriptors: map[resourceType]*MonitoredResourceDescriptor
	monitoredResourceDescriptors map[string]*monitoredres.MonitoredResourceDescriptor

	// timeSeries: map[projectID] -> map[seriesKey] -> *storedTimeSeries
	timeSeries map[string]map[string]*storedTimeSeries

	// alertPolicies: map[fullName]*AlertPolicy
	alertPolicies     map[string]*monitoringpb.AlertPolicy
	nextAlertPolicyID int64
}

func NewMemoryStore() *MemoryStore {
	s := &MemoryStore{
		metricDescriptors:            make(map[string]*metric.MetricDescriptor),
		monitoredResourceDescriptors: make(map[string]*monitoredres.MonitoredResourceDescriptor),
		timeSeries:                   make(map[string]map[string]*storedTimeSeries),
		alertPolicies:                make(map[string]*monitoringpb.AlertPolicy),
	}
	s.seedMonitoredResourceDescriptors()
	return s
}

func (s *MemoryStore) seedMonitoredResourceDescriptors() {
	descriptors := []*monitoredres.MonitoredResourceDescriptor{
		{
			Type:        "global",
			DisplayName: "Global",
			Description: "A global resource.",
			Labels: []*label.LabelDescriptor{
				{Key: "project_id", ValueType: label.LabelDescriptor_STRING, Description: "The identifier of the Google Cloud project."},
			},
		},
		{
			Type:        "gce_instance",
			DisplayName: "GCE VM Instance",
			Description: "A virtual machine instance hosted in Google Compute Engine.",
			Labels: []*label.LabelDescriptor{
				{Key: "project_id", ValueType: label.LabelDescriptor_STRING, Description: "The identifier of the Google Cloud project."},
				{Key: "instance_id", ValueType: label.LabelDescriptor_STRING, Description: "The numeric VM instance identifier."},
				{Key: "zone", ValueType: label.LabelDescriptor_STRING, Description: "The Compute Engine zone."},
			},
		},
		{
			Type:        "k8s_container",
			DisplayName: "Kubernetes Container",
			Description: "A Kubernetes container instance.",
			Labels: []*label.LabelDescriptor{
				{Key: "project_id", ValueType: label.LabelDescriptor_STRING},
				{Key: "location", ValueType: label.LabelDescriptor_STRING},
				{Key: "cluster_name", ValueType: label.LabelDescriptor_STRING},
				{Key: "namespace_name", ValueType: label.LabelDescriptor_STRING},
				{Key: "pod_name", ValueType: label.LabelDescriptor_STRING},
				{Key: "container_name", ValueType: label.LabelDescriptor_STRING},
			},
		},
		{
			Type:        "k8s_pod",
			DisplayName: "Kubernetes Pod",
			Description: "A Kubernetes pod.",
			Labels: []*label.LabelDescriptor{
				{Key: "project_id", ValueType: label.LabelDescriptor_STRING},
				{Key: "location", ValueType: label.LabelDescriptor_STRING},
				{Key: "cluster_name", ValueType: label.LabelDescriptor_STRING},
				{Key: "namespace_name", ValueType: label.LabelDescriptor_STRING},
				{Key: "pod_name", ValueType: label.LabelDescriptor_STRING},
			},
		},
		{
			Type:        "k8s_node",
			DisplayName: "Kubernetes Node",
			Description: "A Kubernetes node.",
			Labels: []*label.LabelDescriptor{
				{Key: "project_id", ValueType: label.LabelDescriptor_STRING},
				{Key: "location", ValueType: label.LabelDescriptor_STRING},
				{Key: "cluster_name", ValueType: label.LabelDescriptor_STRING},
				{Key: "node_name", ValueType: label.LabelDescriptor_STRING},
			},
		},
		{
			Type:        "k8s_cluster",
			DisplayName: "Kubernetes Cluster",
			Description: "A Kubernetes cluster.",
			Labels: []*label.LabelDescriptor{
				{Key: "project_id", ValueType: label.LabelDescriptor_STRING},
				{Key: "location", ValueType: label.LabelDescriptor_STRING},
				{Key: "cluster_name", ValueType: label.LabelDescriptor_STRING},
			},
		},
		{
			Type:        "gae_app",
			DisplayName: "GAE Application",
			Description: "An App Engine application.",
			Labels: []*label.LabelDescriptor{
				{Key: "project_id", ValueType: label.LabelDescriptor_STRING},
				{Key: "module_id", ValueType: label.LabelDescriptor_STRING},
				{Key: "version_id", ValueType: label.LabelDescriptor_STRING},
				{Key: "zone", ValueType: label.LabelDescriptor_STRING},
			},
		},
		{
			Type:        "cloud_run_revision",
			DisplayName: "Cloud Run Revision",
			Description: "A Cloud Run revision.",
			Labels: []*label.LabelDescriptor{
				{Key: "project_id", ValueType: label.LabelDescriptor_STRING},
				{Key: "location", ValueType: label.LabelDescriptor_STRING},
				{Key: "service_name", ValueType: label.LabelDescriptor_STRING},
				{Key: "revision_name", ValueType: label.LabelDescriptor_STRING},
				{Key: "configuration_name", ValueType: label.LabelDescriptor_STRING},
			},
		},
	}
	for _, d := range descriptors {
		s.monitoredResourceDescriptors[d.Type] = d
	}
}

func (s *MemoryStore) CreateMetricDescriptor(_ context.Context, project string, md *metric.MetricDescriptor) (*metric.MetricDescriptor, error) {
	name := fmt.Sprintf("projects/%s/metricDescriptors/%s", project, md.GetType())

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.metricDescriptors[name]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "metric descriptor %q already exists", name)
	}

	stored := proto.Clone(md).(*metric.MetricDescriptor)
	stored.Name = name
	s.metricDescriptors[name] = stored
	return proto.Clone(stored).(*metric.MetricDescriptor), nil
}

func (s *MemoryStore) GetMetricDescriptor(_ context.Context, name string) (*metric.MetricDescriptor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	md, ok := s.metricDescriptors[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "metric descriptor %q not found", name)
	}
	return proto.Clone(md).(*metric.MetricDescriptor), nil
}

func (s *MemoryStore) ListMetricDescriptors(_ context.Context, project string, filterStr string) ([]*metric.MetricDescriptor, error) {
	prefix := fmt.Sprintf("projects/%s/metricDescriptors/", project)

	s.mu.RLock()
	defer s.mu.RUnlock()

	expr, err := filter.Parse(filterStr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid filter: %v", err)
	}

	var result []*metric.MetricDescriptor
	for name, md := range s.metricDescriptors {
		if strings.HasPrefix(name, prefix) {
			if !filter.MatchMetricDescriptor(expr, md) {
				continue
			}
			result = append(result, proto.Clone(md).(*metric.MetricDescriptor))
		}
	}
	return result, nil
}

func (s *MemoryStore) DeleteMetricDescriptor(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.metricDescriptors[name]; !ok {
		return status.Errorf(codes.NotFound, "metric descriptor %q not found", name)
	}
	delete(s.metricDescriptors, name)
	return nil
}

func (s *MemoryStore) GetMonitoredResourceDescriptor(_ context.Context, name string) (*monitoredres.MonitoredResourceDescriptor, error) {
	// name format: "projects/{project}/monitoredResourceDescriptors/{type}"
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[2] != "monitoredResourceDescriptors" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid monitored resource descriptor name: %q", name)
	}
	resourceType := parts[3]

	s.mu.RLock()
	defer s.mu.RUnlock()

	md, ok := s.monitoredResourceDescriptors[resourceType]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "monitored resource descriptor %q not found", name)
	}

	result := proto.Clone(md).(*monitoredres.MonitoredResourceDescriptor)
	result.Name = name
	return result, nil
}

func (s *MemoryStore) ListMonitoredResourceDescriptors(_ context.Context, project string, _ string) ([]*monitoredres.MonitoredResourceDescriptor, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*monitoredres.MonitoredResourceDescriptor, 0, len(s.monitoredResourceDescriptors))
	for _, md := range s.monitoredResourceDescriptors {
		d := proto.Clone(md).(*monitoredres.MonitoredResourceDescriptor)
		d.Name = fmt.Sprintf("projects/%s/monitoredResourceDescriptors/%s", project, md.Type)
		result = append(result, d)
	}
	return result, nil
}

func (s *MemoryStore) CreateTimeSeries(_ context.Context, project string, timeSeries []*monitoringpb.TimeSeries) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.timeSeries[project]; !ok {
		s.timeSeries[project] = make(map[string]*storedTimeSeries)
	}
	projectStore := s.timeSeries[project]

	for _, ts := range timeSeries {
		key := seriesKey(ts.GetMetric(), ts.GetResource())

		existing, ok := projectStore[key]
		if !ok {
			existing = &storedTimeSeries{
				metric:     proto.Clone(ts.GetMetric()).(*metric.Metric),
				resource:   proto.Clone(ts.GetResource()).(*monitoredres.MonitoredResource),
				metricKind: ts.GetMetricKind(),
				valueType:  ts.GetValueType(),
				unit:       ts.GetUnit(),
			}
			projectStore[key] = existing
		}

		for _, p := range ts.GetPoints() {
			newEndTime := p.GetInterval().GetEndTime().AsTime()

			// Enforce chronological ordering: new point must be newer than the latest.
			if len(existing.points) > 0 {
				lastEndTime := existing.points[len(existing.points)-1].GetInterval().GetEndTime().AsTime()
				if !newEndTime.After(lastEndTime) {
					return status.Errorf(codes.InvalidArgument,
						"time series %s: new point end_time %v must be after latest end_time %v",
						ts.GetMetric().GetType(), newEndTime, lastEndTime)
				}
			}
			existing.points = append(existing.points, proto.Clone(p).(*monitoringpb.Point))
		}
	}
	return nil
}

func (s *MemoryStore) ListTimeSeries(_ context.Context, project string, filterStr string, interval *monitoringpb.TimeInterval, view monitoringpb.ListTimeSeriesRequest_TimeSeriesView) ([]*monitoringpb.TimeSeries, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	projectStore, ok := s.timeSeries[project]
	if !ok {
		return nil, nil
	}

	expr, err := filter.Parse(filterStr)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid filter: %v", err)
	}

	intervalStart := interval.GetStartTime().AsTime()
	intervalEnd := interval.GetEndTime().AsTime()

	var result []*monitoringpb.TimeSeries
	for _, sts := range projectStore {
		// Build a synthetic TimeSeries for filter matching.
		candidate := &monitoringpb.TimeSeries{
			Metric:   sts.metric,
			Resource: sts.resource,
		}
		if !filter.MatchTimeSeries(expr, candidate) {
			continue
		}

		// Filter points by time interval.
		var filteredPoints []*monitoringpb.Point
		for _, p := range sts.points {
			pointEnd := p.GetInterval().GetEndTime().AsTime()
			if (pointEnd.Equal(intervalStart) || pointEnd.After(intervalStart)) &&
				(pointEnd.Equal(intervalEnd) || pointEnd.Before(intervalEnd)) {
				filteredPoints = append(filteredPoints, proto.Clone(p).(*monitoringpb.Point))
			}
		}

		if len(filteredPoints) == 0 {
			continue
		}

		// Reverse points: API returns in reverse time order.
		for i, j := 0, len(filteredPoints)-1; i < j; i, j = i+1, j-1 {
			filteredPoints[i], filteredPoints[j] = filteredPoints[j], filteredPoints[i]
		}

		ts := &monitoringpb.TimeSeries{
			Metric:     proto.Clone(sts.metric).(*metric.Metric),
			Resource:   proto.Clone(sts.resource).(*monitoredres.MonitoredResource),
			MetricKind: sts.metricKind,
			ValueType:  sts.valueType,
			Unit:       sts.unit,
		}

		if view == monitoringpb.ListTimeSeriesRequest_FULL {
			ts.Points = filteredPoints
		}

		result = append(result, ts)
	}
	return result, nil
}


func (s *MemoryStore) CreateAlertPolicy(_ context.Context, project string, policy *monitoringpb.AlertPolicy) (*monitoringpb.AlertPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nextAlertPolicyID++
	id := fmt.Sprintf("%d", s.nextAlertPolicyID)
	name := fmt.Sprintf("projects/%s/alertPolicies/%s", project, id)

	stored := proto.Clone(policy).(*monitoringpb.AlertPolicy)
	stored.Name = name
	now := timestamppb.Now()
	stored.CreationRecord = &monitoringpb.MutationRecord{MutateTime: now, MutatedBy: "emulator"}
	stored.MutationRecord = &monitoringpb.MutationRecord{MutateTime: now, MutatedBy: "emulator"}

	// Auto-generate condition names.
	for i, c := range stored.GetConditions() {
		if c.GetName() == "" {
			c.Name = fmt.Sprintf("%s/conditions/%d", name, i)
		}
	}

	s.alertPolicies[name] = stored
	return proto.Clone(stored).(*monitoringpb.AlertPolicy), nil
}

func (s *MemoryStore) GetAlertPolicy(_ context.Context, name string) (*monitoringpb.AlertPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	p, ok := s.alertPolicies[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "alert policy %q not found", name)
	}
	return proto.Clone(p).(*monitoringpb.AlertPolicy), nil
}

func (s *MemoryStore) ListAlertPolicies(_ context.Context, project string, filterStr string) ([]*monitoringpb.AlertPolicy, error) {
	prefix := fmt.Sprintf("projects/%s/alertPolicies/", project)

	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*monitoringpb.AlertPolicy
	for name, p := range s.alertPolicies {
		if strings.HasPrefix(name, prefix) {
			_ = filterStr // AlertPolicy filter is not well-defined; return all for now.
			result = append(result, proto.Clone(p).(*monitoringpb.AlertPolicy))
		}
	}
	return result, nil
}

func (s *MemoryStore) UpdateAlertPolicy(_ context.Context, policy *monitoringpb.AlertPolicy, updateMask *fieldmaskpb.FieldMask) (*monitoringpb.AlertPolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	name := policy.GetName()
	existing, ok := s.alertPolicies[name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "alert policy %q not found", name)
	}

	if updateMask == nil || len(updateMask.GetPaths()) == 0 {
		// Replace the entire policy, preserving server-managed fields.
		updated := proto.Clone(policy).(*monitoringpb.AlertPolicy)
		updated.CreationRecord = existing.CreationRecord
		updated.MutationRecord = &monitoringpb.MutationRecord{MutateTime: timestamppb.Now(), MutatedBy: "emulator"}
		s.alertPolicies[name] = updated
		return proto.Clone(updated).(*monitoringpb.AlertPolicy), nil
	}

	// Apply field mask: update only specified fields.
	updated := proto.Clone(existing).(*monitoringpb.AlertPolicy)
	for _, path := range updateMask.GetPaths() {
		switch path {
		case "display_name":
			updated.DisplayName = policy.GetDisplayName()
		case "documentation":
			updated.Documentation = policy.GetDocumentation()
		case "user_labels":
			updated.UserLabels = policy.GetUserLabels()
		case "conditions":
			updated.Conditions = policy.GetConditions()
		case "combiner":
			updated.Combiner = policy.GetCombiner()
		case "enabled":
			updated.Enabled = policy.GetEnabled()
		case "notification_channels":
			updated.NotificationChannels = policy.GetNotificationChannels()
		case "alert_strategy":
			updated.AlertStrategy = policy.GetAlertStrategy()
		case "severity":
			updated.Severity = policy.GetSeverity()
		}
	}
	updated.MutationRecord = &monitoringpb.MutationRecord{MutateTime: timestamppb.Now(), MutatedBy: "emulator"}
	s.alertPolicies[name] = updated
	return proto.Clone(updated).(*monitoringpb.AlertPolicy), nil
}

func (s *MemoryStore) DeleteAlertPolicy(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.alertPolicies[name]; !ok {
		return status.Errorf(codes.NotFound, "alert policy %q not found", name)
	}
	delete(s.alertPolicies, name)
	return nil
}

func (s *MemoryStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metricDescriptors = make(map[string]*metric.MetricDescriptor)
	s.timeSeries = make(map[string]map[string]*storedTimeSeries)
	s.alertPolicies = make(map[string]*monitoringpb.AlertPolicy)
	s.nextAlertPolicyID = 0
}

func (s *MemoryStore) State() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	projectStats := make(map[string]interface{})
	for project, series := range s.timeSeries {
		totalPoints := 0
		for _, sts := range series {
			totalPoints += len(sts.points)
		}
		projectStats[project] = map[string]interface{}{
			"time_series_count": len(series),
			"total_points":     totalPoints,
		}
	}

	return map[string]interface{}{
		"metric_descriptors":             len(s.metricDescriptors),
		"monitored_resource_descriptors": len(s.monitoredResourceDescriptors),
		"alert_policies":                 len(s.alertPolicies),
		"projects":                       projectStats,
	}
}
