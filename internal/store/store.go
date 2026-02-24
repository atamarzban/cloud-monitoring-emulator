package store

import (
	"context"

	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
)

// Store defines the storage interface for the Cloud Monitoring emulator.
type Store interface {
	// MetricDescriptor CRUD
	CreateMetricDescriptor(ctx context.Context, project string, md *metric.MetricDescriptor) (*metric.MetricDescriptor, error)
	GetMetricDescriptor(ctx context.Context, name string) (*metric.MetricDescriptor, error)
	ListMetricDescriptors(ctx context.Context, project string, filter string) ([]*metric.MetricDescriptor, error)
	DeleteMetricDescriptor(ctx context.Context, name string) error

	// MonitoredResourceDescriptor (read-only, seeded at startup)
	GetMonitoredResourceDescriptor(ctx context.Context, name string) (*monitoredres.MonitoredResourceDescriptor, error)
	ListMonitoredResourceDescriptors(ctx context.Context, project string, filter string) ([]*monitoredres.MonitoredResourceDescriptor, error)

	// TimeSeries
	CreateTimeSeries(ctx context.Context, project string, timeSeries []*monitoringpb.TimeSeries) error
	ListTimeSeries(ctx context.Context, project string, filter string, interval *monitoringpb.TimeInterval, view monitoringpb.ListTimeSeriesRequest_TimeSeriesView) ([]*monitoringpb.TimeSeries, error)

	// AlertPolicy CRUD
	CreateAlertPolicy(ctx context.Context, project string, policy *monitoringpb.AlertPolicy) (*monitoringpb.AlertPolicy, error)
	GetAlertPolicy(ctx context.Context, name string) (*monitoringpb.AlertPolicy, error)
	ListAlertPolicies(ctx context.Context, project string, filter string) ([]*monitoringpb.AlertPolicy, error)
	UpdateAlertPolicy(ctx context.Context, policy *monitoringpb.AlertPolicy, updateMask *fieldmaskpb.FieldMask) (*monitoringpb.AlertPolicy, error)
	DeleteAlertPolicy(ctx context.Context, name string) error

	// Admin
	Reset()

	// State returns summary statistics for the admin API.
	State() map[string]interface{}
}
