package promql

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"google.golang.org/protobuf/types/known/timestamppb"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
)

// MetricTypeToPromName converts a GCP metric type to a Prometheus-compatible __name__.
//
// Domain dots become underscores, the first slash becomes a colon,
// and subsequent path slashes become underscores.
//
// Example: "custom.googleapis.com/my_metric" → "custom_googleapis_com:my_metric"
// Example: "compute.googleapis.com/instance/cpu/utilization" → "compute_googleapis_com:instance_cpu_utilization"
func MetricTypeToPromName(metricType string) string {
	idx := strings.Index(metricType, "/")
	if idx < 0 {
		return strings.ReplaceAll(metricType, ".", "_")
	}
	domain := metricType[:idx]
	path := metricType[idx+1:]
	return strings.ReplaceAll(domain, ".", "_") + ":" + strings.ReplaceAll(path, "/", "_")
}

// PromNameToMetricType converts a Prometheus __name__ back to a GCP metric type.
//
// The colon separates domain from path; domain underscores become dots,
// path underscores become slashes.
//
// NOTE: This is lossy — if the original metric type had underscores in the
// domain or path, they cannot be distinguished from converted characters.
// For the emulator this is acceptable since we can also match by iterating.
func PromNameToMetricType(promName string) string {
	idx := strings.Index(promName, ":")
	if idx < 0 {
		return strings.ReplaceAll(promName, "_", ".")
	}
	domain := promName[:idx]
	path := promName[idx+1:]
	return strings.ReplaceAll(domain, "_", ".") + "/" + strings.ReplaceAll(path, "_", "/")
}

// StoreQueryable implements storage.Queryable backed by our Store, scoped to a GCP project.
type StoreQueryable struct {
	Store   store.Store
	Project string
}

func (q *StoreQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	return &storeQuerier{
		store:   q.Store,
		project: q.Project,
		mint:    mint,
		maxt:    maxt,
	}, nil
}

type storeQuerier struct {
	store   store.Store
	project string
	mint    int64 // milliseconds since epoch
	maxt    int64 // milliseconds since epoch
}

func (q *storeQuerier) Select(ctx context.Context, sortSeries bool, _ *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	interval := q.timeInterval()

	allTS, err := q.store.ListTimeSeries(ctx, q.project, "", interval, monitoringpb.ListTimeSeriesRequest_FULL)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	var result []storage.Series
	for _, ts := range allTS {
		promLabels := GCPToPromLabels(ts)
		if !matchAll(promLabels, matchers) {
			continue
		}

		// ListTimeSeries returns points newest-first; Prometheus needs oldest-first.
		points := ts.GetPoints()
		samples := make([]sample, len(points))
		for i, p := range points {
			ri := len(points) - 1 - i
			samples[ri] = sample{
				t: p.GetInterval().GetEndTime().AsTime().UnixMilli(),
				v: extractFloat64(p.GetValue()),
			}
		}

		result = append(result, &storeSeries{
			labels:  promLabels,
			samples: samples,
		})
	}

	if sortSeries {
		sort.Slice(result, func(i, j int) bool {
			return labels.Compare(result[i].Labels(), result[j].Labels()) < 0
		})
	}

	return newSeriesSet(result)
}

func (q *storeQuerier) LabelValues(ctx context.Context, name string, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	interval := q.timeInterval()

	allTS, err := q.store.ListTimeSeries(ctx, q.project, "", interval, monitoringpb.ListTimeSeriesRequest_HEADERS)
	if err != nil {
		return nil, nil, err
	}

	seen := map[string]struct{}{}
	for _, ts := range allTS {
		promLabels := GCPToPromLabels(ts)
		if !matchAll(promLabels, matchers) {
			continue
		}
		if v := promLabels.Get(name); v != "" {
			seen[v] = struct{}{}
		}
	}

	values := make([]string, 0, len(seen))
	for v := range seen {
		values = append(values, v)
	}
	sort.Strings(values)
	return values, nil, nil
}

func (q *storeQuerier) LabelNames(ctx context.Context, _ *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	interval := q.timeInterval()

	allTS, err := q.store.ListTimeSeries(ctx, q.project, "", interval, monitoringpb.ListTimeSeriesRequest_HEADERS)
	if err != nil {
		return nil, nil, err
	}

	seen := map[string]struct{}{}
	for _, ts := range allTS {
		promLabels := GCPToPromLabels(ts)
		if !matchAll(promLabels, matchers) {
			continue
		}
		promLabels.Range(func(l labels.Label) {
			seen[l.Name] = struct{}{}
		})
	}

	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, nil, nil
}

func (q *storeQuerier) Close() error {
	return nil
}

func (q *storeQuerier) timeInterval() *monitoringpb.TimeInterval {
	return &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(time.UnixMilli(q.mint)),
		EndTime:   timestamppb.New(time.UnixMilli(q.maxt)),
	}
}

// GCPToPromLabels converts a GCP TimeSeries to Prometheus labels.
//
// Mapping:
//   - metric.type       → __name__ (sanitized)
//   - metric.labels.X   → X
//   - resource.type     → resource_type
//   - resource.labels.X → resource_X
func GCPToPromLabels(ts *monitoringpb.TimeSeries) labels.Labels {
	b := labels.NewBuilder(labels.EmptyLabels())

	b.Set(labels.MetricName, MetricTypeToPromName(ts.GetMetric().GetType()))

	for k, v := range ts.GetMetric().GetLabels() {
		b.Set(k, v)
	}

	b.Set("resource_type", ts.GetResource().GetType())

	for k, v := range ts.GetResource().GetLabels() {
		b.Set("resource_"+k, v)
	}

	return b.Labels()
}

func matchAll(lset labels.Labels, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if !m.Matches(lset.Get(m.Name)) {
			return false
		}
	}
	return true
}

func extractFloat64(v *monitoringpb.TypedValue) float64 {
	switch vv := v.GetValue().(type) {
	case *monitoringpb.TypedValue_DoubleValue:
		return vv.DoubleValue
	case *monitoringpb.TypedValue_Int64Value:
		return float64(vv.Int64Value)
	case *monitoringpb.TypedValue_BoolValue:
		if vv.BoolValue {
			return 1.0
		}
		return 0.0
	default:
		return 0.0
	}
}

// --- SeriesSet ---

type storeSeriesSet struct {
	series []storage.Series
	idx    int
}

func newSeriesSet(series []storage.Series) storage.SeriesSet {
	return &storeSeriesSet{series: series, idx: -1}
}

func (s *storeSeriesSet) Next() bool {
	s.idx++
	return s.idx < len(s.series)
}

func (s *storeSeriesSet) At() storage.Series {
	return s.series[s.idx]
}

func (s *storeSeriesSet) Err() error {
	return nil
}

func (s *storeSeriesSet) Warnings() annotations.Annotations {
	return nil
}

// --- Series ---

type sample struct {
	t int64
	v float64
}

type storeSeries struct {
	labels  labels.Labels
	samples []sample
}

func (s *storeSeries) Labels() labels.Labels {
	return s.labels
}

func (s *storeSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	return &sampleIterator{samples: s.samples, idx: -1}
}

// --- Sample Iterator ---

type sampleIterator struct {
	samples []sample
	idx     int
}

func (it *sampleIterator) Next() chunkenc.ValueType {
	it.idx++
	if it.idx >= len(it.samples) {
		return chunkenc.ValNone
	}
	return chunkenc.ValFloat
}

func (it *sampleIterator) Seek(t int64) chunkenc.ValueType {
	// If current position already satisfies, no-op.
	if it.idx >= 0 && it.idx < len(it.samples) && it.samples[it.idx].t >= t {
		return chunkenc.ValFloat
	}
	// Linear scan forward from current position.
	start := it.idx + 1
	if start < 0 {
		start = 0
	}
	for i := start; i < len(it.samples); i++ {
		if it.samples[i].t >= t {
			it.idx = i
			return chunkenc.ValFloat
		}
	}
	it.idx = len(it.samples)
	return chunkenc.ValNone
}

func (it *sampleIterator) At() (int64, float64) {
	s := it.samples[it.idx]
	return s.t, s.v
}

func (it *sampleIterator) AtHistogram(_ *histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, nil
}

func (it *sampleIterator) AtFloatHistogram(_ *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, nil
}

func (it *sampleIterator) AtT() int64 {
	return it.samples[it.idx].t
}

func (it *sampleIterator) Err() error {
	return nil
}
