package promql

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/ata-marzban/cloud-monitoring-emulator/internal/store"
)

// apiResponse is the standard Prometheus API response envelope.
type apiResponse struct {
	Status    string      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType string      `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
}

type queryData struct {
	ResultType string      `json:"resultType"`
	Result     interface{} `json:"result"`
}

type vectorItem struct {
	Metric map[string]string `json:"metric"`
	Value  [2]interface{}    `json:"value"`
}

type matrixItem struct {
	Metric map[string]string `json:"metric"`
	Values [][2]interface{}  `json:"values"`
}

// Handler serves the Prometheus-compatible HTTP API backed by our store.
//
// Path pattern: /v1/projects/{project}/location/{location}/prometheus/api/v1/{endpoint}
type Handler struct {
	store  store.Store
	engine *promql.Engine
}

// NewHandler creates a new Prometheus API handler backed by the given store.
func NewHandler(s store.Store) http.Handler {
	h := &Handler{
		store: s,
		engine: promql.NewEngine(promql.EngineOpts{
			MaxSamples:           50000000,
			Timeout:              2 * time.Minute,
			EnableAtModifier:     true,
			EnableNegativeOffset: true,
			LookbackDelta:       5 * time.Minute,
		}),
	}
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	project, endpoint, ok := parsePromPath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusNotFound, "bad_data", "invalid Prometheus API path")
		return
	}

	switch endpoint {
	case "query":
		h.query(w, r, project)
	case "query_range":
		h.queryRange(w, r, project)
	case "series":
		h.series(w, r, project)
	case "labels":
		h.labelNames(w, r, project)
	case "query_exemplars":
		h.queryExemplars(w, r)
	case "metadata":
		h.metadata(w, r)
	default:
		if name, found := parseLabelValuesEndpoint(endpoint); found {
			h.labelValues(w, r, project, name)
			return
		}
		writeError(w, http.StatusNotFound, "bad_data", "unknown endpoint: "+endpoint)
	}
}

// --- Endpoint handlers ---

func (h *Handler) query(w http.ResponseWriter, r *http.Request, project string) {
	qs := r.FormValue("query")
	if qs == "" {
		writeError(w, http.StatusBadRequest, "bad_data", "missing query parameter")
		return
	}

	ts, err := parseTime(r.FormValue("time"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid time: %v", err))
		return
	}

	queryable := &StoreQueryable{Store: h.store, Project: project}
	qry, err := h.engine.NewInstantQuery(r.Context(), queryable, nil, qs, ts)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	defer qry.Close()

	res := qry.Exec(r.Context())
	if res.Err != nil {
		writeError(w, http.StatusUnprocessableEntity, "execution", res.Err.Error())
		return
	}

	writeQueryResult(w, res)
}

func (h *Handler) queryRange(w http.ResponseWriter, r *http.Request, project string) {
	qs := r.FormValue("query")
	if qs == "" {
		writeError(w, http.StatusBadRequest, "bad_data", "missing query parameter")
		return
	}

	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid start: %v", err))
		return
	}
	end, err := parseTime(r.FormValue("end"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid end: %v", err))
		return
	}
	step, err := parseDuration(r.FormValue("step"))
	if err != nil || step <= 0 {
		writeError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid step: %v", err))
		return
	}

	queryable := &StoreQueryable{Store: h.store, Project: project}
	qry, err := h.engine.NewRangeQuery(r.Context(), queryable, nil, qs, start, end, step)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	defer qry.Close()

	res := qry.Exec(r.Context())
	if res.Err != nil {
		writeError(w, http.StatusUnprocessableEntity, "execution", res.Err.Error())
		return
	}

	writeQueryResult(w, res)
}

func (h *Handler) series(w http.ResponseWriter, r *http.Request, project string) {
	if err := r.ParseForm(); err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	matcherSets := r.Form["match[]"]
	if len(matcherSets) == 0 {
		writeError(w, http.StatusBadRequest, "bad_data", "no match[] parameter provided")
		return
	}

	start, end := parseTimeRange(r)
	queryable := &StoreQueryable{Store: h.store, Project: project}
	querier, err := queryable.Querier(start.UnixMilli(), end.UnixMilli())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "execution", err.Error())
		return
	}
	defer querier.Close()

	seen := map[uint64]struct{}{}
	var result []map[string]string

	for _, ms := range matcherSets {
		matchers, err := parser.ParseMetricSelector(ms)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid match[]: %v", err))
			return
		}
		ss := querier.Select(r.Context(), false, nil, matchers...)
		for ss.Next() {
			lset := ss.At().Labels()
			h := lset.Hash()
			if _, ok := seen[h]; ok {
				continue
			}
			seen[h] = struct{}{}
			result = append(result, labelsToMap(lset))
		}
		if ss.Err() != nil {
			writeError(w, http.StatusInternalServerError, "execution", ss.Err().Error())
			return
		}
	}

	if result == nil {
		result = []map[string]string{}
	}
	writeJSON(w, http.StatusOK, apiResponse{Status: "success", Data: result})
}

func (h *Handler) labelNames(w http.ResponseWriter, r *http.Request, project string) {
	start, end := parseTimeRange(r)
	queryable := &StoreQueryable{Store: h.store, Project: project}
	querier, err := queryable.Querier(start.UnixMilli(), end.UnixMilli())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "execution", err.Error())
		return
	}
	defer querier.Close()

	if err := r.ParseForm(); err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	matcherSets := r.Form["match[]"]

	if len(matcherSets) == 0 {
		names, _, err := querier.LabelNames(r.Context(), nil)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "execution", err.Error())
			return
		}
		if names == nil {
			names = []string{}
		}
		writeJSON(w, http.StatusOK, apiResponse{Status: "success", Data: names})
		return
	}

	// Multiple match[] sets: union results.
	nameSet := map[string]struct{}{}
	for _, ms := range matcherSets {
		matchers, err := parser.ParseMetricSelector(ms)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid match[]: %v", err))
			return
		}
		names, _, err := querier.LabelNames(r.Context(), nil, matchers...)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "execution", err.Error())
			return
		}
		for _, n := range names {
			nameSet[n] = struct{}{}
		}
	}

	names := make([]string, 0, len(nameSet))
	for n := range nameSet {
		names = append(names, n)
	}
	sort.Strings(names)
	writeJSON(w, http.StatusOK, apiResponse{Status: "success", Data: names})
}

func (h *Handler) labelValues(w http.ResponseWriter, r *http.Request, project string, labelName string) {
	start, end := parseTimeRange(r)
	queryable := &StoreQueryable{Store: h.store, Project: project}
	querier, err := queryable.Querier(start.UnixMilli(), end.UnixMilli())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "execution", err.Error())
		return
	}
	defer querier.Close()

	if err := r.ParseForm(); err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	matcherSets := r.Form["match[]"]

	if len(matcherSets) == 0 {
		values, _, err := querier.LabelValues(r.Context(), labelName, nil)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "execution", err.Error())
			return
		}
		if values == nil {
			values = []string{}
		}
		writeJSON(w, http.StatusOK, apiResponse{Status: "success", Data: values})
		return
	}

	valueSet := map[string]struct{}{}
	for _, ms := range matcherSets {
		matchers, err := parser.ParseMetricSelector(ms)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_data", fmt.Sprintf("invalid match[]: %v", err))
			return
		}
		values, _, err := querier.LabelValues(r.Context(), labelName, nil, matchers...)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "execution", err.Error())
			return
		}
		for _, v := range values {
			valueSet[v] = struct{}{}
		}
	}

	values := make([]string, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}
	sort.Strings(values)
	writeJSON(w, http.StatusOK, apiResponse{Status: "success", Data: values})
}

func (h *Handler) queryExemplars(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, apiResponse{Status: "success", Data: []struct{}{}})
}

func (h *Handler) metadata(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, apiResponse{Status: "success", Data: map[string]interface{}{}})
}

// --- Path parsing ---

// parsePromPath extracts project and endpoint from:
// /v1/projects/{project}/location/{location}/prometheus/api/v1/{endpoint...}
func parsePromPath(path string) (project, endpoint string, ok bool) {
	path = strings.TrimPrefix(path, "/")
	parts := strings.SplitN(path, "/", 10)
	// v1/projects/{project}/location/{location}/prometheus/api/v1/{endpoint...}
	//  0    1        2         3         4          5       6   7     8+
	if len(parts) < 9 ||
		parts[0] != "v1" || parts[1] != "projects" ||
		parts[3] != "location" || parts[5] != "prometheus" ||
		parts[6] != "api" || parts[7] != "v1" {
		return "", "", false
	}
	project = parts[2]
	endpoint = strings.Join(parts[8:], "/")
	return project, endpoint, true
}

// parseLabelValuesEndpoint checks if endpoint is "label/{name}/values".
func parseLabelValuesEndpoint(endpoint string) (string, bool) {
	if !strings.HasPrefix(endpoint, "label/") || !strings.HasSuffix(endpoint, "/values") {
		return "", false
	}
	name := endpoint[len("label/") : len(endpoint)-len("/values")]
	if name == "" {
		return "", false
	}
	return name, true
}

// --- Time/duration parsing ---

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Now(), nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		sec := int64(f)
		nsec := int64((f - float64(sec)) * 1e9)
		return time.Unix(sec, nsec), nil
	}
	return time.Parse(time.RFC3339, s)
}

func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	// Bare number → seconds.
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(f * float64(time.Second)), nil
	}
	return time.ParseDuration(s)
}

func parseTimeRange(r *http.Request) (start, end time.Time) {
	start, err := parseTime(r.FormValue("start"))
	if err != nil {
		start = time.Now().Add(-time.Hour)
	}
	end, err = parseTime(r.FormValue("end"))
	if err != nil {
		end = time.Now()
	}
	return start, end
}

// --- Result formatting ---

func writeQueryResult(w http.ResponseWriter, res *promql.Result) {
	var resultType string
	var result interface{}

	switch v := res.Value.(type) {
	case promql.Vector:
		resultType = "vector"
		result = formatVector(v)
	case promql.Matrix:
		resultType = "matrix"
		result = formatMatrix(v)
	case promql.Scalar:
		resultType = "scalar"
		result = formatScalar(v)
	default:
		resultType = string(v.Type())
		result = v.String()
	}

	writeJSON(w, http.StatusOK, apiResponse{
		Status: "success",
		Data: queryData{
			ResultType: resultType,
			Result:     result,
		},
	})
}

func formatVector(v promql.Vector) []vectorItem {
	items := make([]vectorItem, len(v))
	for i, s := range v {
		items[i] = vectorItem{
			Metric: labelsToMap(s.Metric),
			Value:  formatSamplePair(s.T, s.F),
		}
	}
	return items
}

func formatMatrix(m promql.Matrix) []matrixItem {
	items := make([]matrixItem, len(m))
	for i, s := range m {
		values := make([][2]interface{}, len(s.Floats))
		for j, p := range s.Floats {
			values[j] = formatSamplePair(p.T, p.F)
		}
		items[i] = matrixItem{
			Metric: labelsToMap(s.Metric),
			Values: values,
		}
	}
	return items
}

func formatScalar(s promql.Scalar) [2]interface{} {
	return formatSamplePair(s.T, s.V)
}

func formatSamplePair(tMillis int64, v float64) [2]interface{} {
	return [2]interface{}{
		float64(tMillis) / 1000.0,
		formatFloat(v),
	}
}

func formatFloat(v float64) string {
	if math.IsNaN(v) {
		return "NaN"
	}
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	if math.IsInf(v, -1) {
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func labelsToMap(lset labels.Labels) map[string]string {
	m := make(map[string]string, lset.Len())
	lset.Range(func(l labels.Label) {
		m[l.Name] = l.Value
	})
	return m
}

// --- JSON helpers ---

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, errType, msg string) {
	writeJSON(w, status, apiResponse{
		Status:    "error",
		ErrorType: errType,
		Error:     msg,
	})
}
