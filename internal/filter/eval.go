package filter

import (
	"regexp"
	"strings"

	"google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"

	monitoringpb "github.com/ata-marzban/cloud-monitoring-emulator/gen/google/monitoring/v3"
)

// MatchTimeSeries evaluates the filter expression against a TimeSeries.
// Returns true if the expression is nil (no filter).
func MatchTimeSeries(expr Expr, ts *monitoringpb.TimeSeries) bool {
	if expr == nil {
		return true
	}
	return evalExpr(expr, ts.GetMetric(), ts.GetResource())
}

// MatchMetricDescriptor evaluates the filter expression against a MetricDescriptor.
// Only supports metric.type field matching.
func MatchMetricDescriptor(expr Expr, md *metric.MetricDescriptor) bool {
	if expr == nil {
		return true
	}
	// Create a synthetic metric for field resolution.
	m := &metric.Metric{Type: md.GetType()}
	return evalExpr(expr, m, nil)
}

func evalExpr(expr Expr, m *metric.Metric, r *monitoredres.MonitoredResource) bool {
	switch e := expr.(type) {
	case *AndExpr:
		return evalExpr(e.Left, m, r) && evalExpr(e.Right, m, r)
	case *OrExpr:
		return evalExpr(e.Left, m, r) || evalExpr(e.Right, m, r)
	case *NotExpr:
		return !evalExpr(e.Inner, m, r)
	case *ComparisonExpr:
		return evalComparison(e, m, r)
	default:
		return false
	}
}

func evalComparison(expr *ComparisonExpr, m *metric.Metric, r *monitoredres.MonitoredResource) bool {
	fieldVal := resolveField(expr.Field, m, r)

	switch v := expr.Value.(type) {
	case *StringValue:
		return evalStringOp(expr.Op, fieldVal, v.Val)
	case *FunctionCall:
		return evalFunction(expr.Op, fieldVal, v)
	default:
		return false
	}
}

func evalStringOp(op ComparisonOp, fieldVal, target string) bool {
	switch op {
	case OpEq:
		return fieldVal == target
	case OpNeq:
		return fieldVal != target
	case OpHas:
		return strings.Contains(fieldVal, target)
	default:
		return false
	}
}

func evalFunction(op ComparisonOp, fieldVal string, fn *FunctionCall) bool {
	// For function calls, the op is typically = or :
	// The function determines the matching logic.
	switch fn.Name {
	case "starts_with":
		if len(fn.Args) != 1 {
			return false
		}
		result := strings.HasPrefix(fieldVal, fn.Args[0])
		if op == OpNeq {
			return !result
		}
		return result

	case "ends_with":
		if len(fn.Args) != 1 {
			return false
		}
		result := strings.HasSuffix(fieldVal, fn.Args[0])
		if op == OpNeq {
			return !result
		}
		return result

	case "has_substring":
		if len(fn.Args) != 1 {
			return false
		}
		result := strings.Contains(fieldVal, fn.Args[0])
		if op == OpNeq {
			return !result
		}
		return result

	case "one_of":
		for _, arg := range fn.Args {
			if fieldVal == arg {
				if op == OpNeq {
					return false
				}
				return true
			}
		}
		if op == OpNeq {
			return true
		}
		return false

	case "monitoring.regex.full_match":
		if len(fn.Args) != 1 {
			return false
		}
		matched, err := regexp.MatchString("^(?:"+fn.Args[0]+")$", fieldVal)
		if err != nil {
			return false
		}
		if op == OpNeq {
			return !matched
		}
		return matched

	default:
		return false
	}
}

// resolveField resolves a dot-separated field path against metric and resource.
// Supported paths:
//
//	metric.type
//	metric.labels.<key>
//	resource.type
//	resource.labels.<key>
func resolveField(field string, m *metric.Metric, r *monitoredres.MonitoredResource) string {
	parts := strings.SplitN(field, ".", 3)
	if len(parts) < 2 {
		return ""
	}

	switch parts[0] {
	case "metric":
		if m == nil {
			return ""
		}
		switch parts[1] {
		case "type":
			return m.GetType()
		case "labels", "label":
			if len(parts) < 3 {
				return ""
			}
			return m.GetLabels()[parts[2]]
		}

	case "resource":
		if r == nil {
			return ""
		}
		switch parts[1] {
		case "type":
			return r.GetType()
		case "labels", "label":
			if len(parts) < 3 {
				return ""
			}
			return r.GetLabels()[parts[2]]
		}
	}
	return ""
}
