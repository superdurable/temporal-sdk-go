package metrics

import "strings"

const (
	// These reserved tags carry Dex metadata through Handler.WithTags. The private prefix avoids
	// collisions with SDK or user tags, and dexMetricsHandler strips them before emitting metrics.
	dexActivityKindTag = "__dex_activity_kind"
	dexFlowTypeTag     = "__dex_flow_type"
	dexStepTypeTag     = "__dex_step_type"
	dexSubFlowTypeTag  = "__dex_sub_flow_type"
	dexRPCNameTag      = "__dex_rpc_name"

	DexFlowTypeTagName    = "flow_type"
	DexStepTypeTagName    = "step_type"
	DexSubFlowTypeTagName = "sub_flow_type"
	DexRPCNameTagName     = "rpc_name"
)

type dexMetricsHandler struct {
	underlying Handler
	tags       map[string]string
}

// NewDexMetricsHandler rewrites Temporal SDK metric names to the Dex metric
// namespace. Calling it for an already wrapped handler is harmless.
func NewDexMetricsHandler(underlying Handler) Handler {
	if _, ok := underlying.(*dexMetricsHandler); ok {
		return underlying
	}
	return &dexMetricsHandler{underlying: underlying, tags: map[string]string{}}
}

func DexWorkflowTags(flowType string) map[string]string {
	return map[string]string{dexFlowTypeTag: normalizeDexTagValue(flowType)}
}

func DexActivityTags(kind, flowType, stepType, subFlowType, rpcName string) map[string]string {
	return map[string]string{
		dexActivityKindTag: kind,
		dexFlowTypeTag:     normalizeDexTagValue(flowType),
		dexStepTypeTag:     normalizeDexTagValue(stepType),
		dexSubFlowTypeTag:  normalizeDexTagValue(subFlowType),
		dexRPCNameTag:      normalizeDexTagValue(rpcName),
	}
}

func normalizeDexTagValue(value string) string {
	if value == "" {
		return NoneTagValue
	}
	return value
}

func (h *dexMetricsHandler) WithTags(tags map[string]string) Handler {
	cpy := &dexMetricsHandler{underlying: h.underlying, tags: make(map[string]string, len(h.tags)+len(tags))}
	for key, value := range h.tags {
		cpy.tags[key] = value
	}
	publicTags := make(map[string]string, len(tags))
	for key, value := range tags {
		switch key {
		case dexActivityKindTag, dexFlowTypeTag, dexStepTypeTag, dexSubFlowTypeTag, dexRPCNameTag:
			cpy.tags[key] = value
		default:
			publicTags[key] = value
		}
	}
	if len(publicTags) > 0 {
		cpy.underlying = cpy.underlying.WithTags(publicTags)
	}
	return cpy
}

func (h *dexMetricsHandler) Counter(name string) Counter {
	name, tags := h.rewrite(name)
	return h.underlying.WithTags(tags).Counter(name)
}

func (h *dexMetricsHandler) Gauge(name string) Gauge {
	name, tags := h.rewrite(name)
	return h.underlying.WithTags(tags).Gauge(name)
}

func (h *dexMetricsHandler) Timer(name string) Timer {
	name, tags := h.rewrite(name)
	return h.underlying.WithTags(tags).Timer(name)
}

func (h *dexMetricsHandler) Unwrap() Handler { return h.underlying }

func (h *dexMetricsHandler) rewrite(name string) (string, map[string]string) {
	switch {
	case strings.HasPrefix(name, TemporalMetricsPrefix+"local_activity_"):
		return h.rewriteActivity(name, TemporalMetricsPrefix+"local_activity_", "dex_async_")
	case strings.HasPrefix(name, TemporalMetricsPrefix+"activity_"):
		return h.rewriteActivity(name, TemporalMetricsPrefix+"activity_", "dex_sync_")
	case strings.HasPrefix(name, TemporalMetricsPrefix+"workflow_"):
		suffix := strings.TrimPrefix(name, TemporalMetricsPrefix+"workflow_")
		if flowType, ok := h.tags[dexFlowTypeTag]; ok {
			return "dex_flow_" + suffix, map[string]string{DexFlowTypeTagName: normalizeDexTagValue(flowType)}
		}
		return "dex_sys_flow_" + suffix, nil
	case strings.HasPrefix(name, TemporalMetricsPrefix):
		return "dex_" + strings.TrimPrefix(name, TemporalMetricsPrefix), nil
	default:
		return name, nil
	}
}

func (h *dexMetricsHandler) rewriteActivity(name, oldPrefix, newPrefix string) (string, map[string]string) {
	suffix := strings.TrimPrefix(name, oldPrefix)
	flowType := normalizeDexTagValue(h.tags[dexFlowTypeTag])
	switch h.tags[dexActivityKindTag] {
	case "step":
		return newPrefix + "step_" + suffix, map[string]string{
			DexFlowTypeTagName: flowType,
			DexStepTypeTagName: normalizeDexTagValue(h.tags[dexStepTypeTag]),
		}
	case "subflow":
		return newPrefix + "start_sub_flow_" + suffix, map[string]string{
			DexFlowTypeTagName:    flowType,
			DexSubFlowTypeTagName: normalizeDexTagValue(h.tags[dexSubFlowTypeTag]),
		}
	case "rpc":
		return newPrefix + "invoke_rpc_" + suffix, map[string]string{
			DexFlowTypeTagName: flowType,
			DexRPCNameTagName:  normalizeDexTagValue(h.tags[dexRPCNameTag]),
		}
	default:
		return newPrefix + "sys_step_" + suffix, map[string]string{DexFlowTypeTagName: flowType}
	}
}
