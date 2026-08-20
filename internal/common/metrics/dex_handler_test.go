package metrics_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/internal/common/metrics"
)

func TestDexMetricsHandlerRewritesNamesAndTags(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		tags       map[string]string
		wantName   string
		wantTags   map[string]string
	}{
		{
			name:       "workflow flow",
			metricName: metrics.WorkflowCompletedCounter,
			tags:       metrics.DexWorkflowTags("OrderFlow"),
			wantName:   "dex_flow_completed",
			wantTags:   map[string]string{"flow_type": "OrderFlow"},
		},
		{
			name:       "workflow system",
			metricName: metrics.WorkflowCompletedCounter,
			wantName:   "dex_sys_flow_completed",
			wantTags:   map[string]string{},
		},
		{
			name:       "sync step",
			metricName: metrics.ActivityExecutionLatency,
			tags:       metrics.DexActivityTags("step", "OrderFlow", "ChargeCard", "", ""),
			wantName:   "dex_sync_step_execution_latency",
			wantTags:   map[string]string{"flow_type": "OrderFlow", "step_type": "ChargeCard"},
		},
		{
			name:       "async subflow",
			metricName: metrics.LocalActivityTotalCounter,
			tags:       metrics.DexActivityTags("subflow", "OrderFlow", "", "Fulfillment", ""),
			wantName:   "dex_async_start_sub_flow_total",
			wantTags:   map[string]string{"flow_type": "OrderFlow", "sub_flow_type": "Fulfillment"},
		},
		{
			name:       "sync RPC",
			metricName: metrics.ActivityTaskErrorCounter,
			tags:       metrics.DexActivityTags("rpc", "OrderFlow", "", "", "GetInventory"),
			wantName:   "dex_sync_invoke_rpc_task_error",
			wantTags:   map[string]string{"flow_type": "OrderFlow", "rpc_name": "GetInventory"},
		},
		{
			name:       "system activity uses none",
			metricName: metrics.ActivityScheduleToStartLatency,
			wantName:   "dex_sync_sys_step_schedule_to_start_latency",
			wantTags:   map[string]string{"flow_type": "none"},
		},
		{
			name:       "other Temporal metric",
			metricName: metrics.WorkerStartCounter,
			wantName:   "dex_worker_start",
			wantTags:   map[string]string{},
		},
		{
			name:       "non Temporal metric",
			metricName: "application_metric",
			tags:       metrics.DexActivityTags("step", "OrderFlow", "ChargeCard", "", ""),
			wantName:   "application_metric",
			wantTags:   map[string]string{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			capture := metrics.NewCapturingHandler()
			handler := metrics.NewDexMetricsHandler(capture)
			if test.tags != nil {
				handler = handler.WithTags(test.tags)
			}
			handler.Counter(test.metricName).Inc(1)
			require.Len(t, capture.Counters(), 1)
			require.Equal(t, test.wantName, capture.Counters()[0].Name)
			require.Equal(t, test.wantTags, capture.Counters()[0].Tags)
		})
	}
}

func TestDexMetricsHandlerSupportsAllMetricKindsAndUnwrap(t *testing.T) {
	capture := metrics.NewCapturingHandler()
	handler := metrics.NewDexMetricsHandler(capture).WithTags(
		metrics.DexActivityTags("step", "Flow", "Step", "", ""),
	)

	handler.Gauge(metrics.ActivityExecutionFailedCounter).Update(2)
	handler.Timer(metrics.ActivityExecutionLatency).Record(time.Second)

	require.Equal(t, "dex_sync_step_execution_failed", capture.Gauges()[0].Name)
	require.Equal(t, "dex_sync_step_execution_latency", capture.Timers()[0].Name)
	unwrapped, ok := handler.(interface{ Unwrap() metrics.Handler })
	require.True(t, ok)
	require.NotNil(t, unwrapped.Unwrap())
}
