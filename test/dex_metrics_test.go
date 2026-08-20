package test_test

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	dexMetricsWorkflowName       = "dex-metrics-workflow"
	dexSystemWorkflowName        = "dex-system-workflow"
	dexSyncStepActivityName      = "dex-sync-step-activity"
	dexAsyncStepActivityName     = "dex-async-step-activity"
	dexSubFlowActivityName       = "dex-sub-flow-activity"
	dexRPCActivityName           = "dex-rpc-activity"
	dexSystemActivityName        = "dex-system-activity"
	dexWorkflowPanicName         = "dex-provider-panic-workflow"
	dexActivityPanicWorkflowName = "dex-activity-provider-panic-workflow"
	dexActivityPanicName         = "dex-provider-panic-activity"
)

type dexMetricsIntegrationInput struct {
	FlowType      string
	StepType      string
	SubFlowType   string
	RPCName       string
	ContinueAsNew bool
}

func registerDexMetricsWorkflowsAndActivities(w worker.Worker) {
	w.RegisterWorkflowWithOptions(dexMetricsIntegrationWorkflow, workflow.RegisterOptions{
		Name: dexMetricsWorkflowName,
		FlowTypeProvider: func(input any) string {
			return input.(*dexMetricsIntegrationInput).FlowType
		},
	})
	w.RegisterWorkflowWithOptions(dexSystemIntegrationWorkflow, workflow.RegisterOptions{Name: dexSystemWorkflowName})
	w.RegisterActivityWithOptions(dexSyncStepIntegrationActivity, activity.RegisterOptions{
		Name: dexSyncStepActivityName,
		FlowTypeProvider: func(input any) string {
			return input.(*dexMetricsIntegrationInput).FlowType
		},
		StepTypeProvider: func(input any) string {
			return input.(*dexMetricsIntegrationInput).StepType
		},
	})
	w.RegisterActivityWithOptions(dexAsyncStepIntegrationActivity, activity.RegisterOptions{
		Name: dexAsyncStepActivityName,
		StepTypeProvider: func(input any) string {
			return input.(*dexMetricsIntegrationInput).StepType
		},
	})
	w.RegisterActivityWithOptions(dexSubFlowIntegrationActivity, activity.RegisterOptions{
		Name: dexSubFlowActivityName,
		SubFlowTypeProvider: func(input any) string {
			return input.(*dexMetricsIntegrationInput).SubFlowType
		},
	})
	w.RegisterActivityWithOptions(dexRPCIntegrationActivity, activity.RegisterOptions{
		Name: dexRPCActivityName,
		FlowTypeProvider: func(input any) string {
			return input.(*dexMetricsIntegrationInput).FlowType
		},
		RPCNameProvider: func(input any) string {
			return input.(*dexMetricsIntegrationInput).RPCName
		},
	})
	w.RegisterActivityWithOptions(dexSystemIntegrationActivity, activity.RegisterOptions{Name: dexSystemActivityName})
	w.RegisterWorkflowWithOptions(dexProviderPanicWorkflow, workflow.RegisterOptions{
		Name: dexWorkflowPanicName,
		FlowTypeProvider: func(any) string {
			panic("workflow provider failed")
		},
	})
	w.RegisterWorkflowWithOptions(dexActivityProviderPanicWorkflow, workflow.RegisterOptions{
		Name: dexActivityPanicWorkflowName,
	})
	w.RegisterActivityWithOptions(dexProviderPanicActivity, activity.RegisterOptions{
		Name: dexActivityPanicName,
		StepTypeProvider: func(any) string {
			panic("activity provider failed")
		},
	})
}

func (ts *IntegrationTestSuite) TestDexMetricsProvidersAndNames() {
	input := &dexMetricsIntegrationInput{
		FlowType:      "OrderFlow",
		StepType:      "ChargeCard",
		SubFlowType:   "Fulfillment",
		RPCName:       "ReserveInventory",
		ContinueAsNew: true,
	}
	run, err := ts.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "dex-metrics-" + uuid.NewString(),
		TaskQueue: ts.taskQueueName,
	}, dexMetricsWorkflowName, input)
	ts.NoError(err)
	ts.NoError(run.Get(context.Background(), nil))

	ts.requireDexCounter("dex_flow_continue_as_new", map[string]string{"flow_type": "OrderFlow"})
	ts.requireDexCounter("dex_flow_completed", map[string]string{"flow_type": "OrderFlow"})
	ts.requireDexTimer("dex_flow_task_replay_latency", map[string]string{"flow_type": "OrderFlow"})
	ts.requireDexTimer("dex_sync_step_execution_latency", map[string]string{
		"flow_type": "OrderFlow",
		"step_type": "ChargeCard",
	})
	ts.requireDexCounter("dex_async_step_total", map[string]string{
		"flow_type": "OrderFlow",
		"step_type": "ChargeCard",
	})
	ts.requireDexTimer("dex_sync_start_sub_flow_execution_latency", map[string]string{
		"flow_type":     "OrderFlow",
		"sub_flow_type": "Fulfillment",
	})
	ts.requireDexTimer("dex_sync_invoke_rpc_execution_latency", map[string]string{
		"flow_type": "OrderFlow",
		"rpc_name":  "ReserveInventory",
	})
	ts.requireDexTimer("dex_sync_sys_step_execution_latency", map[string]string{"flow_type": "OrderFlow"})

	systemRun, err := ts.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "dex-system-metrics-" + uuid.NewString(),
		TaskQueue: ts.taskQueueName,
	}, dexSystemWorkflowName)
	ts.NoError(err)
	ts.NoError(systemRun.Get(context.Background(), nil))
	ts.requireDexCounter("dex_sys_flow_completed", nil)
	ts.requireDexTimer("dex_sync_sys_step_execution_latency", map[string]string{"flow_type": "none"})

	for _, counter := range ts.metricsHandler.Counters() {
		ts.False(strings.HasPrefix(counter.Name, "temporal_"), counter.Name)
	}
	for _, gauge := range ts.metricsHandler.Gauges() {
		ts.False(strings.HasPrefix(gauge.Name, "temporal_"), gauge.Name)
	}
	for _, timer := range ts.metricsHandler.Timers() {
		ts.False(strings.HasPrefix(timer.Name, "temporal_"), timer.Name)
	}
}

func (ts *IntegrationTestSuite) TestDexMetricsProviderPanicsSkipBusinessFunctions() {
	run, err := ts.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "dex-workflow-provider-panic-" + uuid.NewString(),
		TaskQueue: ts.taskQueueName,
	}, dexWorkflowPanicName, &dexMetricsIntegrationInput{})
	ts.NoError(err)
	err = run.Get(context.Background(), nil)
	ts.ErrorContains(err, `workflow "dex-provider-panic-workflow" FlowTypeProvider panicked: workflow provider failed`)
	ts.NotContains(err.Error(), "workflow business called")

	dexActivityProviderBusinessCalled.Store(false)
	run, err = ts.client.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID:        "dex-activity-provider-panic-" + uuid.NewString(),
		TaskQueue: ts.taskQueueName,
	}, dexActivityPanicWorkflowName, &dexMetricsIntegrationInput{})
	ts.NoError(err)
	err = run.Get(context.Background(), nil)
	ts.ErrorContains(err, `activity "dex-provider-panic-activity" StepTypeProvider panicked: activity provider failed`)
	ts.False(dexActivityProviderBusinessCalled.Load())
}

func dexMetricsIntegrationWorkflow(ctx workflow.Context, input *dexMetricsIntegrationInput) error {
	if input.ContinueAsNew {
		input.ContinueAsNew = false
		return workflow.NewContinueAsNewError(ctx, dexMetricsWorkflowName, input)
	}
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	if err := workflow.ExecuteActivity(ctx, dexSyncStepActivityName, input).Get(ctx, nil); err != nil {
		return err
	}
	localCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	if err := workflow.ExecuteLocalActivity(localCtx, dexAsyncStepActivityName, input).Get(localCtx, nil); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(ctx, dexSubFlowActivityName, input).Get(ctx, nil); err != nil {
		return err
	}
	if err := workflow.ExecuteActivity(ctx, dexRPCActivityName, input).Get(ctx, nil); err != nil {
		return err
	}
	return workflow.ExecuteActivity(ctx, dexSystemActivityName, input).Get(ctx, nil)
}

func dexSystemIntegrationWorkflow(ctx workflow.Context) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	return workflow.ExecuteActivity(ctx, dexSystemActivityName, &dexMetricsIntegrationInput{}).Get(ctx, nil)
}

func dexSyncStepIntegrationActivity(context.Context, *dexMetricsIntegrationInput) error  { return nil }
func dexAsyncStepIntegrationActivity(context.Context, *dexMetricsIntegrationInput) error { return nil }
func dexSubFlowIntegrationActivity(context.Context, *dexMetricsIntegrationInput) error   { return nil }
func dexRPCIntegrationActivity(context.Context, *dexMetricsIntegrationInput) error       { return nil }
func dexSystemIntegrationActivity(context.Context, *dexMetricsIntegrationInput) error    { return nil }

var dexActivityProviderBusinessCalled atomic.Bool

func dexProviderPanicWorkflow(workflow.Context, *dexMetricsIntegrationInput) error {
	panic("workflow business called")
}

func dexActivityProviderPanicWorkflow(ctx workflow.Context, input *dexMetricsIntegrationInput) error {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	return workflow.ExecuteActivity(ctx, dexActivityPanicName, input).Get(ctx, nil)
}

func dexProviderPanicActivity(context.Context, *dexMetricsIntegrationInput) error {
	dexActivityProviderBusinessCalled.Store(true)
	return nil
}

func (ts *IntegrationTestSuite) requireDexCounter(name string, tags map[string]string) {
	ts.T().Helper()
	for _, counter := range ts.metricsHandler.Counters() {
		if counter.Name == name && capturedTagsContain(counter.Tags, tags) {
			return
		}
	}
	require.Failf(ts.T(), "counter not found", "metric %q with tags %v was not captured", name, tags)
}

func (ts *IntegrationTestSuite) requireDexTimer(name string, tags map[string]string) {
	ts.T().Helper()
	for _, timer := range ts.metricsHandler.Timers() {
		if timer.Name == name && capturedTagsContain(timer.Tags, tags) {
			return
		}
	}
	require.Failf(ts.T(), "timer not found", "metric %q with tags %v was not captured", name, tags)
}

func capturedTagsContain(actual, expected map[string]string) bool {
	for key, value := range expected {
		if actual[key] != value {
			return false
		}
	}
	return true
}
