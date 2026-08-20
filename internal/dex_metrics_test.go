package internal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/internal/common/metrics"
)

type dexMetricsTestInput struct {
	FlowType         string
	ActivityFlowType string
	StepType         string
}

type dexCountingDataConverter struct {
	converter.DataConverter
	fromPayloads atomic.Int32
}

func (d *dexCountingDataConverter) FromPayloads(payloads *commonpb.Payloads, valuePtrs ...interface{}) error {
	d.fromPayloads.Add(1)
	return d.DataConverter.FromPayloads(payloads, valuePtrs...)
}

type dexMetricsActivityStruct struct{}

func (*dexMetricsActivityStruct) Run(context.Context, *dexMetricsTestInput) error { return nil }

func TestDexActivityMetricsProviders(t *testing.T) {
	providers := dexActivityProviders(RegisterActivityOptions{
		FlowTypeProvider: func(input any) string { return input.(*dexMetricsTestInput).FlowType },
		StepTypeProvider: func(input any) string { return input.(*dexMetricsTestInput).StepType },
	})

	values, err := providers.values("activity", &dexMetricsTestInput{FlowType: "flow", StepType: "step"}, "parent")
	require.NoError(t, err)
	require.Equal(t, dexActivityMetricKindStep, values.kind)
	require.Equal(t, "flow", values.flowType)
	require.Equal(t, "step", values.stepType)

	values, err = providers.values("activity", &dexMetricsTestInput{}, "parent")
	require.NoError(t, err)
	require.Equal(t, "parent", values.flowType)
	require.Equal(t, "none", values.stepType)

	providers.flowTypeProvider = func(any) string { return "none" }
	values, err = providers.values("activity", &dexMetricsTestInput{}, "parent")
	require.NoError(t, err)
	require.Equal(t, "none", values.flowType)
}

func TestDexMetricsProviderPanicReturnsError(t *testing.T) {
	provider := func(any) string { panic("wrong input") }
	value, err := invokeDexMetricsProvider("workflow", "Engine", "FlowTypeProvider", provider, struct{}{})
	require.Empty(t, value)
	require.EqualError(t, err, `workflow "Engine" FlowTypeProvider panicked: wrong input`)
}

func TestDexMetricsProviderRegistrationValidation(t *testing.T) {
	registry := newRegistry()
	require.Panics(t, func() {
		registry.RegisterActivityWithOptions(func(context.Context, string) error { return nil }, RegisterActivityOptions{
			StepTypeProvider:    func(any) string { return "step" },
			SubFlowTypeProvider: func(any) string { return "subflow" },
		})
	})
	require.Panics(t, func() {
		registry.RegisterWorkflowWithOptions(func(Context) error { return nil }, RegisterWorkflowOptions{
			FlowTypeProvider: func(any) string { return "flow" },
		})
	})
	require.Panics(t, func() {
		registry.RegisterActivityWithOptions(&dexMetricsActivityStruct{}, RegisterActivityOptions{
			StepTypeProvider: func(any) string { return "step" },
		})
	})
}

func TestDexFlowTypeHeader(t *testing.T) {
	require.Empty(t, popDexFlowTypeHeader(nil))
	require.Empty(t, popDexFlowTypeHeader(&commonpb.Header{}))

	header := &commonpb.Header{Fields: map[string]*commonpb.Payload{}}
	env := &workflowEnvironmentImpl{dexFlowType: "flow", dexFlowTypeConfigured: true}
	addDexFlowTypeHeader(header, env)
	require.Equal(t, "flow", popDexFlowTypeHeader(header))
	require.NotContains(t, header.Fields, dexFlowTypeHeaderName)
}

func TestDexActivityProviderUsesDecodedFirstArgumentOnce(t *testing.T) {
	dataConverter := &dexCountingDataConverter{DataConverter: converter.GetDefaultDataConverter()}
	providerInput := make(chan any, 1)
	var activityCalled atomic.Bool
	activityFn := func(_ context.Context, input *dexMetricsTestInput, suffix string) error {
		activityCalled.Store(true)
		require.Equal(t, "step", input.StepType)
		require.Equal(t, "tail", suffix)
		return nil
	}

	var suite WorkflowTestSuite
	env := suite.NewTestActivityEnvironment().SetDataConverter(dataConverter)
	env.RegisterActivityWithOptions(activityFn, RegisterActivityOptions{
		StepTypeProvider: func(input any) string {
			providerInput <- input
			return input.(*dexMetricsTestInput).StepType
		},
	})

	_, err := env.ExecuteActivity(activityFn, &dexMetricsTestInput{StepType: "step"}, "tail")
	require.NoError(t, err)
	require.True(t, activityCalled.Load())
	decodedInput := (<-providerInput).(*dexMetricsTestInput)
	require.Equal(t, "step", decodedInput.StepType)
	require.EqualValues(t, 1, dataConverter.fromPayloads.Load())
}

func TestDexActivityProviderPanicSkipsActivity(t *testing.T) {
	var activityCalled atomic.Bool
	activityFn := func(context.Context, *dexMetricsTestInput) error {
		activityCalled.Store(true)
		return nil
	}

	var suite WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()
	env.RegisterActivityWithOptions(activityFn, RegisterActivityOptions{
		StepTypeProvider: func(any) string { panic("provider failed") },
	})

	_, err := env.ExecuteActivity(activityFn, &dexMetricsTestInput{})
	require.ErrorContains(t, err, "StepTypeProvider panicked: provider failed")
	require.False(t, activityCalled.Load())
}

func TestDexWorkflowProviderPanicSkipsWorkflow(t *testing.T) {
	var suite WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(dexMetricsPanicWorkflow, RegisterWorkflowOptions{
		Name: "dexMetricsPanicWorkflow",
		FlowTypeProvider: func(any) string {
			panic("provider failed")
		},
	})

	env.ExecuteWorkflow("dexMetricsPanicWorkflow", &dexMetricsTestInput{})
	err := env.GetWorkflowError()
	require.ErrorContains(t, err, `workflow "dexMetricsPanicWorkflow" FlowTypeProvider panicked: provider failed`)
	require.NotContains(t, err.Error(), "business called")
}

func TestDexWorkflowAndActivityMetricsPropagation(t *testing.T) {
	capture := metrics.NewCapturingHandler()
	providerInputs := make(chan any, 3)
	var suite WorkflowTestSuite
	suite.SetMetricsHandler(metrics.NewDexMetricsHandler(capture))
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(dexMetricsWorkflow, RegisterWorkflowOptions{
		FlowTypeProvider: func(input any) string {
			providerInputs <- input
			return input.(*dexMetricsTestInput).FlowType
		},
	})
	env.RegisterActivityWithOptions(dexMetricsStepActivity, RegisterActivityOptions{
		FlowTypeProvider: func(input any) string {
			providerInputs <- input
			return input.(*dexMetricsTestInput).ActivityFlowType
		},
		StepTypeProvider: func(input any) string {
			return input.(*dexMetricsTestInput).StepType
		},
	})

	input := &dexMetricsTestInput{FlowType: "OrderFlow", StepType: "ChargeCard"}
	env.ExecuteWorkflow("dexMetricsWorkflow", input)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.Len(t, providerInputs, 3)
	for range 3 {
		require.IsType(t, &dexMetricsTestInput{}, <-providerInputs)
	}

	requireCapturedCounter(t, capture, "dex_flow_completed", map[string]string{
		metrics.DexFlowTypeTagName: "OrderFlow",
	})
	requireCapturedCounter(t, capture, "dex_sync_step_task_error", map[string]string{
		metrics.DexFlowTypeTagName: "OrderFlow",
		metrics.DexStepTypeTagName: "ChargeCard",
	})
	requireCapturedCounter(t, capture, "dex_async_step_total", map[string]string{
		metrics.DexFlowTypeTagName: "OrderFlow",
		metrics.DexStepTypeTagName: "ChargeCard",
	})
}

func dexMetricsWorkflow(ctx Context, input *dexMetricsTestInput) error {
	GetMetricsHandler(ctx).Counter(metrics.WorkflowCompletedCounter).Inc(1)
	ctx = WithActivityOptions(ctx, ActivityOptions{StartToCloseTimeout: time.Minute})
	if err := ExecuteActivity(ctx, dexMetricsStepActivity, input, "regular").Get(ctx, nil); err != nil {
		return err
	}
	ctx = WithLocalActivityOptions(ctx, LocalActivityOptions{StartToCloseTimeout: time.Minute})
	return ExecuteLocalActivity(ctx, dexMetricsStepActivity, input, "local").Get(ctx, nil)
}

func dexMetricsStepActivity(ctx context.Context, _ *dexMetricsTestInput, execution string) error {
	if execution == "regular" {
		GetActivityMetricsHandler(ctx).Counter(metrics.ActivityTaskErrorCounter).Inc(1)
	}
	return nil
}

func dexMetricsPanicWorkflow(Context, *dexMetricsTestInput) error {
	panic("business called")
}

func requireCapturedCounter(t *testing.T, capture *metrics.CapturingHandler, name string, tags map[string]string) {
	t.Helper()
	for _, counter := range capture.Counters() {
		if counter.Name == name {
			for key, value := range tags {
				require.Equal(t, value, counter.Tags[key])
			}
			return
		}
	}
	require.Failf(t, "counter not found", "metric %q was not captured", name)
}
