package internal

import (
	"fmt"
	"reflect"

	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/sdk/internal/common/metrics"
)

const dexFlowTypeHeaderName = "__temporal_sdk_dex_flow_type"

type dexActivityMetricKind string

const (
	dexActivityMetricKindSystem  dexActivityMetricKind = "system"
	dexActivityMetricKindStep    dexActivityMetricKind = "step"
	dexActivityMetricKindSubFlow dexActivityMetricKind = "subflow"
	dexActivityMetricKindRPC     dexActivityMetricKind = "rpc"
)

type dexActivityMetricProviders struct {
	flowTypeProvider    func(any) string
	stepTypeProvider    func(any) string
	subFlowTypeProvider func(any) string
	rpcNameProvider     func(any) string
	kind                dexActivityMetricKind
}

type dexActivityMetricValues struct {
	flowType    string
	stepType    string
	subFlowType string
	rpcName     string
	kind        dexActivityMetricKind
}

func dexActivityProviders(options RegisterActivityOptions) dexActivityMetricProviders {
	providers := dexActivityMetricProviders{
		flowTypeProvider:    options.FlowTypeProvider,
		stepTypeProvider:    options.StepTypeProvider,
		subFlowTypeProvider: options.SubFlowTypeProvider,
		rpcNameProvider:     options.RPCNameProvider,
		kind:                dexActivityMetricKindSystem,
	}
	configuredKinds := 0
	if providers.stepTypeProvider != nil {
		providers.kind = dexActivityMetricKindStep
		configuredKinds++
	}
	if providers.subFlowTypeProvider != nil {
		providers.kind = dexActivityMetricKindSubFlow
		configuredKinds++
	}
	if providers.rpcNameProvider != nil {
		providers.kind = dexActivityMetricKindRPC
		configuredKinds++
	}
	if configuredKinds > 1 {
		panic("activity registration may configure at most one of StepTypeProvider, SubFlowTypeProvider, and RPCNameProvider")
	}
	return providers
}

func (p dexActivityMetricProviders) configured() bool {
	return p.flowTypeProvider != nil || p.stepTypeProvider != nil || p.subFlowTypeProvider != nil || p.rpcNameProvider != nil
}

func (p dexActivityMetricProviders) metricKind() dexActivityMetricKind {
	if p.kind == "" {
		return dexActivityMetricKindSystem
	}
	return p.kind
}

func validateDexProviderInput(fnType reflect.Type, workflow bool) {
	firstArg := 0
	if fnType.NumIn() > 0 && ((workflow && isWorkflowContext(fnType.In(0))) || (!workflow && isActivityContext(fnType.In(0)))) {
		firstArg = 1
	}
	if fnType.NumIn() <= firstArg {
		kind := "activity"
		if workflow {
			kind = "workflow"
		}
		panic(fmt.Sprintf("%s metrics provider requires at least one non-context input argument", kind))
	}
}

func invokeDexMetricsProvider(kind, typeName, providerName string, provider func(any) string, input any) (value string, err error) {
	value, err = invokeDexMetricsProviderRaw(kind, typeName, providerName, provider, input)
	if err != nil {
		return "", err
	}
	if value == "" {
		value = metrics.NoneTagValue
	}
	return value, err
}

func invokeDexMetricsProviderRaw(kind, typeName, providerName string, provider func(any) string, input any) (value string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%s %q %s panicked: %v", kind, typeName, providerName, recovered)
		}
	}()
	value = provider(input)
	return value, nil
}

func (p dexActivityMetricProviders) values(typeName string, input any, inheritedFlowType string) (dexActivityMetricValues, error) {
	values := dexActivityMetricValues{
		flowType:    metrics.NoneTagValue,
		stepType:    metrics.NoneTagValue,
		subFlowType: metrics.NoneTagValue,
		rpcName:     metrics.NoneTagValue,
		kind:        p.metricKind(),
	}
	if inheritedFlowType != "" {
		values.flowType = inheritedFlowType
	}
	var err error
	if p.flowTypeProvider != nil {
		flowType, providerErr := invokeDexMetricsProviderRaw("activity", typeName, "FlowTypeProvider", p.flowTypeProvider, input)
		if providerErr != nil {
			return values, providerErr
		}
		if flowType != "" {
			values.flowType = flowType
		}
	}
	switch p.kind {
	case dexActivityMetricKindStep:
		values.stepType, err = invokeDexMetricsProvider("activity", typeName, "StepTypeProvider", p.stepTypeProvider, input)
	case dexActivityMetricKindSubFlow:
		values.subFlowType, err = invokeDexMetricsProvider("activity", typeName, "SubFlowTypeProvider", p.subFlowTypeProvider, input)
	case dexActivityMetricKindRPC:
		values.rpcName, err = invokeDexMetricsProvider("activity", typeName, "RPCNameProvider", p.rpcNameProvider, input)
	}
	return values, err
}

func dexActivityMetricsHandler(handler metrics.Handler, values dexActivityMetricValues) metrics.Handler {
	if handler == nil {
		handler = metrics.NopHandler
	}
	return handler.WithTags(metrics.DexActivityTags(
		string(values.kind), values.flowType, values.stepType, values.subFlowType, values.rpcName,
	))
}

type dexWorkflowMetricsEnvironment interface {
	setDexWorkflowMetrics(flowType string, configured bool)
	dexWorkflowFlowType() (string, bool)
}

func addDexFlowTypeHeader(header *commonpb.Header, env WorkflowEnvironment) {
	dexEnv, ok := env.(dexWorkflowMetricsEnvironment)
	if !ok {
		return
	}
	flowType, configured := dexEnv.dexWorkflowFlowType()
	if !configured {
		return
	}
	if flowType == "" {
		flowType = metrics.NoneTagValue
	}
	if header.Fields == nil {
		header.Fields = make(map[string]*commonpb.Payload)
	}
	header.Fields[dexFlowTypeHeaderName] = &commonpb.Payload{Data: []byte(flowType)}
}

func popDexFlowTypeHeader(header *commonpb.Header) string {
	if header == nil || header.Fields == nil {
		return ""
	}
	payload := header.Fields[dexFlowTypeHeaderName]
	delete(header.Fields, dexFlowTypeHeaderName)
	if payload == nil || len(payload.Data) == 0 {
		return ""
	}
	return string(payload.Data)
}
