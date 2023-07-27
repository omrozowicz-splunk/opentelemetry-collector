// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportertest // import "go.opentelemetry.io/collector/receiver/receivertest"

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"math/rand"
	"strconv"
	"testing"
)

// UniqueIDAttrName is the attribute name that is used in log records/spans/datapoints as the unique identifier.
const UniqueIDAttrName = "test_id"

// UniqueIDAttrVal is the value type of the UniqueIDAttrName.
type UniqueIDAttrVal string

type CheckConsumeContractParams struct {
	T *testing.T
	// Factory that allows to create a receiver.
	Factory exporter.Factory
	// DataType to test for.
	DataType component.DataType
	// Config of the receiver to use.
	Config       component.Config
	MockReceiver *mockLogsReceiver
}

// CheckConsumeContract checks the contract between the receiver and its next consumer. For the contract
// description see ../doc.go. The checker will detect violations of contract on different scenarios: on success,
// on permanent and non-permanent errors and mix of error types.
func CheckConsumeContract(params CheckConsumeContractParams) {
	// Different scenarios to test for.
	// The decision function defines the testing scenario (i.e. to test for
	// success case or for error case or a mix of both). See for example randomErrorsConsumeDecision.
	scenarios := []struct {
		name         string
		decisionFunc func() error
	}{
		{
			name: "always_succeed",
			// Always succeed. We expect all data to be delivered as is.
			decisionFunc: func() error { return nil },
		},
		{
			name:         "random_non_permanent_error",
			decisionFunc: randomNonPermanentErrorConsumeDecision,
		},
		{
			name:         "random_permanent_error",
			decisionFunc: randomPermanentErrorConsumeDecision,
		},
		{
			name:         "random_error",
			decisionFunc: randomErrorsConsumeDecision,
		},
	}
	for _, scenario := range scenarios {
		params.T.Run(
			scenario.name, func(t *testing.T) {
				checkConsumeContractScenario(params, scenario.decisionFunc)
			},
		)
	}
}

func checkConsumeContractScenario(params CheckConsumeContractParams, decisionFunc func() error) {

	// Create and start the receiver.
	switch params.DataType {
	case component.DataTypeLogs:
		checkLogs(params, decisionFunc)
	case component.DataTypeTraces:
		checkTraces(params, decisionFunc)
	case component.DataTypeMetrics:
		checkMetrics(params, decisionFunc)
	default:
		require.FailNow(params.T, "must specify a valid DataType to test for")
	}

}

func checkMetrics(params CheckConsumeContractParams, decisionFunc func() error) {

}

func checkTraces(params CheckConsumeContractParams, decisionFunc func() error) {

}

func checkLogs(params CheckConsumeContractParams, decisionFunc func() error) {
	receiver := params.MockReceiver
	ctx := context.Background()

	var exp exporter.Logs
	var err error
	exp, err = params.Factory.CreateLogsExporter(ctx, NewNopCreateSettings(), params.Config)
	require.NoError(params.T, err)
	require.NotNil(params.T, exp)

	err = exp.Start(ctx, componenttest.NewNopHost())

	defer func(exp exporter.Logs, ctx context.Context) {
		exp.Shutdown(ctx)
		receiver.clearCounters()
	}(exp, ctx)

	receiver.setExportErrorFunction(decisionFunc)

	for i := 0; i < 10; i++ {
		id := UniqueIDAttrVal(strconv.Itoa(i))
		fmt.Println("Preparing: ", id)
		data := CreateOneLogWithID(id)

		err = exp.ConsumeLogs(ctx, data)
	}

	// The overall number of requests sent by exporter
	fmt.Printf("requested items: %d\n", receiver.requestCount.Load())
	// Successfully delivered items
	fmt.Printf("total items: %d\n", receiver.totalItems.Load())
	// Number of errors that happened
	fmt.Printf("number of errors: %d\n", receiver.totalErrors.Load())
}

// // randomNonPermanentErrorConsumeDecision is a decision function that succeeds approximately
// // half of the time and fails with a non-permanent error the rest of the time.
func randomNonPermanentErrorConsumeDecision() error {
	if rand.Float32() < 0.5 {
		return errNonPermanent
	}
	return nil
}

// randomPermanentErrorConsumeDecision is a decision function that succeeds approximately
// half of the time and fails with a permanent error the rest of the time.
func randomPermanentErrorConsumeDecision() error {
	if rand.Float32() < 0.5 {
		return consumererror.NewPermanent(errPermanent)
	}
	return nil
}

// randomErrorsConsumeDecision is a decision function that succeeds approximately
// a third of the time, fails with a permanent error the third of the time and fails with
// a non-permanent error the rest of the time.
func randomErrorsConsumeDecision() error {
	r := rand.Float64()
	third := 1.0 / 3.0
	if r < third {
		return errPermanent
	}
	if r < 2*third {
		return errNonPermanent
	}
	return nil
}

func CreateOneLogWithID(id UniqueIDAttrVal) plog.Logs {
	data := plog.NewLogs()
	data.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Attributes().PutStr(
		UniqueIDAttrName,
		string(id),
	)
	return data
}
