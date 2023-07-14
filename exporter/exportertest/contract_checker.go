// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportertest // import "go.opentelemetry.io/collector/receiver/receivertest"

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/exporter"
	"math/rand"
	"sync"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
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
	MockReceiver mockLogsReceiver
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
		decisionFunc func(ids idSet) error
	}{
		{
			name: "always_succeed",
			// Always succeed. We expect all data to be delivered as is.
			decisionFunc: func(ids idSet) error { return nil },
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

func checkConsumeContractScenario(params CheckConsumeContractParams, decisionFunc func(ids idSet) error) {

	// Create and start the receiver.
	switch params.DataType {
	case component.DataTypeLogs:
		checkLogs(params)
	//case component.DataTypeTraces:
	//	exp, err = params.Factory.CreateTracesExporter(ctx, NewNopCreateSettings(), params.Config)
	//case component.DataTypeMetrics:
	//	exp, err = params.Factory.CreateMetricsExporter(ctx, NewNopCreateSettings(), params.Config)
	default:
		require.FailNow(params.T, "must specify a valid DataType to test for")
	}

	//require.NoError(params.T, err)
	//err = exp.Start(ctx, componenttest.NewNopHost())
	//require.NoError(params.T, err)
	//defer receiver.srv.GracefulStop()
	//// Begin generating data to the receiver.
	//
	////var generatedIds idSet
	////var generatedIndex int64
	////var mux sync.Mutex
	//var wg sync.WaitGroup
	//
	//const concurrency = 4
	//receiver.setNonPermanentError()

	// Create concurrent goroutines that use the generator.
	// The total number of generator calls will be equal to params.GenerateCount.

	//for j := 0; j < concurrency; j++ {
	//	wg.Add(1)
	//	go func() {
	//		defer wg.Done()
	//		for atomic.AddInt64(&generatedIndex, 1) <= int64(params.GenerateCount) {
	//			ids := params.Generator.Generate()
	//			require.Greater(params.T, len(ids), 0)
	//
	//			mux.Lock()
	//			duplicates := generatedIds.mergeSlice(ids)
	//			mux.Unlock()
	//
	//			// Check that the generator works correctly. There may not be any duplicates in the
	//			// generated data set.
	//			require.Empty(params.T, duplicates)
	//		}
	//	}()
	//}

	//items := consumer.totalItems
	//print(items)
	//// Wait until all generator goroutines are done.
	//wg.Wait()
	//
	//// Wait until all data is seen by the consumer.
	//assert.Eventually(params.T, func() bool {
	//	// Calculate the union of accepted and dropped data.
	//	acceptedAndDropped, duplicates := consumer.acceptedAndDropped()
	//	if len(duplicates) != 0 {
	//		assert.Failf(params.T, "found duplicate elements in received and dropped data", "keys=%v", duplicates)
	//	}
	//	// Compare accepted+dropped with generated. Once they are equal it means all data is seen by the consumer.
	//	missingInOther, onlyInOther := generatedIds.compare(acceptedAndDropped)
	//	return len(missingInOther) == 0 && len(onlyInOther) == 0
	//}, 5*time.Second, 10*time.Millisecond)
	//
	//// Do some final checks. Need the union of accepted and dropped data again.
	//acceptedAndDropped, duplicates := consumer.acceptedAndDropped()
	//if len(duplicates) != 0 {
	//	assert.Failf(params.T, "found duplicate elements in accepted and dropped data", "keys=%v", duplicates)
	//}
	//
	//// Make sure generated and accepted+dropped are exactly the same.
	//
	//missingInOther, onlyInOther := generatedIds.compare(acceptedAndDropped)
	//if len(missingInOther) != 0 {
	//	assert.Failf(params.T, "found elements sent that were not delivered", "keys=%v", missingInOther)
	//}
	//if len(onlyInOther) != 0 {
	//	assert.Failf(params.T, "found elements in accepted and dropped data that was never sent", "keys=%v", onlyInOther)
	//}
	//
	//err = exp.Shutdown(ctx)
	//assert.NoError(params.T, err)
	//
	//// Print some stats to help debug test failures.
	//fmt.Printf(
	//	"Sent %d, accepted=%d, expected dropped=%d, non-permanent errors retried=%d\n",
	//	len(generatedIds),
	//	len(consumer.acceptedIds),
	//	len(consumer.droppedIds),
	//	consumer.nonPermanentFailures,
	//)
}

func checkLogs(params CheckConsumeContractParams) {
	receiver := params.MockReceiver
	ctx := context.Background()

	// Create and start the receiver.
	var exp exporter.Logs
	var err error
	exp, err = params.Factory.CreateLogsExporter(ctx, NewNopCreateSettings(), params.Config)

	require.NoError(params.T, err)
	err = exp.Start(ctx, componenttest.NewNopHost())
	require.NoError(params.T, err)
	defer receiver.srv.GracefulStop()
	defer func(exp exporter.Logs, ctx context.Context) {
		err := exp.Shutdown(ctx)
		if err != nil {

		}
	}(exp, ctx)
	// Begin generating data to the receiver.

	//var generatedIds idSet
	//var generatedIndex int64
	//var mux sync.Mutex
	var wg sync.WaitGroup

	const concurrency = 4
	receiver.setNonPermanentError()

	ld := plog.NewLogs()
	err = exp.ConsumeLogs(ctx, ld)
	fmt.Printf("%d", receiver.totalItems)
	// Wait until all generator goroutines are done.
	wg.Wait()
}

// idSet is a set of unique ids of data elements used in the test (logs, spans or metric data points).
type idSet map[UniqueIDAttrVal]bool

// compare to another set and calculate the differences from this set.
func (ds idSet) compare(other idSet) (missingInOther, onlyInOther []UniqueIDAttrVal) {
	for k := range ds {
		if _, ok := other[k]; !ok {
			missingInOther = append(missingInOther, k)
		}
	}
	for k := range other {
		if _, ok := ds[k]; !ok {
			onlyInOther = append(onlyInOther, k)
		}
	}
	return
}

// merge another set into this one and return a list of duplicate ids.
func (ds *idSet) merge(other idSet) (duplicates []UniqueIDAttrVal) {
	if *ds == nil {
		*ds = map[UniqueIDAttrVal]bool{}
	}
	for k, v := range other {
		if _, ok := (*ds)[k]; ok {
			duplicates = append(duplicates, k)
		} else {
			(*ds)[k] = v
		}
	}
	return
}

// merge another set into this one and return a list of duplicate ids.
func (ds *idSet) mergeSlice(other []UniqueIDAttrVal) (duplicates []UniqueIDAttrVal) {
	if *ds == nil {
		*ds = map[UniqueIDAttrVal]bool{}
	}
	for _, id := range other {
		if _, ok := (*ds)[id]; ok {
			duplicates = append(duplicates, id)
		} else {
			(*ds)[id] = true
		}
	}
	return
}

// union computes the union of this and another sets. A new set if created to return the result.
// Also returns a list of any duplicate ids found.
func (ds *idSet) union(other idSet) (union idSet, duplicates []UniqueIDAttrVal) {
	union = map[UniqueIDAttrVal]bool{}
	for k, v := range *ds {
		union[k] = v
	}
	for k, v := range other {
		if _, ok := union[k]; ok {
			duplicates = append(duplicates, k)
		} else {
			union[k] = v
		}
	}
	return
}

// A function that returns a value indicating what the receiver's next consumer decides
// to do as a result of ConsumeLogs/Trace/Metrics call.
// The result of the decision function becomes the return value of ConsumeLogs/Trace/Metrics.
// Supplying different decision functions allows to test different scenarios of the contract
// between the receiver and it next consumer.
type consumeDecisionFunc func(ids idSet) error

// randomNonPermanentErrorConsumeDecision is a decision function that succeeds approximately
// half of the time and fails with a non-permanent error the rest of the time.
func randomNonPermanentErrorConsumeDecision(_ idSet) error {
	if rand.Float32() < 0.5 {
		return errNonPermanent
	}
	return nil
}

// randomPermanentErrorConsumeDecision is a decision function that succeeds approximately
// half of the time and fails with a permanent error the rest of the time.
func randomPermanentErrorConsumeDecision(_ idSet) error {
	if rand.Float32() < 0.5 {
		return consumererror.NewPermanent(errPermanent)
	}
	return nil
}

// randomErrorsConsumeDecision is a decision function that succeeds approximately
// a third of the time, fails with a permanent error the third of the time and fails with
// a non-permanent error the rest of the time.
func randomErrorsConsumeDecision(_ idSet) error {
	r := rand.Float64()
	third := 1.0 / 3.0
	if r < third {
		return consumererror.NewPermanent(errPermanent)
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
