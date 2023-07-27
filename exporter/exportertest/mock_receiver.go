// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0
package exportertest

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"sync"
	"sync/atomic"
)

var errNonPermanent = status.Error(codes.DeadlineExceeded, "non Permanent error")
var errPermanent = status.Error(codes.Internal, "Permanent error")

type mockReceiver struct {
	srv                 *grpc.Server
	requestCount        *atomic.Int32
	totalItems          *atomic.Int32
	totalErrors         *atomic.Int32
	mux                 sync.Mutex
	exportErrorFunction func() error
}

type mockMetricsReceiver struct {
	pmetricotlp.UnimplementedGRPCServer
	mockReceiver
	exportResponse func() pmetricotlp.ExportResponse
	lastRequest    pmetric.Metrics
}

type mockLogsReceiver struct {
	plogotlp.UnimplementedGRPCServer
	mockReceiver
	exportResponse func() plogotlp.ExportResponse
	lastRequest    plog.Logs
}

type mockTracesReceiver struct {
	ptraceotlp.UnimplementedGRPCServer
	mockReceiver
	exportResponse func() ptraceotlp.ExportResponse
	lastRequest    ptrace.Traces
}

func (r *mockReceiver) setExportErrorFunction(decisionFunction func() error) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.exportErrorFunction = decisionFunction
}

func (r *mockLogsReceiver) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	r.requestCount.Add(int32(1))
	generatedError := r.exportErrorFunction()
	logs, _ := idSetFromLogs(req.Logs())
	if generatedError != nil {
		if consumererror.IsPermanent(generatedError) {
			fmt.Println("permament error happened")
			fmt.Println("Dropping: ", logs)
		} else {
			fmt.Println("non-permament error happened")
			fmt.Println("Retrying: ", logs)
		}
		r.totalErrors.Add(int32(1))
		return r.exportResponse(), generatedError
	} else {
		fmt.Println("Successfully sent:", logs)
	}
	ld := req.Logs()
	r.mux.Lock()
	defer r.mux.Unlock()
	r.totalItems.Add(int32(ld.LogRecordCount()))
	r.lastRequest = ld
	return r.exportResponse(), nil
}

func (r *mockLogsReceiver) getLastRequest() plog.Logs {
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.lastRequest
}

func (r *mockLogsReceiver) setExportResponse(fn func() plogotlp.ExportResponse) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.exportResponse = fn
}

func (r *mockMetricsReceiver) setExportResponse(fn func() pmetricotlp.ExportResponse) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.exportResponse = fn
}

func (r *mockLogsReceiver) clearCounters() {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.totalItems = &atomic.Int32{}
	r.requestCount = &atomic.Int32{}
	r.totalErrors = &atomic.Int32{}
}

func otlpMetricsReceiverOnGRPCServer(ln net.Listener) *mockMetricsReceiver {
	rcv := &mockMetricsReceiver{
		mockReceiver: mockReceiver{
			srv:          grpc.NewServer(),
			requestCount: &atomic.Int32{},
			totalItems:   &atomic.Int32{},
		},
		exportResponse: pmetricotlp.NewExportResponse,
	}

	// Now run it as a gRPC server
	pmetricotlp.RegisterGRPCServer(rcv.srv, rcv)
	go func() {
		_ = rcv.srv.Serve(ln)
	}()

	return rcv
}

func otlpLogsReceiverOnGRPCServer(ln net.Listener) *mockLogsReceiver {
	rcv := &mockLogsReceiver{
		mockReceiver: mockReceiver{
			srv:          grpc.NewServer(),
			requestCount: &atomic.Int32{},
			totalItems:   &atomic.Int32{},
			totalErrors:  &atomic.Int32{},
		},
		exportResponse: plogotlp.NewExportResponse,
	}

	// Now run it as a gRPC server
	plogotlp.RegisterGRPCServer(rcv.srv, rcv)
	go func() {
		_ = rcv.srv.Serve(ln)
	}()

	return rcv
}

func otlpTracesReceiverOnGRPCServer(ln net.Listener) (*mockTracesReceiver, error) {
	sopts := []grpc.ServerOption{}

	rcv := &mockTracesReceiver{
		mockReceiver: mockReceiver{
			srv:          grpc.NewServer(sopts...),
			requestCount: &atomic.Int32{},
			totalItems:   &atomic.Int32{},
		},
		exportResponse: ptraceotlp.NewExportResponse,
	}

	// Now run it as a gRPC server
	ptraceotlp.RegisterGRPCServer(rcv.srv, rcv)
	go func() {
		_ = rcv.srv.Serve(ln)
	}()

	return rcv, nil
}

// idSet is a set of unique ids of data elements used in the test (logs, spans or metric data points).
type idSet map[UniqueIDAttrVal]bool

func idSetFromLogs(data plog.Logs) (idSet, error) {
	ds := map[UniqueIDAttrVal]bool{}
	rss := data.ResourceLogs()
	for i := 0; i < rss.Len(); i++ {
		ils := rss.At(i).ScopeLogs()
		for j := 0; j < ils.Len(); j++ {
			ss := ils.At(j).LogRecords()
			for k := 0; k < ss.Len(); k++ {
				elem := ss.At(k)
				key, exists := elem.Attributes().Get(UniqueIDAttrName)
				if !exists {
					return ds, fmt.Errorf("invalid data element, attribute %q is missing", UniqueIDAttrName)
				}
				if key.Type() != pcommon.ValueTypeStr {
					return ds, fmt.Errorf("invalid data element, attribute %q is wrong type %v", UniqueIDAttrName, key.Type())
				}
				ds[UniqueIDAttrVal(key.Str())] = true
			}
		}
	}
	return ds, nil
}
