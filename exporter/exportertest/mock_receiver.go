package exportertest

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"net"
	"sync"
	"sync/atomic"
)

var errNonPermanent = errors.New("non permanent error")
var errPermanent = errors.New("permanent error")

type mockReceiver struct {
	srv          *grpc.Server
	requestCount *atomic.Int32
	totalItems   *atomic.Int32
	mux          sync.Mutex
	metadata     metadata.MD
	exportError  error
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

func (r *mockReceiver) getMetadata() metadata.MD {
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.metadata
}

func (r *mockReceiver) setExportError(err error) {
	r.mux.Lock()
	defer r.mux.Unlock()
	r.exportError = err
}
func (r *mockLogsReceiver) Export(ctx context.Context, req plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	fmt.Println("EXPORT")
	r.requestCount.Add(int32(1))
	ld := req.Logs()
	r.totalItems.Add(int32(ld.LogRecordCount()))
	r.mux.Lock()
	defer r.mux.Unlock()
	r.lastRequest = ld
	r.metadata, _ = metadata.FromIncomingContext(ctx)
	return r.exportResponse(), r.exportError
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

func (r *mockLogsReceiver) setError(error error) {
	r.setExportError(error)
}

func (r *mockLogsReceiver) setPermanentError() {
	exportPermanentError := errPermanent
	r.setExportError(exportPermanentError)
}

func (r *mockMetricsReceiver) setPermanentError() {
	exportPermanentError := errPermanent
	r.setExportError(exportPermanentError)
}

func (r *mockTracesReceiver) setPermanentError() {
	exportPermanentError := errPermanent
	r.setExportError(exportPermanentError)
}

func (r *mockLogsReceiver) setNonPermanentError() {
	exportPermanentError := errNonPermanent
	r.setExportError(exportPermanentError)
}

func (r *mockMetricsReceiver) setNonPermanentError() {
	exportPermanentError := errNonPermanent
	r.setExportError(exportPermanentError)
}

func (r *mockTracesReceiver) setNonPermanentError() {
	exportPermanentError := errNonPermanent
	r.setExportError(exportPermanentError)
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
