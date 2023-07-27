// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package exportertest

import (
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/otlpexporter"
	"net"
	"testing"
)

// This file is an example that demonstrates how to use the CheckConsumeContract() function.
// We declare a trivial example receiver, a data generator and then use them in TestConsumeContract().
//
//type exampleExporter struct {
//	Factory exporter.Factory
//}
//
//func (s *exampleExporter) Capabilities() consumer.Capabilities {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (s *exampleExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
//	//TODO implement me
//	return exporter.Logs.ConsumeLogs(_, ctx, ld)
//}
//
//func (s *exampleExporter) Start(_ context.Context, _ component.Host) error {
//	return nil
//}
//
//func (s *exampleExporter) Shutdown(_ context.Context) error {
//	return nil
//}

//func (s *exampleExporter) Receive(data plog.Logs) {
//	// This very simple implementation demonstrates how a single items receiving should happen.
//	for {
//		err := s.nextConsumer.ConsumeLogs(context.Background(), data)
//		if err != nil {
//			// The next consumer returned an error.
//			if !consumererror.IsPermanent(err) {
//				// It is not a permanent error, so we must retry sending it again. In network-based
//				// receivers instead we can ask our sender to re-retry the same data again later.
//				// We may also pause here a bit if we don't want to hammer the next consumer.
//				continue
//			}
//		}
//		// If we are hear either the ConsumeLogs returned success or it returned a permanent error.
//		// In either case we don't need to retry the same data, we are done.
//		return
//	}
//}

// A config for exampleReceiver.
//type exampleExporterConfig struct {
//}
//
//func newExampleFactory() exporter.Factory {
//	return exporter.NewFactory(
//		"example_receiver",
//		func() component.Config {
//			return &exampleExporterConfig{}
//		},
//		exporter.WithLogs(createLog, component.StabilityLevelBeta),
//	)
//}
//
//func createLog(ctx context.Context, settings exporter.CreateSettings, config component.Config) (exporter.Logs, error) {
//	exp := &exampleExporter{nextConsumer: nil}
//	//config.(*exampleExporterConfig).exporter = exp
//	return exp, nil
//}

//func createLog(
//	_ context.Context,
//	_ exporter.CreateSettings,
//	cfg component.Config,
//	consumer consumer.Logs,
//) (exporter.Logs, error) {
//	rcv := &exampleExporter{nextConsumer: consumer}
//	cfg.(*exampleReceiverConfig).generator.exporter = rcv
//	return rcv, nil
//}

// TestConsumeContract is an example of testing of the receiver for the contract between the
// receiver and next consumer.
func TestConsumeContract(t *testing.T) {

	ln, err := net.Listen("tcp", "localhost:1234")
	mockReceiver := otlpLogsReceiverOnGRPCServer(ln)
	require.NoError(t, err, "Failed to find an available address to run the gRPC server: %v", err)

	cfg := &otlpexporter.Config{
		TimeoutSettings: exporterhelper.TimeoutSettings{},
		QueueSettings:   exporterhelper.QueueSettings{Enabled: false},
		RetrySettings:   exporterhelper.NewDefaultRetrySettings(),
		GRPCClientSettings: configgrpc.GRPCClientSettings{
			Endpoint: ln.Addr().String(),
			TLSSetting: configtls.TLSClientSetting{
				Insecure: true,
			}},
	}
	params := CheckConsumeContractParams{
		T:            t,
		Factory:      otlpexporter.NewFactory(),
		DataType:     component.DataTypeLogs,
		Config:       cfg,
		MockReceiver: mockReceiver,
	}
	defer mockReceiver.srv.GracefulStop()

	// Run the contract checker. This will trigger test failures if any problems are found.
	CheckConsumeContract(params)
}
