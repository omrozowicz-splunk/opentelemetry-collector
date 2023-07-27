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
