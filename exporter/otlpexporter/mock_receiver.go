// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlpexporter // import "go.opentelemetry.io/collector/exporter/otlpexporter"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confignet"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// DataReceiverBase implement basic functions needed by all receivers.
type DataReceiverBase struct {
	// Port on which to listen.
	Port int
}

type BaseOTLPDataReceiver struct {
	DataReceiverBase
	mockConsumer    *exportertest.MockConsumer
	traceReceiver   receiver.Traces
	metricsReceiver receiver.Metrics
	logReceiver     receiver.Logs
}

func NewOTLPDataReceiver(port int, mockConsumer *exportertest.MockConsumer) *BaseOTLPDataReceiver {
	return &BaseOTLPDataReceiver{
		mockConsumer:     mockConsumer,
		DataReceiverBase: DataReceiverBase{Port: port},
	}
}

func (bor *BaseOTLPDataReceiver) Start() error {
	factory := otlpreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig().(*otlpreceiver.Config)
	cfg.GRPC.NetAddr = confignet.NetAddr{Endpoint: fmt.Sprintf("127.0.0.1:%d", bor.Port), Transport: "tcp"}
	cfg.HTTP = nil
	var err error
	set := receivertest.NewNopCreateSettings()
	if bor.traceReceiver, err = factory.CreateTracesReceiver(context.Background(), set, cfg, bor.mockConsumer); err != nil {
		return err
	}
	if bor.metricsReceiver, err = factory.CreateMetricsReceiver(context.Background(), set, cfg, bor.mockConsumer); err != nil {
		return err
	}
	if bor.logReceiver, err = factory.CreateLogsReceiver(context.Background(), set, cfg, bor.mockConsumer); err != nil {
		return err
	}

	if err = bor.traceReceiver.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		return err
	}
	if err = bor.metricsReceiver.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		return err
	}
	return bor.logReceiver.Start(context.Background(), componenttest.NewNopHost())
}

func (bor *BaseOTLPDataReceiver) Stop() error {
	bor.mockConsumer.Clear()
	if err := bor.traceReceiver.Shutdown(context.Background()); err != nil {
		return err
	}
	if err := bor.metricsReceiver.Shutdown(context.Background()); err != nil {
		return err
	}
	return bor.logReceiver.Shutdown(context.Background())
}

func (bor *BaseOTLPDataReceiver) RequestCounter() exportertest.RequestCounter {
	return bor.mockConsumer.RequestCounter()
}
