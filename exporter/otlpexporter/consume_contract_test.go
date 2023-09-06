package otlpexporter

import (
	"github.com/cenkalti/backoff/v4"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configgrpc"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"testing"
	"time"
)

// NewTestRetrySettings returns the default settings for otlp exporter test.
func NewTestRetrySettings() exporterhelper.RetrySettings {
	return exporterhelper.RetrySettings{
		Enabled: true,
		// interval is short for the test purposes
		InitialInterval:     10 * time.Millisecond,
		RandomizationFactor: backoff.DefaultRandomizationFactor,
		Multiplier:          1.1,
		MaxInterval:         10 * time.Second,
		MaxElapsedTime:      1 * time.Minute,
	}
}

func ReturnOtlpConfig(endpointAddress string) component.Config {
	return &Config{
		TimeoutSettings: exporterhelper.TimeoutSettings{},
		QueueSettings:   exporterhelper.QueueSettings{Enabled: false},
		RetrySettings:   NewTestRetrySettings(),
		GRPCClientSettings: configgrpc.GRPCClientSettings{
			Endpoint: endpointAddress,
			TLSSetting: configtls.TLSClientSetting{
				Insecure: true,
			}},
	}
}

// TestConsumeContract is an example of testing of the exporter for the contract between the
// exporter and the receiver.
func TestConsumeContractOtlpLogs(t *testing.T) {

	params := exportertest.CheckConsumeContractParams{
		T:                    t,
		Factory:              NewFactory(),
		DataType:             component.DataTypeLogs,
		Config:               ReturnOtlpConfig,
		NumberOfTestElements: 10,
		MockReceiverFactory:  exportertest.CreateMockOtlpReceiver,
	}

	exportertest.CheckConsumeContract(params)
}

func TestConsumeContractOtlpTraces(t *testing.T) {

	params := exportertest.CheckConsumeContractParams{
		T:                    t,
		Factory:              NewFactory(),
		DataType:             component.DataTypeTraces,
		Config:               ReturnOtlpConfig,
		NumberOfTestElements: 10,
		MockReceiverFactory:  exportertest.CreateMockOtlpReceiver,
	}

	exportertest.CheckConsumeContract(params)
}

func TestConsumeContractOtlpMetrics(t *testing.T) {

	params := exportertest.CheckConsumeContractParams{
		T:                    t,
		Factory:              NewFactory(),
		DataType:             component.DataTypeMetrics,
		Config:               ReturnOtlpConfig,
		NumberOfTestElements: 10,
		MockReceiverFactory:  exportertest.CreateMockOtlpReceiver,
	}

	exportertest.CheckConsumeContract(params)
}
