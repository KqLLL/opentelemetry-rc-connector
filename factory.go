package opentelemetry_rc_connector

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/connector"
	"go.opentelemetry.io/collector/consumer"
)

const (
	typeStr = "rcconnector"
)

// NewFactory creates a factory for example connector.
func NewFactory() connector.Factory {
	// OpenTelemetry connector factory to make a factory for connectors

	return connector.NewFactory(
		typeStr,
		createDefaultConfig,
		connector.WithTracesToMetrics(createTracesToMetricsConnector, component.StabilityLevelAlpha),
		connector.WithTracesToTraces(createTracesToTracesConnector, component.StabilityLevelAlpha),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

// createTracesToMetricsConnector defines the consumer type of the connector
func createTracesToMetricsConnector(_ context.Context, params connector.CreateSettings, cfg component.Config, nextConsumer consumer.Metrics) (connector.Traces, error) {
	c, err := newConnector(params.Logger, cfg, nextConsumer, nil)
	if err != nil {
		return nil, err
	}

	c.metricsConsumer = nextConsumer

	return c, nil
}

func createTracesToTracesConnector(_ context.Context, params connector.CreateSettings, cfg component.Config, nextConsumer consumer.Traces) (connector.Traces, error) {
	c, err := newConnector(params.Logger, cfg, nil, nextConsumer)
	if err != nil {
		return nil, err
	}

	c.tracesConsumer = nextConsumer

	return c, nil
}
