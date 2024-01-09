package opentelemetry_rc_connector

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"

	"github.com/KqLLL/opentelemetry-rc-connector/lib/sqlparse"
)

// schema for connector
type connectorImp struct {
	config          Config
	metricsConsumer consumer.Metrics
	tracesConsumer  consumer.Traces
	logger          *zap.Logger
	// Include these parameters if a specific implementation for the Start and Shutdown function are not needed
	component.StartFunc
	component.ShutdownFunc
}

// newConnector is a function to create a new connector
func newConnector(logger *zap.Logger, config component.Config, metricsConsumer consumer.Metrics, tracesConsumer consumer.Traces) (*connectorImp, error) {
	logger.Info("Building rc connector")
	cfg := config.(*Config)

	return &connectorImp{
		config:          *cfg,
		logger:          logger,
		metricsConsumer: metricsConsumer,
		tracesConsumer:  tracesConsumer,
	}, nil
}

// Capabilities implements the consumer interface.
func (c *connectorImp) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeTraces method is called for each instance of a trace sent to the connector
func (c *connectorImp) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpan := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpan.ScopeSpans().Len(); j++ {
			scopeSpan := resourceSpan.ScopeSpans().At(j)
			for k := 0; k < scopeSpan.Spans().Len(); k++ {
				attrs := scopeSpan.Spans().At(k).Attributes().AsRaw()
				if values, ok := attrs["type"]; ok && values == "http" {
					scopeSpan.Spans().At(k).SetKind(ptrace.SpanKindServer)
					scopeSpan.Spans().At(k).Attributes().PutStr("type", datadogWebSpanType)
				}

				if values, ok := attrs["db.system"]; ok && values == semconv.DBSystemRedis.Value.AsString() {
					scopeSpan.Spans().At(k).SetKind(ptrace.SpanKindClient)
					scopeSpan.Spans().At(k).Attributes().PutStr("service.name", redisService)
					scopeSpan.Spans().At(k).Attributes().PutStr("type", datadogCacheSpanType)
				}

				if system, ok := attrs["db.system"]; !ok || system != semconv.DBSystemMySQL.Value.AsString() {
					continue
				}
				if sql, ok := attrs["db.statement"]; ok {
					scopeSpan.Spans().At(k).SetKind(ptrace.SpanKindClient)
					scopeSpan.Spans().At(k).Attributes().PutStr("service.name", dbService)
					scopeSpan.Spans().At(k).Attributes().PutStr("type", datadogDbSpanType)
					result, err := sqlparse.ExtractSQL(sql.(string))
					if err != nil {
						return err
					}

					scopeSpan.Spans().At(k).Attributes().PutStr("db.operation", strings.ToUpper(result.Operation))
					scopeSpan.Spans().At(k).Attributes().PutStr("db.sql.table", strings.Join(result.Tables, ","))
					if len(result.Tables) > 1 {
						scopeSpan.Spans().At(k).Attributes().PutStr("db.sql.join", result.JoinType)
					}

					scopeName := fmt.Sprintf("%s %s", strings.ToUpper(result.Operation), result.Tables[0])
					scopeSpan.Spans().At(k).SetName(scopeName)
				}
			}
		}
	}

	return c.tracesConsumer.ConsumeTraces(ctx, td)
}
