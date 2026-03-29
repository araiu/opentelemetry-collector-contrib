// slow_receiver.go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// getOrInsertDefault is a helper function to get or insert a default value for a configoptional.Optional type.
func getOrInsertDefault[T any](opt *configoptional.Optional[T]) *T {
	if opt.HasValue() {
		return opt.Get()
	}
	empty := confmap.NewFromStringMap(map[string]any{})
	_ = empty.Unmarshal(opt)
	return opt.Get()
}

// verbose flag
var verbose bool

// otlpConsumer implements consumer for traces, metrics, and logs
type otlpConsumer struct {
	traces  []ptrace.Traces
	metrics []pmetric.Metrics
	logs    []plog.Logs
	delay   time.Duration
}

func (c *otlpConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (c *otlpConsumer) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	// Apply delay before processing
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.traces = append(c.traces, td)
	printTraces("Received Traces:", td)
	return nil
}

func (c *otlpConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	// Apply delay before processing
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.metrics = append(c.metrics, md)
	printMetrics("Received Metrics:", md)
	return nil
}

func (c *otlpConsumer) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	// Apply delay before processing
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	c.logs = append(c.logs, ld)
	printLogs("Received Logs:", ld)
	return nil
}

// printTraces prints the traces in a readable format
func printTraces(prefix string, td ptrace.Traces) {
	if !verbose {
		now := time.Now().Format("2006-01-02 15:04:05")
		fmt.Printf("[%s] Traces: %d, Spans: %d\n", now, td.ResourceSpans().Len(), td.SpanCount())
		return
	}

	fmt.Printf("\n=== %s ===\n", prefix)
	fmt.Printf("Span count: %d\n", td.SpanCount())

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		fmt.Printf("\nResource %d:\n", i)
		printAttributes(rs.Resource().Attributes())

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			fmt.Printf("  Scope %d: %s\n", j, ss.Scope().Name())

			for k := 0; k < ss.Spans().Len(); k++ {
				s := ss.Spans().At(k)
				fmt.Printf("    Span %d: %s\n", k, s.Name())
				fmt.Printf("      TraceID: %s\n", s.TraceID())
				fmt.Printf("      SpanID: %s\n", s.SpanID())
				fmt.Printf("      Kind: %s\n", s.Kind())

				// Status - using the correct API
				status := s.Status()
				fmt.Printf("      Status: %s (%s)\n", status.Code(), status.Message())

				fmt.Printf("      StartTime: %s\n", s.StartTimestamp())
				fmt.Printf("      EndTime: %s\n", s.EndTimestamp())
				fmt.Printf("      Attributes: ")
				printAttributes(s.Attributes())

				// Print events
				if s.Events().Len() > 0 {
					fmt.Printf("      Events (%d):\n", s.Events().Len())
					for e := 0; e < s.Events().Len(); e++ {
						event := s.Events().At(e)
						fmt.Printf("        - %s (time: %s)\n", event.Name(), event.Timestamp())
						printAttributes(event.Attributes())
					}
				}

				// Print links
				if s.Links().Len() > 0 {
					fmt.Printf("      Links (%d):\n", s.Links().Len())
					for l := 0; l < s.Links().Len(); l++ {
						link := s.Links().At(l)
						fmt.Printf("        - trace=%s span=%s\n", link.TraceID(), link.SpanID())
					}
				}
			}
		}
	}

	// Print pdata representation
	fmt.Printf("\n[PDATA Representation]\n")
	fmt.Printf("Type: ptrace.Traces\n")
	fmt.Printf("ResourceSpans length: %d\n", td.ResourceSpans().Len())
}

// printMetrics prints the metrics in a readable format
func printMetrics(prefix string, md pmetric.Metrics) {
	if !verbose {
		now := time.Now().Format("2006-01-02 15:04:05")
		fmt.Printf("[%s] Metrics: %d\n", now, md.MetricCount())
		return
	}

	fmt.Printf("\n=== %s ===\n", prefix)
	fmt.Printf("Metric count: %d\n", md.MetricCount())

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		fmt.Printf("\nResource %d:\n", i)
		printAttributes(rm.Resource().Attributes())

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			fmt.Printf("  Scope %d: %s\n", j, sm.Scope().Name())

			for k := 0; k < sm.Metrics().Len(); k++ {
				m := sm.Metrics().At(k)
				fmt.Printf("    Metric %d: %s\n", k)
				fmt.Printf("      Name: %s\n", m.Name())
				fmt.Printf("      Type: %s\n", m.Type())
				fmt.Printf("      Description: %s\n", m.Description())
				fmt.Printf("      Unit: %s\n", m.Unit())

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for dp := 0; dp < m.Gauge().DataPoints().Len(); dp++ {
						dpVal := m.Gauge().DataPoints().At(dp)
						fmt.Printf("      DataPoint %d:\n", dp)
						printDataPointAttributes(dpVal.Attributes())
						// Use IntValue() or DoubleValue() based on number type
						if dpVal.ValueType() == pmetric.NumberDataPointValueTypeInt {
							fmt.Printf("        Value: %d (int)\n", dpVal.IntValue())
						} else {
							fmt.Printf("        Value: %.2f (double)\n", dpVal.DoubleValue())
						}
					}
				case pmetric.MetricTypeSum:
					for dp := 0; dp < m.Sum().DataPoints().Len(); dp++ {
						dpVal := m.Sum().DataPoints().At(dp)
						fmt.Printf("      DataPoint %d:\n", dp)
						printDataPointAttributes(dpVal.Attributes())
						if dpVal.ValueType() == pmetric.NumberDataPointValueTypeInt {
							fmt.Printf("        Value: %d (int)\n", dpVal.IntValue())
						} else {
							fmt.Printf("        Value: %.2f (double)\n", dpVal.DoubleValue())
						}
					}
				case pmetric.MetricTypeHistogram:
					for dp := 0; dp < m.Histogram().DataPoints().Len(); dp++ {
						dpVal := m.Histogram().DataPoints().At(dp)
						fmt.Printf("      DataPoint %d:\n", dp)
						printDataPointAttributes(dpVal.Attributes())
						fmt.Printf("        Count: %d, Sum: %.2f\n", dpVal.Count(), dpVal.Sum())
					}
				case pmetric.MetricTypeExponentialHistogram:
					for dp := 0; dp < m.ExponentialHistogram().DataPoints().Len(); dp++ {
						dpVal := m.ExponentialHistogram().DataPoints().At(dp)
						fmt.Printf("      DataPoint %d:\n", dp)
						printDataPointAttributes(dpVal.Attributes())
						fmt.Printf("        Count: %d, Sum: %.2f\n", dpVal.Count(), dpVal.Sum())
					}
				case pmetric.MetricTypeSummary:
					for dp := 0; dp < m.Summary().DataPoints().Len(); dp++ {
						dpVal := m.Summary().DataPoints().At(dp)
						fmt.Printf("      DataPoint %d:\n", dp)
						printDataPointAttributes(dpVal.Attributes())
						fmt.Printf("        Count: %d, Sum: %.2f\n", dpVal.Count(), dpVal.Sum())
					}
				}
			}
		}
	}

	// Print pdata representation
	fmt.Printf("\n[PDATA Representation]\n")
	fmt.Printf("Type: pmetric.Metrics\n")
	fmt.Printf("ResourceMetrics length: %d\n", md.ResourceMetrics().Len())
}

// printLogs prints the logs in a readable format
func printLogs(prefix string, ld plog.Logs) {
	if !verbose {
		now := time.Now().Format("2006-01-02 15:04:05")
		fmt.Printf("[%s] Logs: %d\n", now, ld.LogRecordCount())
		return
	}

	fmt.Printf("\n=== %s ===\n", prefix)
	fmt.Printf("Log record count: %d\n", ld.LogRecordCount())

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		fmt.Printf("\nResource %d:\n", i)
		printAttributes(rl.Resource().Attributes())

		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			fmt.Printf("  Scope %d: %s\n", j, sl.Scope().Name())

			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				fmt.Printf("    Log %d:\n", k)
				fmt.Printf("      Body: %s\n", lr.Body().AsRaw())
				// Severity level
				fmt.Printf("      SeverityNumber: %s\n", lr.SeverityNumber())
				fmt.Printf("      Timestamp: %s\n", lr.Timestamp())
				fmt.Printf("      ObservedTimestamp: %s\n", lr.ObservedTimestamp())
				fmt.Printf("      Attributes: ")
				printAttributes(lr.Attributes())

				if lr.TraceID() != [16]byte{} {
					fmt.Printf("      TraceID: %s\n", lr.TraceID())
				}
				if lr.SpanID() != [8]byte{} {
					fmt.Printf("      SpanID: %s\n", lr.SpanID())
				}
			}
		}
	}

	// Print pdata representation
	fmt.Printf("\n[PDATA Representation]\n")
	fmt.Printf("Type: plog.Logs\n")
	fmt.Printf("ResourceLogs length: %d\n", ld.ResourceLogs().Len())
}

// printAttributes prints attributes in a readable format
func printAttributes(attrs pcommon.Map) {
	if attrs.Len() == 0 {
		fmt.Printf("  (none)\n")
		return
	}
	fmt.Printf("  Attributes:\n")
	attrs.Range(func(k string, v pcommon.Value) bool {
		fmt.Printf("    %s: %v\n", k, v.AsRaw())
		return true
	})
}

// printDataPointAttributes prints data point attributes
func printDataPointAttributes(attrs pcommon.Map) {
	attrs.Range(func(k string, v pcommon.Value) bool {
		fmt.Printf("        %s: %v\n", k, v.AsRaw())
		return true
	})
}

func main() {
	// Parse flags
	flag.BoolVar(&verbose, "verbose", false, "Enable verbose output (default: false)")
	flag.Parse()

	delay := 10 * time.Second

	// Create OTLP receiver
	factory := otlpreceiver.NewFactory()
	cfg := factory.CreateDefaultConfig().(*otlpreceiver.Config)

	// Configure gRPC endpoint - the OTLP receiver will listen on this
	grpcConfig := getOrInsertDefault(&cfg.GRPC)
	grpcConfig.NetAddr.Endpoint = "0.0.0.0:4317"

	// Create consumer with delay
	otlpCons := &otlpConsumer{delay: delay}

	// Create receiver
	ctx := context.Background()
	recv, err := factory.CreateTraces(ctx, receivertest.NewNopSettings(factory.Type()), cfg, otlpCons)
	if err != nil {
		log.Fatalf("Failed to create trace receiver: %v", err)
	}

	// Also create metrics and logs receivers
	metricsRecv, err := factory.CreateMetrics(ctx, receivertest.NewNopSettings(factory.Type()), cfg, otlpCons)
	if err != nil {
		log.Fatalf("Failed to create metrics receiver: %v", err)
	}

	logsRecv, err := factory.CreateLogs(ctx, receivertest.NewNopSettings(factory.Type()), cfg, otlpCons)
	if err != nil {
		log.Fatalf("Failed to create logs receiver: %v", err)
	}

	// Start receivers
	host := componenttest.NewNopHost()
	if err := recv.Start(ctx, host); err != nil {
		log.Fatalf("Failed to start trace receiver: %v", err)
	}
	if err := metricsRecv.Start(ctx, host); err != nil {
		log.Fatalf("Failed to start metrics receiver: %v", err)
	}
	if err := logsRecv.Start(ctx, host); err != nil {
		log.Fatalf("Failed to start logs receiver: %v", err)
	}

	fmt.Printf("Starting slow OTLP receiver on 0.0.0.0:4317...\n")
	fmt.Printf("Delay set to: %v\n", delay)
	fmt.Printf("Verbose mode: %v\n", verbose)
	fmt.Printf("Waiting for OTLP requests (traces, metrics, logs)...\n")

	// Keep the receiver running
	select {}
}

// Suppress unused import warnings
var _ = configtls.NewDefaultConfig
