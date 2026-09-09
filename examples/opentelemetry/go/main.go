package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	endpoint := "http://127.0.0.1:9099/v1/logs"

	traceIDHex := "4bf92f3577b34da6a3ce929d0e0e4736"
	traceIDBytes, _ := hex.DecodeString(traceIDHex)
	spanIDHex := "00f067aa0ba902b7"
	spanIDBytes, _ := hex.DecodeString(spanIDHex)

	now := time.Now().UTC()

	// 1. Build standard OpenTelemetry ExportLogsServiceRequest
	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key:   "service.name",
							Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "payment-gateway"}},
						},
						{
							Key:   "deployment.environment",
							Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "production"}},
						},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{
							Name:    "github.com/myorg/payment",
							Version: "v1.0.0",
						},
						LogRecords: []*logspb.LogRecord{
							{
								TimeUnixNano:   uint64(now.UnixNano()),
								SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
								SeverityText:   "INFO",
								Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "Payment processed successfully"}},
								TraceId:        traceIDBytes,
								SpanId:         spanIDBytes,
								Attributes: []*commonpb.KeyValue{
									{
										Key:   "order_id",
										Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "ord_5521"}},
									},
									{
										Key:   "amount_cents",
										Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 12500}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// 2. Serialize to high-performance binary Protobuf
	protoBytes, err := proto.Marshal(req)
	if err != nil {
		panic(fmt.Sprintf("failed to marshal proto: %v", err))
	}

	// 3. Send to Walspool sidecar
	httpReq, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(protoBytes))
	if err != nil {
		panic(err)
	}
	httpReq.Header.Set("Content-Type", "application/x-protobuf")

	client := &http.Client{Timeout: 5 * time.Second}
	start := time.Now()
	resp, err := client.Do(httpReq)
	elapsed := time.Since(start)

	if err != nil {
		panic(fmt.Sprintf("failed to send OTLP log: %v", err))
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("✔ OTLP Protobuf log ingested in %s (HTTP %d, response bytes: %d)\n", elapsed, resp.StatusCode, len(body))
	fmt.Printf("  Query it with: curl \"http://127.0.0.1:9099/v1/logs?trace_id=%s\"\n", traceIDHex)
}
