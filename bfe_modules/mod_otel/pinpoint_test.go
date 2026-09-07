// Copyright (c) 2019 The BFE Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mod_otel

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/bfenetworks/bfe/bfe_http"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestExtractPinpoint(t *testing.T) {
	h := bfe_http.Header{}
	h.Set(headerTraceID, "appA^1690000000000^123")
	h.Set(headerSpanID, "881283423232")
	h.Set(headerPSpanID, "9918273123")
	h.Set(headerSampled, "s1")
	h.Set(headerPAppName, "appA")
	h.Set(headerPAppType, "Java")
	h.Set(headerPRpcName, "/api/order")

	pp := extractPinpoint(h)
	if pp == nil {
		t.Fatal("expected non-nil pinpointContext")
	}
	if pp.traceID != "appA^1690000000000^123" {
		t.Errorf("traceID = %s", pp.traceID)
	}
	if pp.spanID != 881283423232 {
		t.Errorf("spanID = %d", pp.spanID)
	}
	if pp.parentSpanID != 9918273123 {
		t.Errorf("parentSpanID = %d", pp.parentSpanID)
	}
	if sampled, ok := pp.sampledFlag(); !ok || !sampled {
		t.Errorf("sampledFlag = %v, %v", sampled, ok)
	}
	if !pp.hasUpstream {
		t.Error("hasUpstream should be true")
	}

	// 无 pinpoint header
	if extractPinpoint(bfe_http.Header{}) != nil {
		t.Error("expected nil for empty header")
	}
}

func TestNewRootPinpointContext(t *testing.T) {
	pp := newRootPinpointContext("bfe-test")
	if pp.hasUpstream {
		t.Error("hasUpstream should be false")
	}
	parts := strings.Split(pp.traceID, "^")
	if len(parts) != 3 || parts[0] != "bfe-test" {
		t.Errorf("traceID format wrong: %s", pp.traceID)
	}
	if parts[2] != "1" {
		t.Errorf("first transactionSequence should be 1, got %s", parts[2])
	}
	pp2 := newRootPinpointContext("bfe-test")
	if !strings.HasSuffix(pp2.traceID, "^2") {
		t.Errorf("transactionSequence should increase, got %s", pp2.traceID)
	}
	if pp.spanID == -1 || pp.spanID == pp2.spanID {
		t.Error("spanId invalid or duplicated")
	}
}

func newTestTracerProvider(sr *tracetest.SpanRecorder) *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
		sdktrace.WithSpanProcessor(sr),
		sdktrace.WithIDGenerator(ctxIDGenerator{}),
	)
}

func TestSpanIdentityWithUpstream(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := newTestTracerProvider(sr)
	defer tp.Shutdown(context.Background())

	h := bfe_http.Header{}
	h.Set(headerTraceID, "appA^1690000000000^123")
	h.Set(headerSpanID, "881283423232")
	h.Set(headerPSpanID, "9918273123")
	h.Set(headerSampled, "s1")
	pp := extractPinpoint(h)

	ctx := pp.attachContext(context.Background())
	_, span := tp.Tracer("test").Start(ctx, "test-span")
	span.End()

	sc := span.SpanContext()
	if sc.TraceID() != hashTraceId("appA^1690000000000^123") {
		t.Errorf("trace_id = %s, want hash of pinpoint traceID", sc.TraceID())
	}
	if sc.SpanID() != spanIdToOtel(881283423232) {
		t.Errorf("span_id = %s, want upstream assigned spanId", sc.SpanID())
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d", len(spans))
	}
	if spans[0].Parent().SpanID() != spanIdToOtel(9918273123) {
		t.Errorf("parent_span_id = %s, want upstream pSpanID", spans[0].Parent().SpanID())
	}
}

func TestSpanIdentityWithoutUpstream(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := newTestTracerProvider(sr)
	defer tp.Shutdown(context.Background())

	pp := newRootPinpointContext("bfe-test")
	ctx := pp.attachContext(context.Background())
	_, span := tp.Tracer("test").Start(ctx, "test-span")
	span.End()

	sc := span.SpanContext()
	if sc.TraceID() != pp.otelTraceID() {
		t.Errorf("trace_id = %s, want %s", sc.TraceID(), pp.otelTraceID())
	}
	if sc.SpanID() != spanIdToOtel(pp.spanID) {
		t.Errorf("span_id = %s, want %s", sc.SpanID(), spanIdToOtel(pp.spanID))
	}

	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d", len(spans))
	}
	if spans[0].Parent().SpanID().IsValid() {
		t.Errorf("root span should have no parent, got %s", spans[0].Parent().SpanID())
	}
}

func TestUpstreamNotSampled(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := newTestTracerProvider(sr)
	defer tp.Shutdown(context.Background())

	h := bfe_http.Header{}
	h.Set(headerTraceID, "appA^1^1")
	h.Set(headerSpanID, "100")
	h.Set(headerPSpanID, "200")
	h.Set(headerSampled, "s0")
	pp := extractPinpoint(h)

	ctx := pp.attachContext(context.Background())
	_, span := tp.Tracer("test").Start(ctx, "test-span")
	span.End()

	if span.SpanContext().IsSampled() {
		t.Error("span should not be sampled when upstream is s0")
	}
	if len(sr.Ended()) != 0 {
		t.Error("unsampled span should not be exported")
	}
}

func TestInjectPinpoint(t *testing.T) {
	pp := &pinpointContext{
		traceID:      "T1",
		spanID:       100,
		parentSpanID: 200,
	}
	h := bfe_http.Header{}
	injectPinpoint(h, pp, "bfe-svc", "example.com/api", true)

	if h.Get(headerTraceID) != "T1" {
		t.Errorf("Pinpoint-TraceID = %s", h.Get(headerTraceID))
	}
	downstreamSid, _ := strconv.ParseInt(h.Get(headerSpanID), 10, 64)
	if downstreamSid == 100 || downstreamSid == 200 {
		t.Errorf("downstream spanId should be new, got %d", downstreamSid)
	}
	if h.Get(headerPSpanID) != "100" {
		t.Errorf("Pinpoint-pSpanID = %s", h.Get(headerPSpanID))
	}
	if h.Get(headerSampled) != "s1" {
		t.Errorf("Pinpoint-Sampled = %s", h.Get(headerSampled))
	}
	if h.Get(headerPAppName) != "bfe-svc" {
		t.Errorf("Pinpoint-pAppName = %s", h.Get(headerPAppName))
	}
	if h.Get(headerPAppType) != "BFE" {
		t.Errorf("Pinpoint-pAppType = %s", h.Get(headerPAppType))
	}
	if h.Get(headerPRpcName) != "example.com/api" {
		t.Errorf("Pinpoint-pRpcName = %s", h.Get(headerPRpcName))
	}

	injectPinpoint(h, pp, "bfe-svc", "example.com/api", false)
	if h.Get(headerSampled) != "s0" {
		t.Errorf("Pinpoint-Sampled = %s, want s0", h.Get(headerSampled))
	}
}

func TestSpanIdToOtelRoundTrip(t *testing.T) {
	sid := spanIdToOtel(-1)
	if !sid.IsValid() {
		t.Error("spanId -1 should map to valid otel SpanID")
	}
	var buf [8]byte
	copy(buf[:], sid[:])
	if trace.SpanID(buf) != sid {
		t.Error("round trip failed")
	}
}
