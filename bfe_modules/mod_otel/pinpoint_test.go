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
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestExtractPinpoint(t *testing.T) {
	h := bfe_http.Header{}
	h.Set(headerTraceID, "appA^1690000000000^123")
	h.Set(headerSpanID, "881283423232")
	h.Set(headerPSpanID, "9918273123")
	h.Set(headerSampled, "s1")

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
	pp := newRootPinpointContext("10.0.0.1:8080")
	if pp.hasUpstream {
		t.Error("hasUpstream should be false")
	}
	parts := strings.Split(pp.traceID, "^")
	if len(parts) != 3 || parts[0] != "10.0.0.1:8080" {
		t.Errorf("traceID format wrong: %s", pp.traceID)
	}
	if parts[2] != "1" {
		t.Errorf("first transactionSequence should be 1, got %s", parts[2])
	}
	pp2 := newRootPinpointContext("10.0.0.1:8080")
	if !strings.HasSuffix(pp2.traceID, "^2") {
		t.Errorf("transactionSequence should increase, got %s", pp2.traceID)
	}
	if pp.spanID == -1 || pp.spanID == pp2.spanID {
		t.Error("spanId invalid or duplicated")
	}
}

func TestEnsureNewSpanId(t *testing.T) {
	pp := &pinpointContext{spanID: 100, parentSpanID: 200}
	pp.ensureNewSpanId()
	if pp.newSpanID == 0 || pp.newSpanID == 100 || pp.newSpanID == 200 {
		t.Errorf("newSpanID = %d, should be new and not equal to spanId/parentSpanId", pp.newSpanID)
	}
	first := pp.newSpanID
	pp.ensureNewSpanId()
	if pp.newSpanID != first {
		t.Error("ensureNewSpanId should not regenerate (retry keeps same spanId)")
	}
}

func newTestTracerProvider(sr *tracetest.SpanRecorder) *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(
		sdktrace.WithSampler(pinpointSampler{inner: sdktrace.AlwaysSample()}),
		sdktrace.WithSpanProcessor(sr),
	)
}

func TestPinpointSamplerUpstreamNotSampled(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := newTestTracerProvider(sr)
	defer tp.Shutdown(context.Background())

	h := bfe_http.Header{}
	h.Set(headerTraceID, "appA^1^1")
	h.Set(headerSpanID, "100")
	h.Set(headerPSpanID, "200")
	h.Set(headerSampled, "s0")
	pp := extractPinpoint(h)

	ctx := context.Background()
	if sampled, ok := pp.sampledFlag(); ok {
		ctx = context.WithValue(ctx, ctxPinpointSampledKey{}, sampled)
	}
	_, span := tp.Tracer("test").Start(ctx, "test-span")
	span.End()

	if span.SpanContext().IsSampled() {
		t.Error("span should not be sampled when upstream is s0")
	}
	if len(sr.Ended()) != 0 {
		t.Error("unsampled span should not be exported")
	}
}

func TestPinpointSamplerUpstreamSampled(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := newTestTracerProvider(sr)
	defer tp.Shutdown(context.Background())

	h := bfe_http.Header{}
	h.Set(headerTraceID, "appA^1^1")
	h.Set(headerSpanID, "100")
	h.Set(headerSampled, "s1")
	pp := extractPinpoint(h)

	ctx := context.Background()
	if sampled, ok := pp.sampledFlag(); ok {
		ctx = context.WithValue(ctx, ctxPinpointSampledKey{}, sampled)
	}
	_, span := tp.Tracer("test").Start(ctx, "test-span")
	span.End()

	if !span.SpanContext().IsSampled() {
		t.Error("span should be sampled when upstream is s1")
	}
	if len(sr.Ended()) != 1 {
		t.Error("sampled span should be exported")
	}
}

func attrValue(attrs []attribute.KeyValue, key string) (string, bool) {
	for _, a := range attrs {
		if a.Key == attribute.Key(key) {
			return a.Value.AsString(), true
		}
	}
	return "", false
}

func TestLogPinpointWithUpstream(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())

	h := bfe_http.Header{}
	h.Set(headerTraceID, "appA^1^1")
	h.Set(headerSpanID, "100")
	h.Set(headerPSpanID, "200")
	h.Set(headerSampled, "s1")
	pp := extractPinpoint(h)
	pp.ensureNewSpanId()

	_, span := tp.Tracer("test").Start(context.Background(), "test-span")
	logPinpoint(span, pp, "bfe-cluster", "10.0.0.1:8080", true)
	span.End()

	attrs := sr.Ended()[0].Attributes()
	checks := map[string]string{
		attrPinpointTraceID:   "appA^1^1",
		attrPinpointSpanID:    "100",
		attrPinpointPSpanID:   "200",
		attrPinpointNewSpanID: strconv.FormatInt(pp.newSpanID, 10),
		attrPinpointSampled:   "s1",
		attrPinpointPAppName:  "bfe-cluster",
		attrPinpointPAppType:  "BFE",
		attrPinpointPRpcName:  "10.0.0.1:8080",
		attrBfeAppType:        "BFE",
	}
	for k, want := range checks {
		got, ok := attrValue(attrs, k)
		if !ok {
			t.Errorf("attribute %s missing", k)
		} else if got != want {
			t.Errorf("attribute %s = %s, want %s", k, got, want)
		}
	}
}

func TestLogPinpointWithoutUpstream(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	defer tp.Shutdown(context.Background())

	pp := newRootPinpointContext("10.0.0.1:8080")
	pp.ensureNewSpanId()

	_, span := tp.Tracer("test").Start(context.Background(), "test-span")
	logPinpoint(span, pp, "bfe-cluster", "10.0.0.1:8080", true)
	span.End()

	attrs := sr.Ended()[0].Attributes()

	// 情况 B：span_id 不上报
	if _, ok := attrValue(attrs, attrPinpointSpanID); ok {
		t.Error("pinpoint.span_id should not be reported without upstream")
	}
	// p_span_id 不上报
	if _, ok := attrValue(attrs, attrPinpointPSpanID); ok {
		t.Error("pinpoint.p_span_id should not be reported without upstream")
	}
	checks := map[string]string{
		attrPinpointTraceID:   pp.traceID,
		attrPinpointNewSpanID: strconv.FormatInt(pp.newSpanID, 10),
		attrPinpointSampled:   "s1",
		attrPinpointPAppName:  "bfe-cluster",
		attrPinpointPRpcName:  "10.0.0.1:8080",
	}
	for k, want := range checks {
		got, ok := attrValue(attrs, k)
		if !ok {
			t.Errorf("attribute %s missing", k)
		} else if got != want {
			t.Errorf("attribute %s = %s, want %s", k, got, want)
		}
	}
}

func TestInjectPinpoint(t *testing.T) {
	pp := &pinpointContext{
		traceID:      "T1",
		spanID:       100,
		parentSpanID: 200,
		newSpanID:    300,
	}
	h := bfe_http.Header{}
	injectPinpoint(h, pp, "bfe-cluster", "10.0.0.1:8080", true)

	if h.Get(headerTraceID) != "T1" {
		t.Errorf("Pinpoint-TraceID = %s", h.Get(headerTraceID))
	}
	if h.Get(headerSpanID) != "300" {
		t.Errorf("Pinpoint-SpanID = %s, want 300", h.Get(headerSpanID))
	}
	if h.Get(headerPSpanID) != "100" {
		t.Errorf("Pinpoint-pSpanID = %s", h.Get(headerPSpanID))
	}
	if h.Get(headerSampled) != "s1" {
		t.Errorf("Pinpoint-Sampled = %s", h.Get(headerSampled))
	}
	if h.Get(headerPAppName) != "bfe-cluster" {
		t.Errorf("Pinpoint-pAppName = %s", h.Get(headerPAppName))
	}
	if h.Get(headerPAppType) != "BFE" {
		t.Errorf("Pinpoint-pAppType = %s", h.Get(headerPAppType))
	}
	if h.Get(headerPRpcName) != "10.0.0.1:8080" {
		t.Errorf("Pinpoint-pRpcName = %s", h.Get(headerPRpcName))
	}

	// 重试场景：第二次注入不重新生成 spanId
	injectPinpoint(h, pp, "bfe-cluster", "10.0.0.1:8080", false)
	if h.Get(headerSpanID) != "300" {
		t.Errorf("Pinpoint-SpanID = %s after retry, want same 300", h.Get(headerSpanID))
	}
	if h.Get(headerSampled) != "s0" {
		t.Errorf("Pinpoint-Sampled = %s, want s0", h.Get(headerSampled))
	}

	// newSpanID 未生成时自动惰性生成
	pp2 := &pinpointContext{traceID: "T2", spanID: 100}
	injectPinpoint(h, pp2, "bfe-cluster", "10.0.0.1:8080", true)
	if pp2.newSpanID == 0 || pp2.newSpanID == 100 {
		t.Errorf("newSpanID = %d, should be generated", pp2.newSpanID)
	}
}
