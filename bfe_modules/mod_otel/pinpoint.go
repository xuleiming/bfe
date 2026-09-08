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

// Pinpoint 上下文兼容支持：
// 参考《detector 堆栈串联逻辑说明》及《BFE-Pinpoint对接方案》，BFE 作为中间节点完成
// 上游 pinpoint 上下文的解析复用、向 Detector 的 OTel 上报（attributes）、
// 以及向下游的 pinpoint header 透传。
//
// 注意：pinpoint 的 traceId/spanId 与 OTel span 的 trace_id/span_id 相互独立，
// OTel 身份字段完全按 OTel 标准逻辑生成，pinpoint ID 仅用于 header 传递和 attributes 上报。

import (
	"fmt"
	mrand "math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bfenetworks/bfe/bfe_basic"
	"github.com/bfenetworks/bfe/bfe_http"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	headerTraceID  = "Pinpoint-TraceID"
	headerSpanID   = "Pinpoint-SpanID"
	headerPSpanID  = "Pinpoint-pSpanID"
	headerSampled  = "Pinpoint-Sampled"
	headerPAppName = "Pinpoint-pAppName"
	headerPAppType = "Pinpoint-pAppType"
	headerPRpcName = "Pinpoint-pRpcName"

	sampledYes = "s1"
	sampledNo  = "s0"

	attrPinpointTraceID   = "pinpoint.trace_id"
	attrPinpointSpanID    = "pinpoint.span_id"
	attrPinpointNewSpanID = "pinpoint.new_span_id"
	attrPinpointPSpanID   = "pinpoint.p_span_id"
	attrPinpointSampled   = "pinpoint.sampled"
	attrPinpointPAppName  = "pinpoint.p_app_name"
	attrPinpointPAppType  = "pinpoint.p_app_type"
	attrPinpointPRpcName  = "pinpoint.p_rpc_name"

	attrBfeAppType = "bfe.app_type"
	bfeAppType     = "BFE"
)

// pinpointContext 保存一次请求关联的 pinpoint 链路上下文。
// 注意：其中 ID 仅用于 pinpoint header 传递和 attributes 上报，
// 不参与 OTel span 的 trace_id/span_id/parent_span_id 生成
type pinpointContext struct {
	traceID      string // pinpoint 原始 TraceID（agentId^agentStartTime^transactionSequence）
	spanID       int64  // 上游为 BFE 分配的 spanId（S0），无上游时为 0
	parentSpanID int64  // 上游自身的 spanId（S_prev），无上游时为 0
	newSpanID    int64  // BFE 本次为下游生成的 spanId（S_new），即发往下游的 Pinpoint-SpanID
	hasUpstream  bool   // 是否复用了上游传入的上下文
	sampledRaw   string // 上游 Pinpoint-Sampled 原始值（s0/s1），无则为空
}

var (
	agentStartTime = time.Now().Unix() // 模拟 agentStartTime：进程启动时间
	transactionSeq int64               // transactionSequence：进程内原子递增，从 1 开始
	spanIDRand     = mrand.New(mrand.NewSource(time.Now().UnixNano()))
	spanIDRandMu   sync.Mutex
)

// newTransactionId 按 detector 规则生成 TraceID：agentId^agentStartTime^transactionSequence
func newTransactionId(agentId string) string {
	seq := atomic.AddInt64(&transactionSeq, 1)
	return fmt.Sprintf("%s^%d^%d", agentId, agentStartTime, seq)
}

// newSpanId 按 detector 参考实现生成 spanId：随机长整型，不为 -1
func newSpanId() int64 {
	spanIDRandMu.Lock()
	defer spanIDRandMu.Unlock()

	id := int64(spanIDRand.Uint64())
	for id == -1 {
		id = int64(spanIDRand.Uint64())
	}
	return id
}

// nextSpanId 生成下游使用的 spanId，且不为 -1、不与当前 spanId / parentSpanId 重复
func nextSpanId(spanId, parentSpanId int64) int64 {
	spanIDRandMu.Lock()
	defer spanIDRandMu.Unlock()

	id := int64(spanIDRand.Uint64())
	for id == -1 || id == spanId || id == parentSpanId {
		id = int64(spanIDRand.Uint64())
	}
	return id
}

// ensureNewSpanId 惰性生成下游 spanId（重试多个后端时复用同一个，不重新生成）
func (pp *pinpointContext) ensureNewSpanId() {
	if pp.newSpanID == 0 {
		pp.newSpanID = nextSpanId(pp.spanID, pp.parentSpanID)
	}
}

func parseSpanId(s string) (int64, bool) {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// extractPinpoint 从请求头解析上游传入的 pinpoint 上下文；
// 未携带 Pinpoint-TraceID 时返回 nil
func extractPinpoint(h bfe_http.Header) *pinpointContext {
	traceIdStr := h.Get(headerTraceID)
	if traceIdStr == "" {
		return nil
	}

	pp := &pinpointContext{
		traceID:     traceIdStr,
		hasUpstream: true,
		sampledRaw:  h.Get(headerSampled),
	}

	// 复用上游为本端分配的 spanId / parentSpanId，缺失或非法时退化为 0
	pp.spanID, _ = parseSpanId(h.Get(headerSpanID))
	pp.parentSpanID, _ = parseSpanId(h.Get(headerPSpanID))

	return pp
}

// newRootPinpointContext 无上游时 BFE 作为链路起点，自行生成 traceId / spanId。
// agentId 取 BFE 实例名（本端地址 IP:port）
func newRootPinpointContext(agentId string) *pinpointContext {
	pp := &pinpointContext{
		traceID: newTransactionId(agentId),
		spanID:  newSpanId(),
	}
	return pp
}

// sampledFlag 解析采样标记：s1 -> true，s0 -> false，其他 -> 未指定
func (pp *pinpointContext) sampledFlag() (bool, bool) {
	switch pp.sampledRaw {
	case sampledYes:
		return true, true
	case sampledNo:
		return false, true
	}
	return false, false
}

// ctxPinpointSampledKey 用于在 Start 前将上游采样标记放入 context，供采样器读取
type ctxPinpointSampledKey struct{}

// pinpointSampler 采样器包装：上游 Pinpoint-Sampled 为 s0 时丢弃 span（不上报），
// 其余情况委托内层采样器（ParentBased + SampleRate）
type pinpointSampler struct {
	inner sdktrace.Sampler
}

func (s pinpointSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if sampled, ok := p.ParentContext.Value(ctxPinpointSampledKey{}).(bool); ok && !sampled {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop}
	}
	return s.inner.ShouldSample(p)
}

func (s pinpointSampler) Description() string {
	return "PinpointBased{" + s.inner.Description() + "}"
}

// pinpointServerAddr 本次请求的 BFE 本端地址（接收该请求的监听地址）
func pinpointServerAddr(req *bfe_basic.Request) string {
	if req == nil || req.Connection == nil {
		return ""
	}
	return req.Connection.LocalAddr().String()
}

// logPinpoint 上报 pinpoint 相关 attributes（原始值完整携带，供 Detector 关联）。
// appName 为 BFE cluster name（配置项），serverAddr 为本次请求的 BFE 本端地址；
// sampled 为 OTel 采样决策（情况 B 的 pinpoint.sampled 取值依据）；
// p_app_name/p_app_type/p_rpc_name 描述 BFE 自身（作为下游的父节点），两种取值情况一致
func logPinpoint(span trace.Span, pp *pinpointContext, appName string, serverAddr string, sampled bool) {
	if span == nil || pp == nil {
		return
	}

	span.SetAttributes(
		attribute.String(attrPinpointTraceID, pp.traceID),
		attribute.String(attrPinpointNewSpanID, strconv.FormatInt(pp.newSpanID, 10)),
		attribute.String(attrPinpointPAppName, appName),
		attribute.String(attrPinpointPAppType, bfeAppType),
		attribute.String(attrPinpointPRpcName, serverAddr),
		attribute.String(attrBfeAppType, bfeAppType),
	)

	if pp.hasUpstream {
		// 情况 A：span_id / p_span_id 取上游原值；sampled 透传上游值
		span.SetAttributes(
			attribute.String(attrPinpointSpanID, strconv.FormatInt(pp.spanID, 10)),
			attribute.String(attrPinpointPSpanID, strconv.FormatInt(pp.parentSpanID, 10)),
		)
		if pp.sampledRaw != "" {
			span.SetAttributes(attribute.String(attrPinpointSampled, pp.sampledRaw))
		}
	} else {
		// 情况 B：span_id 为空，sampled 按 BFE 采样策略生成
		if sampled {
			span.SetAttributes(attribute.String(attrPinpointSampled, sampledYes))
		} else {
			span.SetAttributes(attribute.String(attrPinpointSampled, sampledNo))
		}
	}
}

// injectPinpoint 向下游请求注入 pinpoint header（复用 traceId，spanId 每请求只生成一次，
// 重试多个后端时复用同一个 newSpanID）。
// appName 为 BFE cluster name（配置项），serverAddr 为本次请求的 BFE 本端地址
func injectPinpoint(h bfe_http.Header, pp *pinpointContext, appName string, serverAddr string, sampled bool) {
	if h == nil || pp == nil {
		return
	}

	pp.ensureNewSpanId()
	h.Set(headerTraceID, pp.traceID)
	h.Set(headerSpanID, strconv.FormatInt(pp.newSpanID, 10))
	h.Set(headerPSpanID, strconv.FormatInt(pp.spanID, 10))
	h.Set(headerPAppName, appName)
	h.Set(headerPAppType, bfeAppType)
	h.Set(headerPRpcName, serverAddr)
	if sampled {
		h.Set(headerSampled, sampledYes)
	} else {
		h.Set(headerSampled, sampledNo)
	}
}
