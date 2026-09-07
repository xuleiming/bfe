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
// 参考《detector 堆栈串联逻辑说明》，BFE 作为中间节点完成
// 上游 pinpoint 上下文的解析复用、向 Detector 的 OTel 上报、
// 以及向下游的 pinpoint header 透传。

import (
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	mrand "math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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

	attrPinpointTraceID  = "pinpoint.trace_id"
	attrPinpointSpanID   = "pinpoint.span_id"
	attrPinpointPSpanID  = "pinpoint.p_span_id"
	attrPinpointSampled  = "pinpoint.sampled"
	attrPinpointPAppName = "pinpoint.p_app_name"
	attrPinpointPAppType = "pinpoint.p_app_type"
	attrPinpointPRpcName = "pinpoint.p_rpc_name"

	attrBfeAppType = "bfe.app_type"
	bfeAppType     = "BFE"
)

// pinpointContext 保存一次请求关联的 pinpoint 链路上下文
type pinpointContext struct {
	traceID      string        // pinpoint 原始 TraceID（agentId^agentStartTime^transactionSequence）
	traceIDHash  trace.TraceID // 映射到 OTel 的 trace_id
	spanID       int64         // 本端本次请求的 spanId（S0）
	parentSpanID int64         // 上游的 spanId（S_prev），无上游时为 0
	hasUpstream  bool          // 是否复用了上游传入的上下文
	sampledRaw   string        // 上游 Pinpoint-Sampled 原始值（s0/s1），无则为空
	pAppName     string        // 上游 pAppName（如有）
	pAppType     string        // 上游 pAppType（如有）
	pRpcName     string        // 上游 pRpcName（如有）
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

// nextSpanId 生成下游使用的 spanId，且不与当前 spanId / parentSpanId 重复
func nextSpanId(spanId, parentSpanId int64) int64 {
	spanIDRandMu.Lock()
	defer spanIDRandMu.Unlock()

	id := int64(spanIDRand.Uint64())
	for id == -1 || id == spanId || id == parentSpanId {
		id = int64(spanIDRand.Uint64())
	}
	return id
}

// hashTraceId pinpoint TraceID 为字符串，无法直接放入 OTel 128 位 trace_id，
// 这里做确定性哈希映射，同一 TraceID 映射结果不变
func hashTraceId(s string) trace.TraceID {
	sum := sha256.Sum256([]byte(s))
	var tid trace.TraceID
	copy(tid[:], sum[:16])
	return tid
}

// spanIdToOtel pinpoint spanId 为 64 位长整型，直接编码为 OTel SpanID
func spanIdToOtel(id int64) trace.SpanID {
	var sid trace.SpanID
	binary.BigEndian.PutUint64(sid[:], uint64(id))
	return sid
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
		pAppName:    h.Get(headerPAppName),
		pAppType:    h.Get(headerPAppType),
		pRpcName:    h.Get(headerPRpcName),
	}

	// 复用上游为本端分配的 spanId / parentSpanId，缺失或非法时退化为 0
	pp.spanID, _ = parseSpanId(h.Get(headerSpanID))
	pp.parentSpanID, _ = parseSpanId(h.Get(headerPSpanID))

	return pp
}

// newRootPinpointContext 无上游时 BFE 作为链路起点，自行生成 traceId / spanId
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

// otelTraceID 返回映射后的 OTel trace_id
func (pp *pinpointContext) otelTraceID() trace.TraceID {
	if !pp.traceIDHash.IsValid() {
		pp.traceIDHash = hashTraceId(pp.traceID)
	}
	return pp.traceIDHash
}

// attachContext 将 pinpoint 上下文转为 OTel 父 context 或预设 span 身份：
// - 有上游：以 pinpoint traceId(映射值)/pSpanID 构造 remote 父节点，并预设本端 spanId
// - 无上游：预设 traceId(映射值)/spanId，本端作为链路起点
func (pp *pinpointContext) attachContext(ctx context.Context) context.Context {
	if pp.hasUpstream && pp.parentSpanID != 0 {
		flags := trace.FlagsSampled
		if sampled, ok := pp.sampledFlag(); ok && !sampled {
			flags = trace.TraceFlags(0)
		}
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    pp.otelTraceID(),
			SpanID:     spanIdToOtel(pp.parentSpanID),
			TraceFlags: flags,
			Remote:     true,
		})
		ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
		return context.WithValue(ctx, presetSpanIdKey{}, spanIdToOtel(pp.spanID))
	}
	// 无有效父节点：整体预设 traceId/spanId
	return context.WithValue(ctx, presetIdsKey{}, presetIds{
		traceID: pp.otelTraceID(),
		spanID:  spanIdToOtel(pp.spanID),
	})
}

// logPinpoint 上报 pinpoint 相关 attributes（原始值完整携带，供 Detector 关联）
func logPinpoint(span trace.Span, pp *pinpointContext, sampled bool) {
	if span == nil || pp == nil {
		return
	}

	span.SetAttributes(
		attribute.String(attrPinpointTraceID, pp.traceID),
		attribute.String(attrPinpointSpanID, strconv.FormatInt(pp.spanID, 10)),
		attribute.String(attrBfeAppType, bfeAppType),
	)

	if pp.hasUpstream {
		span.SetAttributes(
			attribute.String(attrPinpointPSpanID, strconv.FormatInt(pp.parentSpanID, 10)),
			attribute.String(attrPinpointPAppName, pp.pAppName),
			attribute.String(attrPinpointPAppType, pp.pAppType),
			attribute.String(attrPinpointPRpcName, pp.pRpcName),
		)
		if pp.sampledRaw != "" {
			span.SetAttributes(attribute.String(attrPinpointSampled, pp.sampledRaw))
		}
	} else {
		if sampled {
			span.SetAttributes(attribute.String(attrPinpointSampled, sampledYes))
		} else {
			span.SetAttributes(attribute.String(attrPinpointSampled, sampledNo))
		}
	}
}

// pinpointRpcName 本端向下游发起的接口地址（host+path）
func pinpointRpcName(r *bfe_http.Request) string {
	host := r.Host
	if i := strings.Index(host, ":"); i >= 0 {
		host = host[:i]
	}
	return host + r.URL.Path
}

// injectPinpoint 向下游请求注入 pinpoint header（复用 traceId，逐跳生成 spanId）
func injectPinpoint(h bfe_http.Header, pp *pinpointContext, serviceName string, rpcName string, sampled bool) {
	if h == nil || pp == nil {
		return
	}

	downstreamSpanId := nextSpanId(pp.spanID, pp.parentSpanID)
	h.Set(headerTraceID, pp.traceID)
	h.Set(headerSpanID, strconv.FormatInt(downstreamSpanId, 10))
	h.Set(headerPSpanID, strconv.FormatInt(pp.spanID, 10))
	h.Set(headerPAppName, serviceName)
	h.Set(headerPAppType, bfeAppType)
	h.Set(headerPRpcName, rpcName)
	if sampled {
		h.Set(headerSampled, sampledYes)
	} else {
		h.Set(headerSampled, sampledNo)
	}
}

// presetIdsKey / presetSpanIdKey 用于通过 context 向 IDGenerator 传递预设 span 身份
type presetIdsKey struct{}
type presetSpanIdKey struct{}

type presetIds struct {
	traceID trace.TraceID
	spanID  trace.SpanID
}

// ctxIDGenerator 包装 IDGenerator：context 中带预设身份时按预设生成，
// 否则退回随机生成。用于 pinpoint 场景复用上游 traceId/spanId。
type ctxIDGenerator struct{}

func (ctxIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	if p, ok := ctx.Value(presetIdsKey{}).(presetIds); ok {
		return p.traceID, p.spanID
	}
	var tid trace.TraceID
	for {
		randRead(tid[:])
		if tid.IsValid() {
			break
		}
	}
	return tid, ctxIDGenerator{}.NewSpanID(ctx, tid)
}

func (ctxIDGenerator) NewSpanID(ctx context.Context, traceID trace.TraceID) trace.SpanID {
	if sid, ok := ctx.Value(presetSpanIdKey{}).(trace.SpanID); ok && sid.IsValid() {
		return sid
	}
	var sid trace.SpanID
	for {
		randRead(sid[:])
		if sid.IsValid() {
			break
		}
	}
	return sid
}

func randRead(b []byte) {
	if _, err := cryptorand.Read(b); err != nil {
		panic(err)
	}
}

// 确保 ctxIDGenerator 满足 sdktrace.IDGenerator 接口
var _ sdktrace.IDGenerator = ctxIDGenerator{}
