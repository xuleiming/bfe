# expr 条件表达式引擎设计

日期：2026-10-30
关联：[modifications/2026-10-30-expr-condition-engine](../modifications/2026-10-30-expr-condition-engine/design-changes.md)

## 1. 背景与目标

BFE 既有条件体系（`bfe_basic/condition`：goyacc 解析 + 60+ 固定搭配原语）只有
`&&`/`||`/`!` 布尔组合能力，新增匹配维度必须修改 Go 代码。本设计引入基于
[expr-lang/expr](https://expr-lang.org/) 的通用表达式引擎作为**第二引擎**，与
旧引擎双轨并存：

- 旧原语继续可用，语义不变（默认模式下行为与旧版完全一致）；
- 新风格表达式（取数函数 + expr 运算/比较/集合/正则谓词）可表达旧原语无法表达
  的逻辑（算术、数值区间、多值谓词、跨规则中间结果传递等）；
- 全部条件消费方（路由表 + 各 mod_* 模块规则）经 `condition.Build` 统一分派，
  模块侧零改动。

本期范围：条件引擎 + 旧原语函数化兼容层 + 三模式开关。动作参数表达式与
动态数据源（Redis/Http 等）不在本期。

## 2. 方案选型

### 2.1 为什么选 expr

- 编译期强类型检查（`expr.Env(&Env{})` + `expr.AsBool()`）：类型错误、未知
  函数、结果非 bool 在配置加载期暴露，而非运行期；
- 无副作用、无循环：表达式求值有确定上界，配置错误不会拖死 worker；
- 内置函数丰富（字符串/集合/谓词/类型转换），避免自研求值器的长尾维护成本；
- RE2 正则（`matches`），无回溯爆炸风险。

### 2.2 双轨而非替换

存量配置规模大且旧引擎字符串转义语义三分支不一致（见 §6），一刀切替换风险
不可控。采用进程级三模式开关渐进迁移：

| 模式 | 行为 |
|------|------|
| `LegacyFirst`（默认） | 旧引擎优先；旧引擎解析失败（新风格/混写）才用 expr 引擎；expr 编译失败则配置报错 |
| `ExprCompatible` | expr 引擎 + `legacyPatcher` 重写旧原语名（新旧混写）；编译失败直接报错，不回退 |
| `ExprOnly` | 纯 expr；旧原语名视为未知函数 |

旧原语不做"AST 自动翻译"，而是**函数化兼容**：每个旧原语实现为 `Env` 方法
（LegacyEnv），patcher 在类型检查前把原语名重写为方法名。无代码生成层、无
字符串层改写，规避转义二次处理问题。

## 3. 架构与数据流

### 3.1 组件

```
┌─────────────────────────────────────────────────────────┐
│ 配置加载期（每文件一次）                                   │
│  route_rule.data / mod_*.data ──► condition.Build(condStr)│
│                                   │ 按 EngineMode 分派     │
│     ┌─────────────────────────────┼──────────────────┐    │
│     ▼ LegacyFirst                 ▼ ExprCompatible/  │    │
│  旧 parser (goyacc)            ExprOnly                │    │
│  成功→Legacy Cond              bfe_expr.Compile:       │    │
│  失败▼                         expr.Env(&Env{})+AsBool │
│  bfe_expr.Compile ◄────────  legacy_patcher 重写旧名    │    │
│     │ 成功→*bfe_expr.Condition   ValidateLegacyArgs    │    │
│     │ 失败→配置加载报错          常量参数校验            │    │
└─────┼──────────────────────────────────────────────────┘
      ▼
运行期（每请求）
  NewEnv(req) 一次（缓存于 req.Context）──► cond.Match(req)
                                              │ expr.Run(prog, env)
                                              ▼
                                     出错→按 false + warn(带 logid)
```

### 3.2 Env 设计（struct + 导出方法）

```go
type Env struct {
    BaseEnv   // Req*/Res*/Ses*/Route*/BackendAddr 取数
    TimeEnv   // Now / InTimeRange / InPeriodicRange
    HashEnv   // Hash / IpIn(CIDR) / IpRange / Rand / RandInt / Srand
    VarEnv    // UsrVar*/UsrVarSet*/ReqContext
    InnerEnv  // %var 内置变量函数化（BfeClientIp 等）
    LegacyEnv // 旧原语同名方法（legacyNameMap 全量）
}
```

- 编译期以 `&Env{}` 为类型环境做完整类型检查；运行期 `expr.Run` 传 `*Env`
  （指针接收者方法集）；
- 每请求 `NewEnv(req)` 一次并缓存于 `req.Context`：同一请求内所有表达式
  求值共享同一上下文，用户变量读写与随机源语义由此保证"每请求一致"；
- 随机源随 `NewEnv` 按请求隔离（`math/rand/v2` 风格按时间播种），`Srand(seed)`
  只重播种本请求；
- **命名即配置接口**：方法名面向配置发布，冻结不改。

### 3.3 求值约定

- 结果必须 bool（编译期 `expr.AsBool()` 强制）；
- 取数函数数据缺失返回零值（`""`/`0`/`[]`/`false`），不视为错误；
- 运行期求值错误：该条规则条件按 `false` + warn 日志（logid + 表达式串），
  不影响其他规则与请求；
- 条件表达式允许受控副作用（`UsrVarSetXxx(...); false` 序列表达式）：规则按
  配置顺序求值、每条规则 Cond 一次请求至多求值一次，因此"前排种子规则写、
  后续规则读"是可靠的跨规则传递机制；写入值跨模块可见（存于请求级 Context）。

### 3.4 状态存储

无持久状态。引擎级状态仅：

| 状态 | 位置 | 生命周期 |
|------|------|----------|
| EngineMode | `bfe_basic/condition` 包级变量 | 进程级，启动时由 bfe.conf `ConditionEngineMode` 注入 |
| 编译产物 `*vm.Program` | 各规则结构体内（随配置热更替换） | 配置版本生命周期 |
| 每请求 Env / 用户变量 | `req.Context` | 请求生命周期 |

## 4. 模式注入与配置

`bfe.conf [server]` 段 `ConditionEngineMode`（默认 `LegacyFirst`，兼容
`0`/`1`/`2`）。`bfe_server` 启动时在加载路由/模块配置**之前**调用
`condition.SetEngineMode(...)`，保证所有 `condition.Build` 调用点行为一致。
配置热更不影响模式（模式变更需重启）。

## 5. 边界情况与优化

- **性能**：表达式编译全部发生在加载期，运行期仅 `Run`；`IpIn` 网段列表
  运行期每次调用解析（对齐 go-bfe-pro 现状，热路径优化留待 benchmark）；
  `ReqBody()`/`ReqBodyJson()` 有 IO 成本，文档标注谨慎用于热路径；
  Env 复用经 `req.Context` 缓存，避免每规则重复构造。
- **求值顺序**：expr 不保证子表达式求值顺序，同一表达式内"写后读同一变量"
  须用 `UsrVarSetInt("k", v) > 100` 写判一体形式。
- **expr 检查器盲区**：bool 形参位置的未知标识符（如大写 `FALSE`）编译期
  不拒绝、运行期按 false（旧引擎加载期报错）——配置笔误场景，记录不修复。
- **AI 原语**：`apikey_in`/`model_in`/`req_ai_intent_in` 等经 LegacyEnv 兼容，
  读既有 `req.GetAiBasicInfo()` 等接口；新式 AI 函数本期不注册。

## 6. 新旧引擎已知差异（有意决策，对拍测试固化）

1. **字符串转义**：expr 对双/单引号字面量统一按 Go 规则解码、反引号不解码；
   旧引擎三分支不一致（`*_in` 系 `strconv.Unquote` 解码；regmatch/gjson path
   原样透传；scanner 白名单只校验不解码）。同一条件串在 `ExprCompatible`/
   `ExprOnly` 下语义可能不同（典型：正则 `\.` 新引擎编译失败，须改写为
   `\\.` / 反引号 / `[.]`）。默认 `LegacyFirst` 100% 复现旧行为。
2. **加载期校验**：`ValidateLegacyArgs` 保持旧实现"加载期拒绝非法配置"语义
   （正则/哈希区间/时间格式/IP 区间/变量名等常量参数）。
3. **哈希空输入**：旧原语方法保持"每次调用新随机桶"旧语义；新式 `Hash()`
   为空输入提供"每请求稳定随机桶"，两者并存各有文档。
4. **`res_code_in`**：旧原语方法保持字符串比较；新式 `ResCode()` 为 int 比较。

## 7. 测试策略

- `legacy_compare_test.go`：新旧引擎对拍，旧引擎为 oracle，按 bfe 实际原语
  清单覆盖，重点固化 §6 差异；
- `build_expr_test.go`：三模式下分派与返回类型；
- `bfe_expr/*_test.go`：各子 Env 行为；
- 集成测试（`tests/integration`）：ExprOnly 路由、ExprCompatible 混写、
  LegacyFirst 回退三场景；
- 回归：默认 LegacyFirst 下全部既有单测与集成测试零回退。
