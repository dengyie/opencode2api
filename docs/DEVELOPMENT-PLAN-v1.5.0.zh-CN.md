# opencode2api v1.5.0 开发计划（稳定性与配额治理增强）

> 状态：**authoritative**（本文档是 v1.5.0 开发的唯一蓝图，实现必须逐条对齐本文；实现过程中的偏差必须回写本文）
> 日期：2026-11-04
> 基线：jasonxu114514/opencode2api **v1.4.6**（commit `58f5667`，GPL-3.0）
> 本仓库：`dengyie/opencode2api`（自有 repo，继续以 **GPL-3.0** 发布，保留上游 LICENSE 与出处声明）

---

## 1. 背景与定位

### 1.1 为什么要做这次开发

对生产环境（tebi，`127.0.0.1:8318`，New-API 渠道 #83/#95 上游）做了与另一条技术路线
`oc-free-bridge v3.9`（Node 桥，驱动真实 opencode 客户端，2026-11-03 发行版）的深度对比，
结论：opencode2api 在**失败语义、key 池、模型目录、usage 语义、出口工程**上已经领先，
但在**长静默流保活、流空转治理、冷却恢复主动性、准入控制**上存在 6 个真实缺口。
本版本逐项补齐，全部能力**可配置、默认保守**，保证零配置升级的行为兼容。

### 1.2 六个缺口（来源：oc-free 深度对比，2026-11-04）

| # | 缺口 | oc-free 的成熟做法 | v1.4.6 现状 |
|---|------|--------------------|-------------|
| P1-1 | SSE 心跳 | 45s reasoning 通道保活帧 | 无任何 keepalive（长 reasoning 静默会被中间层/客户端 idle 掐断） |
| P1-2 | 流空转看门狗 | 双计时器：静默 480s abort + 真实错误码 | commit 后无 idle 计时，上游 wedge 只能等 300s 总预算 |
| P1-3 | 冷却 key 额度探活 | 每 10min 一条 4-token 真实请求，恢复即清冷却 | 冷却恢复纯被动；探针刻意不记录 key 状态 |
| P2-4 | 配额预检 | 冷却期内请求直接 429+Retry-After，0 上游请求 | 全 key 冷却时仍硬打 earliest-cooldown key |
| P2-5 | 全局并发闸 | max 2 并发 + 排队 90s | 无准入控制（依赖 New-API 侧 max_inflight） |
| P3-6 | GEO 分类 | 404 `model_not_available_in_region`，不重试 | 地区拒绝混入普通 403 重试逻辑，跨 key 白烧尝试 |

### 1.3 铁律（实现时不可违背）

1. **不改变现有默认行为**：所有新配置项要么默认关闭（`0`），要么默认值即为当前安全行为
   （唯一例外：`heartbeat_seconds` 默认 15 开启——它只新增下游帧，不改变任何既有语义，见 §4.1 论证）。
2. **诚实错误**：新错误路径一律真实状态码 + 结构化 `error.code`，禁止伪装 200。
3. **上游请求零浪费**：预检/探活的每次上游调用都要有明确目的，探活频率有硬下限。
4. **单文件职责**：新逻辑尽量落在新文件（`keepalive.go` / `quotaprobe.go` / `inflight.go`），
   对既有文件的改动收敛到挂点（≤20 行/文件），降低与上游未来的合并成本。

---

## 2. 需求与验收标准总表

| # | 需求 | 验收标准 |
|---|------|----------|
| P1-1 | 流式响应静默时向下游发保活帧 | ① 上游静默 ≥ `heartbeat_seconds` 后下游每 tick 收到 1 帧；② Chat/Responses 用 SSE 注释行 `: keepalive`，Anthropic 用标准 `event: ping`；③ 客户端 SDK 解析不受影响（注释行被忽略）；④ 流结束 ticker 必停；⑤ `heartbeat_seconds: 0` 关闭 |
| P1-2 | commit 后上游静默超限 → 立刻终止 | ① 上游静默 ≥ `idle_timeout_seconds` → 向下游发 in-band 错误（`upstream_idle_timeout`）→ 取消上游请求（停止烧 token）→ 结束；② 非流式路径不受影响；③ 默认 0 关闭 |
| P1-3 | quota 冷却 key 自动探活 | ① key 因 429 quota 进入长冷却后，每 `quota_probe_seconds`（默认 600s）用该 key 发 1 条最小探针；② 2xx → 清冷却 + `quota_probe_recovered` 日志；③ 429 → 顺延下轮；④ 探针用独立会话身份；⑤ 默认 600s，0 关闭 |
| P2-4 | 全 key quota 冷却时预检拒绝 | ① 候选 lane 全部 quota 冷却 → 429 `insufficient_quota`/`free_usage_limit` + `Retry-After`（最早到期），0 上游请求；② 部分 key 可用 → 正常路径；③ probe/Playground 身份旁路；④ 匿名 lane 熔断不受影响 |
| P2-5 | 全局并发闸 | ① `max_inflight` 满 → 排队，超过 `queue_wait_seconds` → 429 `gateway_busy` + `Retry-After: 5`；② 默认 0 = 不限流；③ 指标暴露 inflight/queued |
| P3-6 | GEO 拒绝分类 | ① 上游"not available in your country/region" → 不跨 key/不跨 tier 重试，返回 404 `model_not_available_in_region`；② 不把模型标记为全局不可用（GEO 取决于出口） |

---

## 3. 基线事实（v1.4.6 行为，实现时以此为准）

以下均为已核实的代码事实（文件:行号对应 v1.4.6）：

- **流式写出**：同协议直通 `ForwardStream` = `io.Copy(&sseFlushWriter{...}, TeeReader(reader, observer))`
  （`internal/protocol/stream.go:199`），`sseFlushWriter.Write` 内 flush（:172-184）。
  跨协议转码 `TranscodeStream` → `bridgeStreamEncoder.rawSSE`（`internal/protocol/stream_emitter.go:713-724`），
  **两条路径均无任何 timer/ticker**。
- **超时体系**：请求总预算 `requestCtx = WithTimeout(r.Context(), Retry.TimeoutSeconds)`（`gateway.go:327`）；
  per-attempt deadline 在 commit 时 `Stop()`（`first_event.go:214-259`）；first-event 超时单独可配。
  **commit 后无 idle 计时**。
- **key 池**：`upstreamNode{key, index, preferredProxy, proxyIndex, failures, cooldownUntil}`
  （`internal/gateway/pool.go:598-606`，全 atomic）。`MarkFailure`（pool.go:853-868）对
  401/403/429/5xx 冷却：`cooldown * 2^(failures-1)` 封顶 8x，上游 `Retry-After` 更长则全额采纳。
  **没有 quota 语义字段**；429 与传输错误同等对待。
- **探针**：`probeFreeModel`（`internal/gateway/probe.go:106-215`）走 `probeAttempt`
  （probe.go:217-219）——**刻意绕过 doInferenceAttempt、不记录 key/proxy 状态**；
  由 1 分钟 ticker（`availability.go:68-85`）驱动，成功 1h 后再探、失败 24h。
- **错误分类**：`isNetworkAbuseBlock`（`internal/gateway/abuse.go:66-78`，IP 滥用 403 → 出口冷却
  `free_tier_block_seconds`）；形状门 403 刻意不匹配；`isDeterministicClientResponse`
  （abuse.go:126-137）确定性 4xx 停止重试。**无 GEO 分类**。
- **准入**：`handleInference`（gateway.go:247-…）链路：认证 → 模型目录（404/400，:283-295）→
  `Route` → SystemOne 分流 → `prepareRouteBodies` → `DeriveRequestIDs` → `doUpstream`。
  **无并发闸**。
- **配置**：`Load`（`internal/config/config.go:283-312`）先种默认值再严格解码
  （`DisallowUnknownFields`）；`Normalize`（:317-…）做校验。新字段必须同时改这两处。
- **版本**：`cmd/opencode2api/main.go:22 var version = "dev"`，发布用
  `-ldflags "-X main.version=…"` 注入。

---

## 4. 详细设计

### 4.1 P1-1 SSE 心跳（`heartbeat_seconds`）

**动机**：免费档模型常见数分钟级 reasoning 静默。v1.4.6 在此期间向下游零字节，
New-API/nginx/客户端任一层的 idle 超时都会掐断连接，且网关无法区分「模型在思考」与「链路已死」。

**设计**：新增 `internal/protocol/keepalive.go`——每条已 commit 的流式响应配一个
keepalive 监督 goroutine：

```
type Keepalive struct { … }
func StartKeepalive(ctx context.Context, w io.Writer, flusher http.Flusher, protocol wire.Protocol,
                    opts KeepaliveOptions) *Keepalive
  - opts.SilenceThreshold = cfg.Performance.HeartbeatSeconds（秒；0 = 不启动）
  - 内部：lastWrite atomic.Int64（UnixNano）；ticker 周期 = min(5s, threshold/3)
  - 每次 tick：now-lastWrite >= threshold → 按 protocol 写一帧 → Flush → 更新 lastWrite
  - ctx.Done / Stop() → ticker 停止，goroutine 退出
func (k *Keepalive) MarkWrite()          // 数据泵每次真实写下游后调用
func (k *Keepalive) SilenceNanos() int64 // 供 P1-2 idle 看门狗复用
```

帧格式（按协议）：
| 协议 | 保活帧 | 依据 |
|---|---|---|
| Chat / Responses | `: keepalive\n\n`（SSE 注释行） | RFC 8895/SSE 规范：解析器必须忽略 `:` 开头行；OpenAI SDK 实测忽略 |
| Anthropic | `event: ping\ndata: {"type":"ping"}\n\n` | Anthropic 官方流本来就周期性发 ping |

**挂点**（两个，均为 ≤10 行）：
1. `stream.go ForwardStream`：构造 `sseFlushWriter` 处包一层，`io.Copy` 循环内每次 Write 后
   `k.MarkWrite()`；函数退出 `defer k.Stop()`。
2. `stream_emitter.go bridgeStreamEncoder.rawSSE`：写入成功后 `k.MarkWrite()`（encoder 持有 k）；
   encoder 的创建处启动，`bridgeStreamEncoder.Close`（或流结束路径）`Stop()`。

**互斥**：数据泵 goroutine 与 ticker goroutine 并发写 `http.ResponseWriter`。
`sseFlushWriter` 与 `bridgeStreamEncoder` 的写路径加 `sync.Mutex`（保活帧与数据帧互斥）。
锁粒度 = 单帧，无死锁风险（锁内不做网络读）。

**默认值论证**：15s 注释行对任何合规 SSE 客户端不可见，不改变语义；15s 低于常见中间层
idle 超时（nginx 默认 60s）。因此默认开启是安全的；`0` 可关。

**配置**：`performance.heartbeat_seconds`（int，默认 15，0=off）。

### 4.2 P1-2 commit 后 idle 看门狗（`idle_timeout_seconds`）

**动机**：上游 wedge（连接挂着但永不产出）目前要吃满 300s 总预算才结束，
客户端只能收到笼统的 504；且 wedge 期间上游连接与 token 都挂着。

**设计**：与 keepalive 同源——监督 goroutine 扩展为「心跳 + 空转」双职责
（同一 `lastWrite` 时钟，两个阈值）：

```
tick:
  silence = now - lastUpstreamData        // 注意：上游数据时钟，不是下游写时钟
  if silence >= IdleTimeoutSeconds:       // IdleTimeoutSeconds > 0 时启用
      if dead.CompareAndSwap(false, true):
          1) 按 protocol 向下游发 in-band 错误帧（复用既有 emitError 语义）
             Chat:      data: {"error":{"message":"…idle Ns…","type":"server_error","code":"upstream_idle_timeout"}} + [DONE]
             Anthropic: event: error
             Responses: response.failed
          2) cancel(requestCtx)      // 传播到上游 attempt → 传输层断开，停止烧上游
          3) log: event=upstream_idle_timeout, session, silence
```

- `lastUpstreamData` 由数据泵在**每次从上游读到数据**时更新（与下游写时钟分开：
  心跳写下游不复活 idle 时钟，反之亦然）。
- `dead` 标志：数据泵转发前检查，置位后停止转发（避免错误帧后仍透传上游残量）。
- 取消方式：watchdog 持有 `cancel`（即 `gateway.go:327` 的 `requestCtx` cancel）——
  通过 `KeepaliveOptions.OnIdle func(silence time.Duration)` 回调注入，keepalive.go
  不 import gateway，保持分层。onIdle 内部顺序：发帧 → 置 dead → cancel → 日志。
- 客户端取消竞态：`ClientCanceled` 路径已存在（stream.go:161-166），watchdog 触发前
  若客户端先走，ctx.Done 会让 goroutine 静默退出。
- 非流式路径：不做（总预算已覆盖）。

**默认值**：0 = off（保持现行为）。tebi 部署配置设 240s（低于 300s 总预算，先于总预算触发）。

**配置**：`performance.idle_timeout_seconds`（int，默认 0）。

### 4.3 P1-3 冷却 key 额度探活（`quota_probe_seconds`）

**动机**：免费层撞墙的 `Retry-After` 常达小时级（tebi 实测 12564s）。冷却恢复纯被动，
全 key 冷却时网关形同瘫�痪，只能等下一条真实请求撞墙才知道好没好。

**设计**：pool 层加 quota 语义 + gateway 层加探活循环。

**pool.go 改动**（`upstreamNode` 追加 2 个 atomic 字段）：
```
quotaUntil  atomic.Int64   // quota 型冷却到期时刻（UnixNano；0=无）
probeNextAt atomic.Int64   // 下次允许探活时刻
```
- `MarkFailure` 内：resp 为 429 且 body 命中 quota 措辞
  （`free usage limit` / `usage_limit` / `quota_exceeded` / `FreeUsageLimitError`，
  与 `rotation.go:59-70 quotaError` 的判定字集一致）→ 同步写 `quotaUntil = cooldownUntil`、
  `probeNextAt = now + quotaProbeInterval`。
- `MarkSuccess` / `RestoreProxy`：清零 quotaUntil（语义：额度恢复/代理恢复都视为可用）。
- 新查询 API：`QuotaCooledUntil() time.Time`（该池所有节点 quotaUntil 的**最大值**）、
  `AllQuotaCooled() (earliestRecovery time.Time, ok bool)`——全部节点都在 quota 冷却时返回
  最早到期时间。§4.4 预检用。

**新文件 `internal/gateway/quotaprobe.go`**：
```
func (g *Gateway) runQuotaProbeLoop(ctx)      // 60s ticker
  for each pool(zen/go):
    for node in pool.quotaCooledNodes():      // quotaUntil > now 且 probeNextAt <= now
      if !g.quotaProbeSem.TryAcquire(): break // 全局并发 1，温和探活
      go probeQuotaKey(node)
func probeQuotaKey(node)
  body  = 流式 "Reply with the single word OK.", max_tokens 8   // 对齐 probe.go 尺寸
  身份  = 全新 session/project id（复用 probe.go 的独立身份派生，绝不复用业务会话桶）
  走该 node 的 key + 当前绑定 proxy 发起（专用 attempt，不经过 doInferenceAttempt 计数）
  2xx:  node.MarkSuccess 等价清零（failures/quotaUntil/cooldownUntil）
        log event=quota_probe_recovered key_id=…
        telemetry: quota_probe_total{result=recovered}
  429:  probeNextAt = now + quotaProbeInterval（不打扰，等下轮）
        若上游 Retry-After 更长 → 同步顺延 quotaUntil
  其它: probeNextAt = now + quotaProbeInterval（不因探针误伤冷却语义）
```
- 探活结果**只影响该 node**；anonymous lane 不参与（它有自己的熔断 half-open）。
- `quotaProbeInterval = cfg.Performance.QuotaProbeSeconds`（默认 600，0=整个循环不启动）。

**配置**：`performance.quota_probe_seconds`（int，默认 600，0=off）。

### 4.4 P2-4 配额预检（零上游请求）

**设计**：`handleInference` 在 `prepareRouteBodies` 之后、`doUpstream` 之前插入：

```
if lane, until, ok := g.quotaWallFor(route); ok {     // route 涉及的所有 key lane 全部 quota 冷却
    retryAfter := ceil(until - now)
    wire.WriteError(w, external, 429,
      "opencode free usage limit: all configured keys are quota-cooled",
      "insufficient_quota", "free_usage_limit")
    w.Header().Set("Retry-After", retryAfter)          // 注意在 WriteError 前设头（实现时核对 WriteError 是否已写头）
    log event=quota_precheck_blocked tier=… retry_after=…
    telemetry: quota_precheck_blocked_total
    return
}
```
- `quotaWallFor(route)`：`route.Tier` 涉及的 key lane（zen/go）逐一 `AllQuotaCooled()`；
  全部命中才预检。**anonymous lane 不纳入**（有自己的熔断）。
- **旁路**：probe / Playground 身份（v1.4.6 已有 `probeRequestIDs` 身份分离，#162）直接跳过预检，
  保证管理面探活永远可达。
- 与 New-API 的配合：429 + Retry-After 让渠道层毫秒级降级到 P5 商业渠道（tebi 10-28 已验证的行为），
  而不是在死墙上重试烧尝试次数。
- **风险核对点**：`doKeyUpstream` 的 cursor 对非空池从不返回 nil（冷却只是降级偏好），
  预检是唯一能在「全墙」时拦住上游调用的闸；不影响部分冷却时的既有换 key 行为。

### 4.5 P2-5 全局并发闸（`max_inflight` / `queue_wait_seconds`）

**设计**：新文件 `internal/gateway/inflight.go`：
```
type inflightGate struct {
    slots chan struct{}     // cap = MaxInflight（0 = 不启用）
    queued atomic.Int64
    wait   time.Duration    // QueueWaitSeconds
}
func (g *inflightGate) acquire(ctx context.Context) (release func(), err error)
  // cap==0 → 直通
  // 有空位 → 占用返回
  // 满且 wait==0 → 立即 errBusy
  // 满 → select { slot / ctx.Done() / time.After(wait) → errBusy }
errBusy → 429 {"type":"rate_limit_error","code":"gateway_busy"}, Retry-After: 5
```
- **挂点**：`handleInference` 最前（认证之后、模型检查之前——并发闸保护一切上游资源）；
  `release` 用 `defer` 挂满整个回合（含流式转发全程）。
- 计数语义：inflight = 正在进行的推理回合（从拿到 slot 到回合结束），与传输层连接数无关。
- **暴露**：WebUI/健康状态 JSON 增加 `inflight`、`queued` 两个字段（admin/server.go 状态端点，
  实现时定位具体 handler；若结构不便则先日志+telemetry，WebUI 项记入 §10 待办）。
- **tebi 建议值**：`max_inflight: 12`（New-API 两渠道各 max_inflight 5 + 管理面余量），
  `queue_wait_seconds: 10`；**默认 0/0 完全关闭**。

### 4.6 P3-6 GEO 分类

**设计**：
- `abuse.go` 新增：
```
var geoBlockPatterns = []string{
  "not available in your country", "not available in your region",
  "unsupported country", "unsupported region", "region not supported",
}
func isGeoBlock(status int, body []byte) bool   // 4xx 且 body 命中
```
- `upstream.go` 尝试循环：GEO 命中 → 按 deterministic 处理（停止跨 key/跨 tier 重试），
  与 `isDeterministicClientResponse` 同路径（abuse.go:126 的调用点，实现时把 GEO 并入该判定或
  在其调用处并列检查）。
- 终态映射：404 + `code: "model_not_available_in_region"`、`type: "invalid_request_error"`
  （对齐 oc-free 语义；不用 403——403 在多数客户端语义里是"可重试的鉴权问题"）。
- **不**调用 availability disable：GEO 是出口属性，WARP 出口切换后同一模型可能可用；
  只在响应里如实告知。

### 4.7 交互矩阵（新特性彼此之间）

| 组合 | 行为 |
|---|---|
| 心跳 + idle 看门狗 | 同一 goroutine；idle 只看上游时钟，心跳帧不复活 idle；idle 触发即 Stop 心跳 |
| idle 看门狗 + rotation 流（首事件前） | rotation 的 prelude hold 窗口有自己的 failure 换模型逻辑，watchdog 仅在 commit 后接管 |
| 预检 + 探活 | 预检给客户端 Retry-After = 最早恢复；探活负责把恢复时刻提前变成现实 |
| 并发闸 + 预检 | 闸在最外层；被闸挡住的请求不进预检（都不打上游，顺序无关紧要，但日志要能区分 busy vs quota） |

---

## 5. 配置 schema 变更汇总

```jsonc
// config.json → "performance" 新增（全部可选，缺省即下表默认）：
{
  "heartbeat_seconds": 15,      // P1-1；0=off
  "idle_timeout_seconds": 0,    // P1-2；0=off；tebi 设 240
  "quota_probe_seconds": 600,   // P1-3；0=off
  "max_inflight": 0,            // P2-5；0=off；tebi 设 12
  "queue_wait_seconds": 0       // P2-5；0=满即拒；tebi 设 10
}
```

**兼容性**：
- 旧 config.json（无新字段）→ `Load` 字面量默认值生效 → 行为 = 现行为 + 心跳开启。
- `DisallowUnknownFields` 要求新字段必须加入 `PerformanceConfig` struct（config.go:232-251）。
- `Normalize` 校验（config.go:373-385 一带追加）：五个新字段均 `>= 0`；
  `idle_timeout_seconds > retry.timeout_seconds` 时记 warn（永远先于总预算触发才有意义）。
- WebUI 配置编辑（admin 动态读写 Config）自动携带新字段，无需前端改动。

## 6. 可观测性

**新日志事件**（结构化 key 不变风格，component=gateway/keepalive/pool）：

| event | 含义 |
|---|---|
| `keepalive_frame`（debug 级） | 每次保活帧（含 protocol、silence） |
| `upstream_idle_timeout` | idle 看门狗触发（session、silence、已转发字节） |
| `quota_probe_recovered` / `quota_probe_still_walled` | P1-3 探活结果（key_id、耗时） |
| `quota_precheck_blocked` | P2-4 预检拒绝（tier、retry_after） |
| `gateway_busy` | P2-5 并发闸拒绝（inflight、queued、等待时长） |
| `geo_block_detected` | P3-6 GEO 终态（model、出口） |

**telemetry/metrics.go**：追加 counter
`keepalive_frames_total`、`upstream_idle_timeouts_total`、`quota_probes_total{result}`、
`quota_precheck_blocked_total`、`gateway_busy_total`、gauge `inflight`/`queued`。
（实现时若 metrics 结构不便扩展，则日志优先、metrics 记入 §10 待办。）

## 7. 测试计划

**单元测试**（新增 `*_test.go`；上游发行树刻意无测试，本仓库恢复测试纪律）：

| 文件 | 用例 |
|---|---|
| `internal/protocol/keepalive_test.go` | ① 静默超阈值发帧且格式按协议正确；② 数据写入重置时钟不发帧；③ 0=off；④ Stop 后无泄漏 goroutine；⑤ 并发 MarkWrite/帧写无数据竞争（-race） |
| `internal/gateway/inflight_test.go` | ① cap 满排队与超时 busy；② wait=0 立即 busy；③ release 后排队者获槽；④ ctx 取消 |
| `internal/gateway/pool_test.go`（追加） | ① 429 quota 措辞 → quotaUntil 置位；② 非 quota 429 不置位；③ MarkSuccess/RestoreProxy 清零；④ AllQuotaCooled 的 earliest 语义 |
| `internal/gateway/abuse_test.go`（追加） | GEO 措辞命中/不命中、非 4xx 不命中 |
| `internal/config/config_test.go`（新增） | 新字段默认值、显式 0、非法负数拒绝、旧 config 无新字段可加载 |

**本地冒烟**（Windows，scratch 配置，anonymous 关闭、无 key）：
1. `go vet ./... && go test ./... && go build` 全绿。
2. 起 dev 二进制 → `GET /v1/models`（应 404 model 或空目录但不崩）→ 确认新配置被接受。
3. **心跳验证**：本地起一个假上游（慢 SSE，每 40s 一行注释），网关指向它 →
   客户端侧 15s 一帧 `: keepalive`；`-race` 下无告警。
4. **idle 验证**：假上游 commit 后完全静默 → 240s（测试用 5s）收到 in-band
   `upstream_idle_timeout` 且假上游连接被断开。

**tebi 回归清单**（部署后）：
- `supervisorctl status opencode2api` RUNNING；healthz `status: ok`，keys 数不变。
- `GET :8318/v1/models` 200，模型清单与升级前一致。
- New-API 渠道 #83/#95 测试请求（若免费池仍 429：**预检应让 429 秒回**，观察
  `quota_precheck_blocked` 日志与渠道侧毫秒级降级——这本身就是 P2-4 的生产验证）。
- 日志无 panic/未知字段报错；WebUI 配置页能看到新字段。

## 8. 版本、仓库与发布策略

- 仓库：`dengyie/opencode2api`（私有创建，随时可转公开；GPL-3.0 传染性要求公开时附源，
  私有仓库不构成分发）。**保留上游 LICENSE 原文**；README 增补「基于 jasonxu114514/opencode2api
  v1.4.6 (GPL-3.0) 增强」出处段。
- git 策略：**全新历史**。commit 1 = 原样基线导入；commit 2 = 本开发文档；
  之后每特性一 commit（`feat(p1-1): …` 风格）。不保留上游 commit 历史（shallow clone 亦不完整），
  上游出处由 LICENSE + README + commit 1 message 承载。
- 版本：`v1.5.0`，构建注入 `-ldflags "-X main.version=v1.5.0"`；tag 推 GitHub。
- 根目录新增 `CHANGELOG.md`：v1.5.0 条目 = §2 总表。

## 9. tebi 部署 SOP

> 生产现状（据 vault [[tebi CPA 栈部署与运维]]）：`/personal/cpa/opencode2api/`，
> supervisor 程序名 `opencode2api`，API `127.0.0.1:8318`、WebUI `127.0.0.1:8319`，
> config.json `0600`，日志 `/tmp/mnt/cpa/logs/opencode2api.log`（NAS NFS 配额敏感）。

1. **预检**：`ssh tebi 'supervisorctl status opencode2api'` RUNNING 才继续。
2. **备份**（二进制+配置快照，放 `/tmp/mnt/` 防 NFS 配额）：
   `ssh tebi 'ts=$(date +%Y%m%d_%H%M%S); mkdir -p /tmp/mnt/oc2api-bak_$ts &&
     cp -a /personal/cpa/opencode2api/opencode2api /personal/cpa/opencode2api/config.json /tmp/mnt/oc2api-bak_$ts/'`
3. **上传**：`scp opencode2api-linux tebi:/tmp/mnt/opencode2api.v1.5.0` →
   `sha256sum` 本地远端比对 → `chmod 755`。
4. **原子替换 + 配置**：
   `mv` 覆盖二进制；`config.json` 增补 §5 五字段（heartbeat 15 / idle 240 / quota_probe 600 /
   max_inflight 12 / queue_wait 10），`chmod 600`。
5. **重启**：`supervisorctl restart opencode2api` → 等 3s → healthz。
6. **回归**：§7 tebi 清单逐项执行。
7. **回滚**：`cp /tmp/mnt/oc2api-bak_$ts/opencode2api → 原位 && 还原 config.json &&
   supervisorctl restart opencode2api`。

## 10. 风险登记册与待办

| 风险 | 缓解 |
|---|---|
| 保活帧与数据帧竞态破坏 SSE | 单帧互斥锁 + `-race` 单测 + 冒烟④ |
| idle 取消 requestCtx 误伤 rotation 换模型 | watchdog 仅 commit 后启动（rotation prelude 有独立 failure 窗口） |
| 探活请求本身烧额度 | 尺寸 ≤ 8 token、全局并发 1、间隔 ≥ 600s、仅在 quota 冷却时运行 |
| 预检误判（quotaUntil 长期残留） | MarkSuccess/RestoreProxy 清零 + 探活自动纠偏 + `?` 管理面可查 |
| 上游未来改版冲突 | 新逻辑收敛在独立文件，挂点 ≤20 行/文件 |
| WebUI 未展示 inflight/queued | 待办（不阻塞 v1.5.0） |
| metrics 扩展受阻 | 待办（日志事件先行） |

---

## 附：实现顺序（commit 粒度）

1. `feat(config): add performance knobs for keepalive/idle/probe/inflight`（§5）
2. `feat(protocol): SSE keepalive frames during upstream silence`（P1-1）
3. `feat(gateway): post-commit idle watchdog cancels walled upstreams`（P1-2）
4. `feat(gateway): quota-aware cooldown + low-frequency key probes`（P1-3）
5. `feat(gateway): quota pre-check rejects with zero upstream calls`（P2-4）
6. `feat(gateway): global inflight gate with bounded queueing`（P2-5）
7. `feat(gateway): classify geo-blocked upstream rejections`（P3-6）
8. `test: unit coverage for keepalive/inflight/pool-quota/geo/config`
9. `docs: changelog + provenance notes in README`
