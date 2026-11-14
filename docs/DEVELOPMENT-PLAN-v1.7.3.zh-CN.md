# DEVELOPMENT PLAN — v1.7.3（热路径性能与内存优化 + 可观测基座）

> 状态：已实现。本文档是 v1.7.3 的函数级设计记录；实现与设计的任何偏差都必须回写本文。
> 基线：v1.7.2（commit `413f425`）。来源：两轮热路径/内存调研 + 用户选定范围「热路径全套」。
> 部署后 review 增补（commit `3619e90`）：pprof/memory_limit 为启动期生效字段，`EffectiveListeners`/
> `RestartStatus`/`applyLocked`/`EffectiveView` 四处补齐，改动经 WebUI 保存后现在会进
> `restart_required_fields` 提示重启（修复前静默无效）；测试 `runtime_restart_test.go` 锁定。

## 1. 背景与定位

v1.7.2 上线后生产 168/168 请求全 200，功能面稳定；本版纯性能与可观测性，不改任何对外行为。调研定位四类热路径浪费：

1. **JSON 重复劳动**：匿名道对每个上游出口提前 shape 全部 body（`prepareAnonymousBody` 每出口一次全量 parse）；v1 Anthropic lane 的 beta 头每次 key 尝试都从同一 body 全量 typed decode。
2. **编码缓冲零池化**：`jsonutil.Marshal` 每次 `new(bytes.Buffer)`，全仓热路径调用点集中在 gateway.go / upstream.go。
3. **流式 usage 观察器全流双拷贝**：`streamUsageTracker.Write` 把整条流所有 SSE frame `append([]byte(nil),...)` 拷出 + `consume` 里 `string(frame)` 再拷一次 + 每帧新建 `strings.NewReader` + scanner 回放——长流内存 O(流长)。
4. **无运行时观测基座**：无 pprof、无 GOMEMLIMIT。

明确不做（范围外）：WARP transport 复用、log ring × dump_request_bodies 治理、`conversationSeed`（核实后每请求至多一次且仅在无会话头时，不动）、`prepareRouteBodies`（每 tier body 因协议不同必须独立编码，入口 parse 已只有一次）。

## 2. 铁律

1. **伪装指纹字节级不变**：所有 marshal 保持 `SetEscapeHTML(false)`；v2 chat body 键序（`v2ChatKeyOrder`）不动；v1 beta 头派生结果与 typed decode 逐字节一致（测试锁定）。
2. **等价变换**：错误码、事件名、healthz 字段语义、SSE 分帧语义（含 CRLF、未终止尾部不入账）全部不变。
3. **池化不泄漏**：pooled buffer 的字节绝不以别名逃逸——小于 256KB 拷出，大于等于 256KB buffer 不还池（backing array 随返回值走）。
4. **安全默认**：pprof 默认关（挂载即鉴权、仅 WebUI 端口）；memory_limit 默认 0（Go 默认行为）。
5. 全量 `go test -race` 绿后才部署。

## 3. 详细设计

### 3.1 jsonutil 自适应缓冲池（internal/jsonutil/encode.go）

- 包级 `bufferPool`（`sync.Pool`，New 返回 `*bytes.Buffer`）；`poolableBufferCap = 256KB`。
- `Marshal`：借 buffer → encode → **Cap ≤ 256KB**：拷出独立 `[]byte` + Reset + 还池；**Cap > 256KB**：字节随 backing array 直接返回，buffer 丢弃不还池（避免全量 memcpy 或池钉住 MB 级内存）。
- 基准（-count 3，i5-10400F）：

| 场景 | 旧（fresh buffer） | 新（自适应池） |
|---|---|---|
| Small (~60B body) | 975 ns, 313 B/op, 10 allocs | 976 ns, **249 B/op, 9 allocs** |
| Large (~1.2MB body) | 1237 µs, 1160871 B/op, 14 allocs | 1372 µs, **1162637 B/op, 14 allocs**（持平；并发下池化收益更大） |

### 3.2 匿名道惰性 shape（upstream.go + warp.go）

- `doAnonymousUpstream` / `doWarpAnonymousUpstream` 的出口循环改为 `shapedBody{once sync.Once; body; renames}` 惰性结构：首次真正向该出口发起尝试时才 `prepareAnonymousBody`。今天 `anonymousUpstreams()` 只返回 1 个出口（v1 lane 已退役注释），收益 = 出口循环在首试前 break 时省掉整次 parse+encode；未来恢复多出口时按需付费。

### 3.3 v1 beta 头缓存（upstream.go）

- `cachedAnthropicBetas(body []byte)`：SHA-256 前 16 字节为键的 map 缓存（`maxCachedBetaBodies = 64`，时间戳 map + 最旧驱逐，镜像 telemetry 会话 LRU 惯例）；`setV1Headers` 的 anthropic 分支改调它。key 轮换 MaxAttempts 次尝试中只有首次付 typed decode。
- 单测（betacache_test.go）：缓存结果与直算 `v1AnthropicBetas` 逐字节一致（覆盖 mcp/context-management/speed/tool 分支）；驱逐后不超 64 条且幸存条目仍正确；并发同 body 全部看到同一派生。

### 3.4 streamUsageTracker 零拷贝扫描（internal/protocol/stream.go）

- `Write`：frame 直接以 `observer.buffer[:index+width]` 切片喂给 `consume`（不拷 frame）；消费后把残余尾 `append` 到新小 buffer，防旧大 backing array 钉住。
- `consume`：重写为 frame 字节上的内联逐行扫描（镜像 `readSSE` 记录语义：空行结束记录、`:` 注释跳过、`event:`/`data:` 前缀剥离、data 行 `\n` join、`\r` 后缀剥离）；`parseEvent` 拆出独立方法。**eventName 在 parseEvent 消费后才清空**（首个实现的顺序 bug：置空先行会丢失 Anthropic `event: error` 行名，测试抓住后修复）。
- `Finish` 语义不变：未终止尾部丢弃、不计 usage。
- 基准（512 帧 × ~300B，8 chunk Write，长流 ~150KB）：

| 指标 | 旧 | 新 |
|---|---|---|
| time/op | 7.24 ms | **1.76 ms**（4.1×） |
| B/op（分配总量） | 35.5 MB | **1.58 MB**（-95.6%） |
| 吞吐 | 21.35 MB/s | **87.64 MB/s** |

- 单测（stream_tracker_test.go）：跨 Write 半帧/半 JSON 分片、CRLF 终止符、Anthropic message_start usage + error 事件、未终止尾部不入账、parse error 记忆不中断、缓冲有界性（千帧后 buffer 为 0 字节、partial 尾恰为 partial 大小）。

### 3.5 可观测基座

- **config**：`PerformanceConfig.MemoryLimitBytes int64 json:"memory_limit_bytes"`（≥0，0=Go 默认）、`PprofEnabled bool json:"pprof_enabled"`（默认 false）；Normalize 校验非负。
- **pprof**（internal/admin/server.go）：`Handler()` 在 `performance.pprof_enabled` 时挂 `mux.Handle("/debug/pprof/", a.authenticate(http.HandlerFunc(pprofHandler)))`——仅 WebUI admin 端口、复用既有会话认证；`pprofHandler` 显式分发 Cmdline/Profile/Symbol/Trace/Index，不触碰 DefaultServeMux。关闭时路由不注册（404 由 staticHandler 的 /api/ 分支兜不住 → 落到 SPA fallback，实际 `/debug/pprof/` 返回 index.html；见风险表）。
- **GOMEMLIMIT**（cmd/opencode2api/main.go）：`>0` 时启动 `debug.SetMemoryLimit` + Info `memory_limit_set` 事件。
- config.example.json 补两键（0 / false）。

## 4. 版本与发布策略

- 版本 `v1.7.3`，`-ldflags "-X main.version=v1.7.3"`。
- 单 commit：`perf(gateway): hot-path buffer pooling, lazy shaping, beta-header cache, zero-copy stream tracking, pprof/memory observability`。

## 5. tebi 部署 SOP（影子法，同 v1.7.2）

1. 备份：`cp /personal/cpa/opencode2api/opencode2api /tmp/mnt/opencode2api.v1.7.2_bak_$(date +%Y%m%d_%H%M%S)`。
2. Windows 交叉编译 `GOOS=linux GOARCH=amd64` → sha256 → scp → 本地盘 cp + 原子 mv。
3. `supervisorctl restart opencode2api` → healthz `ok v1.7.3` + `version_sync` 段不变 → 直连 200 → New-API 端到端 200。
4. 生产 config 零变更（新键全有安全默认；遵守「先二进制后配置」）。
5. 回滚：备份件回原位 → restart。

## 6. 风险登记册

| 风险 | 缓解 |
|---|---|
| 池化 buffer 别名逃逸（两个请求共享底层内存） | ≤256KB 强制拷出 + >256KB 不还池；encode_test.go 并发/独立性断言 + -race |
| v2 chat body 键序/HTML 转义漂移（错版指纹） | Marshal 语义测试锁定（不转义、去尾换行）；全量 E2E 走真实伪装路径 |
| beta 缓存碰撞或陈旧 | SHA-256 前 16 字节（需二次原像才碰撞）；键=body 全量，body 变即键变；等价性测试 |
| tracker 重写丢 eventName / CRLF / 尾部语义 | 专项测试套（分片、CRLF、error 事件、尾部不入账）；全量 protocol + gateway E2E |
| pprof 误暴露 | 默认关；开启也仅 WebUI 端口 + 会话认证 |
| GOMEMLIMIT 设置过低触发 GC 死循环 | 默认 0 不设；生产暂不配 |
| 本机 -race 需 64-bit cgo | C:/Users/mango/scoop/apps/mingw/current/bin/gcc.exe（C:\MinGW 是 32-bit，不可用） |
