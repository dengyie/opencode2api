# DEVELOPMENT PLAN — v1.6.0（TetherPool Mode B 深度集成）

> 状态：已实现。本文档是 v1.6.0 的函数级设计记录；实现与设计的任何偏差都必须回写本文。
> 基线：v1.5.7（commit `caae018`）。

## 1. 背景与定位

### 1.1 为什么

生产（tebi）现有 6 个 zen key 的出口由 `config.json` 静态写死（5 条 TetherPool Mode A 参数化代理 + warp-sg 直连备用），`nodePool` 按 `i % len(proxies)` 位置轮转绑定。三个痛点：

1. **key 与出口的对应关系靠人工对齐**——加 key 必须手工加代理条目并保证顺序，错位即绑错出口。
2. **墙期只能被动等**——免费层配额按「账号 + 出口 IP」双维计，key 被墙后只能等 Retry-After 或 600s 探活，无法主动换出口 IP。
3. **第 6 个 key（my8Lw）落在 warp-sg 直连备用位**，不在 TetherPool 池内，失去断路器/re-home 保护。

### 1.2 TetherPool Mode B 能力（2026-11-09 实测）

控制面 `:9100`（huawei-vps，EasyTier Mesh `10.144.144.9`）：`POST /v1/allocation/acquire`（service 命名空间绑定，返回 `gateway_proxy_url`）、`POST /v1/battery/event`（429 反馈 + `force_rebind` 换绑，实测换节点换 IP）、绑定死亡由服务端自动 re-home（代理 URL 永不变）。tebi 经 EasyTier no_tun 转发 `127.0.0.1:9101` 已通；`:9100` 转发为部署前置项。

### 1.3 铁律

1. **集成是增强，不是依赖**：控制面不可达时零行为退化（回退静态代理列表），任何 TetherPool 故障不得触碰 key 冷却语义（冷却只归 `MarkFailure`/`MarkSuccess`）。
2. **全或无**：provision 结果要么完整覆盖全部 zen key，要么整体放弃——部分列表会让 `i % N` 位置绑定错位。
3. **密封测试**：一切测试走本地 httptest 假控制面/假上游，CI 无网络必须全绿。
4. **请求路径零阻塞**：墙期反馈走有界 channel + 独立 worker，非阻塞发送，丢弃优于等待。

## 2. 需求与验收标准总表

| # | 需求 | 优先级 | 验收 |
|---|---|---|---|
| P1-1 | 1 key = 1 API 分配绑定 = 1 独立粘性出口 | P1 | build 后 `zenNodes.nodes[i]` 的 proxy 为该 key 的分配 URL；healthz `tetherpool.accounts` 逐 key 可见 |
| P1-2 | 控制面不可达 → 静态回退 + 自愈重试 | P1 | build 不失败、healthz `mode=static_fallback`；worker 每 5min 重试直至成功 |
| P1-3 | 墙期反哺：429 → battery event 主动换绑 | P1 | live 429 与探针 still_walled 均触发；单 key 按 `rebind_min_interval_seconds` 去重 |
| P2-1 | config apply 重建不重复打 API | P2 | 进程内缓存（`displayID|region` 为键），二刷 mode=cache 零调用 |
| P2-2 | 可观测性：事件 + healthz 段 | P2 | `tetherpool_provisioned/fallback/rebind/reprovision` 事件 + healthz `tetherpool` 段 |
| P3-1 | token 鉴权支持 | P3 | `Authorization: Bearer` + 日志 redactor 洗脱 |

**明确不做**（边界）：`X-TetherPool-Error: binding_rehomed` 重放优化（现有重试自愈，P3 备忘）；epoch 增量对账（走网关路由用不到）；匿名道出口改由 Mode B 管理（匿名道继续共用 transports，与现状一致）。

## 3. 基线事实（实现所依据的现状）

- `RuntimeController.build`（runtime.go:201）是唯一构建缝：config apply 全量重建 Gateway（新 transports/pools），无原地变更。
- `Gateway.New` → `transportPool(cfg.RuntimeProxies(), ...)`（gateway.go:126）→ `nodePool(zenKeys/goKeys, transports, cooldown)`（:131/:135）；`nodePool` 以 `proxyIndex := i % len(items)` 位置绑定（pool.go:651）。
- `RuntimeProxies()` 优先返回未导出的 `effectiveProxies`（config.go:575）——注入缝。
- 429 墙检测集中点：`observeKeyResult` 的 `status == 429` 分支（upstream.go:1087，`key_quota_cooled` 事件处）；全墙期唯一观察者是探针循环 `quota_probe_still_walled` 分支（quotaprobe.go:155）——此时 precheck 已挡掉全部 live 请求。
- 后台循环生命周期范式：`rearmQuotaProbes`（runtime.go:166）= root ctx + cancel 字段 + `RuntimeManager`/`applyLocked`/`Shutdown` 三点调用。
- 日志范式：内联字面量 attrs，`"component","gateway"/"upstream","event","<snake>"`；key 一律 `config.KeyDisplayID`。
- 控制面 API 参数名按实现：`/v1/allocation/acquire` 用 `force_rebalance`（API.md 误写 `force_rebind`）；本集成唯一用到它的是 acquire 幂等语义，v1.6.0 不主动调 force 换绑（换绑走 battery event）。

## 4. 详细设计

### 4.1 配置段（internal/config/config.go）

`TetherPoolConfig`（挂在 `Performance` 之后）：`enabled` / `base_url`（默认 `http://127.0.0.1:9100`，tebi 走 EasyTier 转发）/ `service`（默认 `opencode2api`）/ `token`（可选 Bearer，进 redactor）/ `timeout_seconds`（默认 10，1–120，整次 resolve 的总预算）/ `rebind_on_wall`（默认 false，上线后实测疗效再开）/ `rebind_min_interval_seconds`（默认 600，60–86400）/ `regions`（按 key 序轮转的区域偏好，空 = 控制面自选）。

`Normalize()`：先补默认再校验；**范围校验仅在 enabled 时生效**——关闭态是惰性死段，不得拒绝遗留值（有测试锁定）。**交叉校验（review 加固）**：`rebind_on_wall` 必须随 `enabled`，否则 worker 不存在、报告落无读通道静默丢弃。

新导出 `Config.WithRuntimeProxies([]string) Config`：克隆并设 `effectiveProxies`，配合 `RuntimeProxies()` 的优先级实现「注入不落盘」。

### 4.2 控制面 client（internal/tetherpool/client.go）

`NewClient(baseURL, token, timeout)` → `Acquire(ctx, service, accountID, egress, region)`（POST `/v1/allocation/acquire`，region 空则省略参数）/ `BatteryEvent(ctx, BatteryEvent)`（JSON 体）/ `Health(ctx)`。统一 `do()`：`http.NewRequestWithContext` + `httpx.V2UserAgent()` + 2xx 才 decode（`io.LimitReader` 1 MiB）+ 非 2xx 带状态码与 512B 摘要报错。**query 一律 `url.Values.Encode()`**（review 加固）：account_id 源自任意 key 末 5 位，可能含 `&`/`#`/`%`/空格等非查询安全字符，手拼会打歪请求。响应只消费 `gateway_proxy_url`（节点信息仅用于观测）。

### 4.3 Provisioner（internal/tetherpool/provision.go）

`AccountID(key) = "oc2api_" + config.KeyDisplayID(key)`（key 末 5 位，稳定，重启不漂移）。`Resolve(ctx, cfg, logger)`：

1. 未启用或无 zen key → `(nil, Snapshot{}, nil)`，调用方保持静态。
2. 每 key：`displayID|region` 查缓存 → 命中直接用（`Cached:true`）；未命中并发 acquire（信号量 4）。
3. **整次 resolve 共享 `timeout_seconds` 总预算**（`context.WithTimeout`），不得悬挂 config apply。
4. 全部就位 → `entries = [按 zen_keys 序的 gateway_proxy_url...] + cfg.RuntimeProxies()（静态备援）`，快照 `mode = api|cache`（`api` 全新取，`cache` 任一命中）。
5. 任一槽位空 → 返回错误（all-or-nothing），调用方保持静态并记 `static_fallback`。

`validateGatewayURL`：scheme http/https + host 非空，与 `transportPool` 支持面一致。

### 4.4 build 注入（internal/gateway/runtime.go）

`RuntimeController` 增 `tp *tetherpool.Provisioner`、`tpRebind chan tetherpool.RebindRequest`（容量 64，**进程级无条件创建**——首建在 rearm 之前，channel 必须先于 worker 存在）、`tpSnapshot atomic.Value`、`tpCancel`。

`build()` 在 `New` 前插入：enabled 时 `Resolve(m.root, cfg, m.logger)` → 成功 `cfg = cfg.WithRuntimeProxies(merged)` + `tetherpool_provisioned` 事件；失败 `tetherpool_fallback`（WARN）保持静态；快照存 `tpSnapshot` 且 enabled 时挂到 `gateway.tpSnapshot`。`New` 后 attach：`gateway.tpRebind = m.tpRebind`（同 rotation :216 模式）。

`rearmTetherPool(cfg)`（对齐 rearmQuotaProbes 三点式：RuntimeManager 尾部 / applyLocked / Shutdown）：enabled 即启动 worker——worker 同时承担 battery 分发与 **重试自愈**（快照为 `static_fallback` 时按 `tpRetryInterval`（默认 5min，controller 字段非包级变量，避免跨 goroutine 竞争）调 `m.Reload()` 重建；reload 会 rearm 本 worker，旧 worker 经 ctx.Done 退出，无栈叠）。**review 加固**：快照仅在 `New()` 成功后才 `Store`——失败的 apply 不得污染 worker 与 healthz 的真相；`Shutdown()` 改持 `updateMu`——cancel 字段（quotaProbe/tp）的读写与 apply/rearm 全程互斥（顺带修掉 pre-existing 的 quotaProbeCancel 同类竞争）。

### 4.5 墙期反哺（internal/gateway/tetherpool.go + upstream.go + quotaprobe.go + pool.go）

`upstreamNode` 增 `rebindFiredAt atomic.Int64`。`Gateway.observeQuotaWall(node, tier, reason)`：三重门（channel 非空 / `rebind_on_wall` / `tier==zen`）→ `now-fired >= min_interval` 的 CAS 守卫 → 非阻塞 send（满则丢弃并保留时间戳，下个窗口再试，避免打满通道后的重试风暴）。

挂点一：`observeKeyResult` 429 分支（live 流量，reason=`live_429`）；挂点二：`probeQuotaKey` 的 `quota_probe_still_walled` 分支（全墙期唯一观察者，reason=`probe_still_walled`）。

worker 收到即 POST `battery/event{account_id, event_type:rate_limited_429, target_service, force_rebind:true}`，结果记 `tetherpool_rebind` / `tetherpool_rebind_failed`。**语义边界**：换绑只解 IP 侧配额分量，账号侧滚动窗口仍需自然流过；疗效靠线上 `request_routed` 观察。

### 4.6 healthz（internal/gateway/health.go）

可选 `tetherpool` 段（pointer + omitempty，同 `warp`/`load` 模式）：`{mode, accounts: [{account_id, node, region, exit_ip, cached}], updated_at}`。`static_fallback` 是**信息不是故障**——不进 `issues`、不影响 ready/503 判定。

### 4.7 交互矩阵

| 组件 | 交互 | 结论 |
|---|---|---|
| nodePool 位置绑定 | allocated 条目按 zen_keys 序前置 | 1:1 成立；静态条目自然成为 RebindProxy 备援池 |
| 匿名道 | 共用 transports（含 allocated URL） | 与现状同构（今天匿名道就走 5 条 oc2api 静态代理），无回归 |
| WARP 池 | 独立于 proxies | 零改动 |
| 代理健康检查 | allocated URL 也受 `proxy_unavailable/restored` 管理 | 沿用；控制面 re-home 期间 502 由既有重试+冷却吸收 |
| 代理文件/proxyfile | merged 走 `effectiveProxies`，不落盘 | WebUI 保存不受污染 |
| strict decode | 新段必须入 struct | 旧二进制读新 config 会报错——先升二进制再改配置（部署 SOP 顺序保证） |

## 5. 配置 schema 变更汇总

```jsonc
"tetherpool": {
  "enabled": false,                          // 总开关（默认关）
  "base_url": "http://127.0.0.1:9100",       // 控制面；tebi 走 EasyTier 转发
  "service": "opencode2api",                 // 绑定命名空间
  "token": "",                               // 可选 Bearer（服务端设了 IP_POOL_API_TOKEN 才需要）
  "timeout_seconds": 10,                     // 单次调用与整次 resolve 的总预算（1-120）
  "rebind_on_wall": false,                   // 墙期主动换绑（默认关，实测后再开）
  "rebind_min_interval_seconds": 600,        // 单 key 两次换绑报告的最小间隔（60-86400）
  "regions": ["singapore", "tokyo", "india"] // 按 key 序轮转；生产不选 netherlands（2026-11-09 实测 181 节点全灭）
}
```

兼容性：旧 config（无此段）加载即默认关闭、行为零变化；新 config 遇旧二进制会因 `DisallowUnknownFields` 拒绝——部署顺序必须先二进制后配置。

## 6. 可观测性

| 事件 | 级别 | 场景 |
|---|---|---|
| `tetherpool_provisioned` | Info | resolve 成功（带 mode/accounts 数） |
| `tetherpool_fallback` | Warn | resolve 失败保持静态（带 error） |
| `tetherpool_reprovision` / `tetherpool_reprovision_failed` | Info / Warn | static_fallback 态 5 分钟重试 |
| `tetherpool_rebind` / `tetherpool_rebind_failed` | Info / Warn | battery 报告投递结果（带 key_id/reason） |
| healthz `tetherpool` 段 | — | mode + 逐 key 绑定（account/node/region/exit_ip/cached） |

## 7. 测试计划（全部密封）

| 文件 | 用例 |
|---|---|
| `internal/config/config_test.go` | 默认值 / 字段 round-trip / enabled 校验矩阵 / disabled 惰性 / WithRuntimeProxies+Clone 隔离 |
| `internal/tetherpool/tetherpool_test.go` | acquire URL+解码 / token+错误 / battery 体 / 逐 key 1:1+静态备援 / 区域按序轮转（集合断言）/ 缓存零调用 / 全或无 / 坏 URL 拒绝 / disabled no-op / AccountID 派生 |
| `internal/gateway/tetherpool_test.go` | build 注入后 transports 形态 + healthz 渲染 / 控制面不可达 build 不败 + static_fallback / 缓存重建 mode=cache / live 429 → 单报告 + 间隔去重 + 新 episode / 开关与 tier 门控 / worker 投递 battery 体 |

本地验收：`go vet ./... && gofmt -l && go test ./... -race -count=1` 全绿（本机 race 可用）。

## 8. 版本、仓库与发布策略

- 版本 `v1.6.0`，`-ldflags "-X main.version=v1.6.0"`。
- commit 粒度：`feat(config)` → `feat(tetherpool)` → `feat(gateway)` → `docs`，与本文各节一一对应。
- push main 触发 `checks.yml`（vet/gofmt/race test 三 job）与 `docker-publish.yml`（ghcr 镜像）；tebi 生产走本地构建，不受 ghcr 影响。

## 9. tebi 部署 SOP（独立阶段，需另约窗口）

1. **预检**：确认 CI 全绿；`ssh huawei-vps 'curl -s 127.0.0.1:9100/health'` ok。
2. **EasyTier 转发（需窗口）**：tebi `/personal/easytier/config.toml` 追加 `[[port_forward]] bind_addr="127.0.0.1:9100" dst_addr="10.144.144.9:9100"`（备份先行）→ 重启 easytier——**会瞬断全部 mesh 转发**（warp-sg 直连、hybgzs :10823、gem2api、9101 网关等），选低峰并预估秒级恢复。
3. **二进制先行（enabled=false，零行为变更）**：备份 `/tmp/mnt/opencode2api.v1.5.7`（sha256）→ linux/amd64 构建 → sha256 校验 → `install -m 755` + `mv` 原子替换 → `supervisorctl restart opencode2api` → healthz ok + 12 模型矩阵回归。
4. **启用**：config 增补 `tetherpool` 段（enabled=true + regions）→ restart → healthz `tetherpool.mode=api`、6 账号 `accounts` 与 `:9101/metrics` 的 `oc2api_*` 计数互证 → 真实请求 200。
5. **回滚**：config `enabled=false`（或还原 bak）→ restart；二进制回滚 `cp /tmp/mnt/opencode2api.v1.5.7 ...`。

## 10. 风险登记册

| 风险 | 缓解 |
|---|---|
| 控制面故障拖慢启动 | resolve 总预算 `timeout_seconds` 封顶；失败即静态回退，build 永不因 TetherPool 失败 |
| 部分分配导致绑定错位 | all-or-nothing + 进程缓存填槽（有测试锁定） |
| 换绑风暴 | 单 key 间隔守卫（CAS）+ 容量 64 channel 非阻塞丢弃 + 默认关 |
| key 末 5 位撞号 → 共享同账号绑定 | Resolve 检出即 WARN（`tetherpool_duplicate_key_id`），语义为共享出口非故障（idempotent acquire）；理论碰撞空间 62^5，现网 6 key 无撞 |
| healthz（未鉴权）新增 exit_ip/node 暴露 | 评估接受：生产 API 绑 127.0.0.1 回环，WARP 出口为共享段非敏感；与既有 `proxies` 计数段同端点演进 |
| Reload 重试与用户操作竞争 | worker 经 `m.Reload()`→`Apply` 走 `updateMu`，与 WebUI/管理 API 天然互斥 |
| gateway_proxy_url 语法耦合 | URL 由控制面返回（不本地拼接）；`validateGatewayURL` 兜底，坏响应按失败回退 |
| 免费 quota 的账号侧分量不被换绑解除 | 文档明示预期；疗效以 `request_routed` 实测为准，`rebind_on_wall` 默认关 |
