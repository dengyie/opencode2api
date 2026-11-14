# DEVELOPMENT PLAN — v1.7.0（key 凭据隔离：401 内部轮换，全灭才对外报错）

> 状态：已实现。本文档是 v1.7.0 的函数级设计记录；实现与设计的任何偏差都必须回写本文。
> 基线：v1.6.1（commit `c3383c4`）。

## 1. 背景与定位

### 1.1 事故（2026-11-14）

zen key my8Lw 自 11-09 23:53 起被上游持续 `401 Invalid credential`（凭据死），但网关只做 15s×2^n 指数冷却（封顶 8x=120s）：

1. **死 key 每 ≤2min 回流轮换**，反复被打、反复 401，白白烧请求。
2. **最后一个 key 的 401 原样透传**（`doUpstreamTiers` 兜底 lastErr → `copyErrorResponse`），New-API（前置网关）对透传 401 立即自动禁用渠道（status=3 直写 DB），渠道 #95 两次下线。

429 已有完整闭环（v1.5.0：`quotaUntil` + `quota_probe_seconds` 探活恢复 + `AllQuotaCooled` 预检零上游调用），401/403 缺同一套语义。用户要求：**key 报错优先内部轮换处理，实在不行才对外报错**。

### 1.2 铁律（对齐 v1.5.0 quotaWall 语义）

1. **401 永不透传**：凭据隔离后，对外只有合成 503 + Retry-After（New-API 对 503 只冷却不禁用）。
2. **配额与凭据两类状态互不污染**：401 不动 `quotaUntil`，429 不动 `credUntil`（`TestMarkFailureNonQuotaNoQuotaMark` 语义扩展锁定）。
3. **隔离是探活可恢复的**：与 429 共用 `quota_probe` 循环，probe 200 即回流，无需重启。
4. **`credential_quarantine_seconds: 0` = 旧行为**：不标 `credUntil`、不排探活、出口收敛照旧（E2E 锁定）。
5. **匿名道不隔离**：匿名 key 无凭据（`doAnonymousUpstream` 不改），403 IP 滥用封禁走既有 `freeTierBlockTTL`。

## 2. 基线事实

- 冷却集中点：`pool.go` `MarkFailure`（429/401/403/5xx 统一入口）；`MarkSuccess`/`MarkListingSuccess` 清态。
- 探活循环：`quotaprobe.go` `probeQuotaCooledKeys` 只看 `QuotaCooledNodes()`（429 专属）。
- 预检：`gateway.go` `quotaWall()` 只认 `AllQuotaCooled` + 匿名道实证墙。
- 出口：`upstream.go` `doKeyUpstream` 末尾 `lastResponse` 原样返回；`gateway.go` handler 错误分支 `copyErrorResponse` 保真透传。
- WARP 变体：`warp.go` `observeKeyResultWARP` 调 `MarkFailure` 后发 `key_quota_cooled`（`egress=warp`）。

## 3. 详细设计

### 3.1 状态机（internal/gateway/pool.go）

- `upstreamNode.credUntil atomic.Int64`：凭据隔离截止。语义超集：401/403 时 `cooldownUntil = credUntil`，`Cursor.Next()` 的冷却选择天然跳过。
- `upstreamNodes.credentialQuarantine time.Duration`（0 = 关闭）。
- `MarkFailure` switch 三分支：
  - 429 → `quotaUntil` + probeNextAt（不变）；
  - **401/403 且 credentialQuarantine > 0** → `credUntil = now + quarantine`、`cooldownUntil = credUntil`（覆盖 Retry-After——死 key 的 Retry-After 无意义）、`failures = 0`（隔离非连败，回流干净起跑）、probeNextAt 排一轮；
  - 其余（5xx/网络错误）→ 普通指数冷却（不变）。
- `MarkSuccess`/`MarkListingSuccess` 清 `credUntil`（listing 能过 = 上游认了凭据）。

### 3.2 池查询（镜像 429 三件套）

- `CredentialQuarantinedNodes() []*upstreamNode`：`credUntil > now` 的 key（healthz 计数用）。
- `AllCredentialQuarantined() (time.Time, bool)`：全 key 在墙时返回最早回流时刻（预检 Retry-After 用）；空池/部分在墙 = false。
- `ProbeDueNodes() []*upstreamNode`：取代探活循环里的 `QuotaCooledNodes()`——`probeNextAt` 到期 **且**（`quotaUntil` 或 `credUntil` 在墙）的 key；已出墙但 probeNextAt 残留的 key 顺手清零（零值规范）。

### 3.3 观测与事件（upstream.go / warp.go）

`observeKeyResult` 与 `observeKeyResultWARP` 在 `MarkFailure` 后：401/403 且 `credentialQuarantine > 0` → WARN 事件 `key_credential_cooled`（tier/key_id/status/quarantine_seconds；WARP 变体带 `egress=warp`）。

### 3.4 出口收敛（upstream.go）

- 哨兵 `errKeyCredentialsExhausted`（abuse.go，模式对齐 `errGeoBlocked`）。
- `doKeyUpstream` 开头：`AllCredentialQuarantined()` 全在墙 → 直接返回哨兵（0 次尝试）。
- `doKeyUpstream` 末尾：`lastResponse` 为 401/403 且 credentialQuarantine > 0 → drain body，返回哨兵替代透传。`doSelectedKeyUpstream`（debug 路径）不改——调试需要看到真实 401。
- handler 错误分支（gateway.go）：`errors.Is(err, errKeyCredentialsExhausted)` → 设 Retry-After（池内最早 `credUntil`）→ `wire.WriteError(503, keyPoolCredentialExhaustedMessage, "key_pool_credential_exhausted")`。哨兵按普通 lastErr 继续走 next tier（go tier 兜底）。

### 3.5 预检（gateway.go credentialWall）

`quotaWall()` 旁镜像 `credentialWall()`：全部 key `AllCredentialQuarantined` → 503 + Retry-After + 事件 `credential_precheck_blocked` + code `key_pool_credential_exhausted`，零上游调用。429 预检需匿名道实证墙佐证；凭据预检不需要佐证（401 是上游对 key 本身的判决，与出口无关）。

### 3.6 探活恢复（quotaprobe.go）

- `probeQuotaCooledKeys` 改用 `ProbeDueNodes()`（同时覆盖两类墙）。
- `probeQuotaKey` 探测结果 401/403 → 事件 `quota_probe_still_credential_dead` + reschedule；200 → `MarkSuccess`（现成 `quota_probe_recovered` 事件）。

### 3.7 配置与可观测

- `PerformanceConfig.CredentialQuarantineSeconds`（`credential_quarantine_seconds`，默认 86400，校验 0..2592000）。**生产 config.json 零变更**——默认即开，旧二进制 strict decode 不识别新键，部署遵守「先二进制后配置」。
- healthz `keys.credential_quarantined` 计数（informational，不影响 ready）。

## 4. 测试

| 文件 | 覆盖 |
|---|---|
| `poolquota_test.go` | 401/403 隔离标记（credUntil=cooldownUntil、probeNextAt、quotaUntil 不动、failures 归零）；Retry-After 不延长隔离；=0 旧行为；MarkSuccess/MarkListingSuccess 清态；AllCredentialQuarantined 三态；Cursor 跳过隔离 key；ProbeDueNodes 覆盖隔离 key |
| `supervision_e2e_test.go` | `TestE2ECredentialQuarantineLoop`：假上游恒 401 → 503 非 401 + code + Retry-After → healthz 计数 1 → 预检零上游调用 → 上游翻 200 → probe 恢复 → live 200。`TestE2ECredentialQuarantineDisabled`：=0 时 401 透传旧行为 |

本地验收：`go vet ./... && gofmt -l && go test ./... -count=1` 全绿（本机 gcc 32 位，`-race` 不可用；CI race job 兜底）。

## 5. 版本、仓库与发布策略

- 版本 `v1.7.0`，`-ldflags "-X main.version=v1.7.0"`。
- commit 粒度：`feat(gateway)`（状态机+收敛+预检+探活）→ `feat(config)` → `test` → `docs`。
- push main 触发 `checks.yml`（vet/gofmt/race test）与 `docker-publish.yml`（ghcr 语义版镜像）；tebi 生产走本地构建。

## 6. tebi 部署 SOP（影子法，对齐 2026-11-04）

1. 备份：`cp /personal/opencode2api/opencode2api /tmp/mnt/opencode2api.v1.6.1_bak_$(date +%Y%m%d_%H%M%S)`（NFS 件）+ config 快照。
2. Windows 交叉编译 `GOOS=linux GOARCH=amd64` → sha256 → `scp` tebi `/tmp` → `cp` 到本地盘 + 原子 `mv`（NFS 不直写）。
3. `supervisorctl restart opencode2api` → healthz `ok v1.7.0`（keys=5、`credential_quarantined` 字段出现）→ :8318 直连 200 → New-API #95/#83 端到端 200。
4. 回滚：`cp /tmp/mnt/opencode2api.v1.6.1_bak_* → 原位` → restart。

## 7. 风险登记册

| 风险 | 缓解 |
|---|---|
| 403 误隔离（出口被封≠key 死） | `isNetworkAbuseBlock`/geoBlock 路径在 `MarkFailure` 之前分流，真正落到凭据隔离的 403 是上游对 key 的拒绝；隔离可被 probe 提前恢复 |
| 隔离期间全 key 死透 → 渠道 503 | New-API 对 503 只冷却不禁用（实测）；Retry-After 指向最早回流 |
| 隔离太久（默认 24h）错过上游侧修复 | probe 每轮探活，上游恢复即回流；`0` 可回旧行为 |
| 5xx 误入隔离 | switch 精确匹配 401/403，5xx 走普通冷却（测试锁定） |
