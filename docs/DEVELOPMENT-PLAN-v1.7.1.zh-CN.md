# DEVELOPMENT PLAN — v1.7.1（凭据隔离加固：哨兵跨 tier 存活 + 探活续期）

> 状态：已实现。本文档是 v1.7.1 的函数级设计记录；实现与设计的任何偏差都必须回写本文。
> 基线：v1.7.0（commit `ba554cc`）。来源：v1.7.0 生产级 code review 的 F1–F5 发现。

## 1. 背景与定位

v1.7.0 上线后的严格 code review 发现两类条件性缺陷与三处测试盲区。生产当前形态（`go_keys` 为空、zen 单 tier）不触发，但都是真实缺陷，按「根因修复 + 测试锁定」处置：

1. **F1（P2）**：`doUpstreamTiers` 把 `errKeyCredentialsExhausted` 当普通 lastErr——zen tier 返回哨兵后，若 Go tier 以纯传输层错误（无 HTTP 响应）收场，`lastErr` 被覆盖，最终返回非哨兵 error → handler 走 generic 502 `upstream_error`，丢失 `key_pool_credential_exhausted` 503 语义与 Retry-After。401 仍不透传（Go tier 的 401 在隔离开启时同样转哨兵），铁律未破。
2. **F2（P3）**：探活 401/403 不刷新隔离期——死 key 在两次 probe 之间出墙回流，下一次真实请求重吃 401（合成 503 兜底）形成锯齿。
3. **F3-F5（P3）**：无双 tier E2E、无 WARP 道凭据测试、Retry-After 只断言存在性。

## 2. 铁律（继承 v1.7.0，新增一条）

1. 401 永不透传；隔离后对外只有合成 503。
2. 配额与凭据两类状态互不污染。
3. 隔离是探活可恢复的。
4. `credential_quarantine_seconds: 0` = 旧行为。
5. 匿名道不隔离。
6. **（新增）响应优先于错误**：后继 tier 的真实 HTTP 响应仍按既有约定获胜（`lastResponse` 非空即返回响应）；哨兵只在「最终没有任何响应」时接管。哨兵优先级不改变这一约定。

## 3. 详细设计

### 3.1 哨兵跨 tier 存活（internal/gateway/upstream.go doUpstreamTiers）

- 函数顶部新增 `credExhausted := false`。
- tier 循环内 `if errors.Is(err, errKeyCredentialsExhausted) { credExhausted = true }`（与 `lastErr = err` 并存）。
- 函数尾部收敛顺序：`blocked` → `lastResponse != nil`（响应获胜，约定不变）→ `credExhausted`（哨兵接管）→ `lastErr`。
- 效果：zen 哨兵 + go 纯传输错误 → 503 + `key_pool_credential_exhausted`；zen 哨兵 + go 真实 5xx 响应 → 照旧转发 5xx（信息量更大且属既有约定）。

### 3.2 探活续期（internal/gateway/pool.go RepinCredentialQuarantine + quotaprobe.go）

- 新方法 `(*upstreamNodes).RepinCredentialQuarantine(node)`：`credentialQuarantine <= 0` 或 `quotaProbeInterval <= 0` 直接 no-op；`credUntil <= now`（已出墙）不复活；剩余隔离 > 一个探活间隔则交给下一次 probe 判决；否则 `credUntil = now + credentialQuarantine`（仅前滚，不缩短），并保持 `cooldownUntil ≥ credUntil` 的超集不变式。
- 调用点：`probeQuotaKey` 的 `http_401/http_403` 分支（`quota_probe_still_credential_dead` 事件前）。
- 正确性论证：探活按 `quotaProbeInterval` 节奏轮转，出墙前最后一个 probe 必然落在最后一段间隔内 → 剩余 < interval → 必触发续期。死 key 因此永久保持隔离（每次续期都需要上游再判一次 401），上游恢复时 probe 200 → `MarkSuccess` 全清。
- MarkFailure 语义不变：真实请求路径的标记仍以 `MarkFailure` 为唯一入口。

### 3.3 测试

| 文件 | 覆盖 |
|---|---|
| `poolquota_test.go` | `TestRepinCredentialQuarantine`（临近出墙前滚 + cooldown 超集、远期不动、过期不复活、=0 no-op）；`TestWARPResult429DoesNotQuarantine` / `TestWARPResultAbuse403DoesNotQuarantine`（WARP 429/网络滥用 403 是出口级，不碰 key）；`TestWARPResult401Quarantines`（WARP 真实 401 隔离 + probe 排期 + 成功全清） |
| `supervision_e2e_test.go` | `TestE2ECredentialSentinelSurvivesGoTierFailure`：zen（全 401 → 哨兵）+ go（POST hijack 断连 = 纯传输错误）双 tier，`prefer: "zen"` 强制 zen 先行，断言 503 + code；`credentialFakeUpstream` capabilities 文档补 `opencode-go` provider（否则 `tierSupportedLocked(go)` 在 catalog 刷新后拒绝 go tier，KeyTiers 退化为 [zen]）；Retry-After 断言收紧为 `1..7200` |

**回归验证**：`git stash -- internal/gateway/upstream.go` 后 E2E 必失败（502 `upstream_error`），恢复后通过——测试真实覆盖修复路径（曾两次误判：单 tier 无覆盖路径、`prefer` 默认 `go` 导致 zen 殿后不被覆盖，均已修正测试设计）。

## 4. 版本与发布策略

- 版本 `v1.7.1`，`-ldflags "-X main.version=v1.7.1"`。
- 单 commit：`fix(gateway): credential sentinel survives cross-tier failover; probe repins near-expiry quarantine`（生产无行为变化的加固 + 测试，不合拆）。
- push main 触发 checks.yml + docker-publish.yml；tebi 生产走本地构建（影子法，同 v1.7.0 SOP）。

## 5. tebi 部署 SOP（影子法，同 v1.7.0）

1. 备份：`cp /personal/cpa/opencode2api/opencode2api /tmp/mnt/opencode2api.v1.7.0_bak_$(date +%Y%m%d_%H%M%S)`。
2. Windows 交叉编译 `GOOS=linux GOARCH=amd64` → sha256 → scp → 本地盘 cp + 原子 mv。
3. `supervisorctl restart opencode2api` → healthz `ok v1.7.1` → 直连 200 → New-API 端到端 200。
4. 回滚：备份件回原位 → restart。

## 6. 风险登记册

| 风险 | 缓解 |
|---|---|
| 哨兵优先级吞掉后继 tier 的真实错误信息 | 响应优先约定不变：有 lastResponse 时哨兵不接管，仅纯错误路径接管 |
| Repin 造成死 key 永久隔离 | 每次续期都需上游再判 401；probe 200 即全清；`0` 配置可整体关闭 |
| Repin 与 MarkFailure 竞态 | 原子量读改写与 MarkFailure 同池串行（probe 单飞 CAS）；最坏情况只是多续或少续一轮间隔 |
| 测试假绿（覆盖路径未被执行） | E2E 用 stash 法回归验证过失败形态；`prefer: "zen"` + go tier capabilities 条目写入测试文档 |
