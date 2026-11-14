# DEVELOPMENT PLAN — v1.7.2（伪装客户端版本随上游自动对齐）

> 状态：已实现。本文档是 v1.7.2 的函数级设计记录；实现与设计的任何偏差都必须回写本文。
> 基线：v1.7.1（commit `a561f2e`）。来源：zcode2api（zcodehub）动态版本追踪器的移植评估。

## 1. 背景与定位

网关以伪装客户端指纹（v1/v2 两条 lane 的 UA、session 头、SDK 版本对）通过上游 v2 gate 指纹校验。伪装版本此前全是编译期常量（`httpx/client.go`），上游发新版后需要人工改码重建。zcode2api 的 `version_tracker.py` 已实证「官方公开发版清单 → semver 比较 → 原子换伪装值」的模式，本版将其移植为 Go 实现：

- **v2 lane 全自动**：UA 形态 `opencode/latest/<version>/cli` 只含一个版本号，可安全自动对齐。
- **v1 lane 冻结**：UA `opencode/<client> ai-sdk/provider-utils/<sdk> runtime/bun/<bun>` 的三值配套来自真实抓包，无公开映射表；自动改 client 版本会造出「新 client + 旧 SDK」矛盾指纹。故 v1 只在本蓝图登记，不做自动化。
- 生产形态（tebi，v2 /inference 匿名道 + zen key 道）属 v2 lane 范畴，检测零成本。

数据源实证（2026-11-14）：GitHub `anomalyco/opencode` releases，latest = v2.3.0；tag 流混有 `process-daemon-v0.5.2` 等组件 tag，必须过滤。

## 2. 铁律

1. **ceiling 钳制（正文锁的对应物）**：自动应用永不越过 `version_sync.ceiling`（空 = 编译期版本）。其余指纹常量是从某个特定 release 抓包的，新 claim 未经验证即错版风险——宁可滞后，不可错版（镜像 zcode2api PROMPT_ALIGNED_VERSION 语义）。
2. **tag 严格过滤**：只接受 `v<纯数字 semver>`（v/V 前缀可剥），组件 tag / draft / prerelease / 带后缀一律忽略。
3. 检测常开，应用需 `auto_sync: true`（默认 false）。
4. 非法 ceiling 拒绝加载（`not-a-version`、带 `-beta` 后缀），不许静默废掉钳制。
5. v1 lane 常量不动，测试锁定其值防漂移。
6. 版本探测失败只告警，旧 claim 是所有 ≤1.7.1 版本的既有状态，下轮重试。

## 3. 详细设计

### 3.1 httpx 原子版本（internal/httpx/client.go）

- `v2ClientVersion` 改 `atomic.Value`，`compileV2ClientVersion = "2.1.1"` 常量种子 + `init()` 装载。
- 新 API：`V2ClientVersion()`（读）、`SetV2ClientVersion(v)`（写，无钳制——钳制是 tracker 的职责）、`V2UserAgent()` 每次调用读原子。
- `V1UserAgent`/`V1ModelUserAgent`/三个 SDK 常量不变。

### 3.2 version tracker（internal/gateway/versiontracker.go）

- `versionTracker{mu, state, ceiling, autoSync, client}`；`newVersionTracker(ceiling, autoSync)`：ceiling 空 → 编译期版本。
- `Gateway.StartVersionSync(ctx)`（runtime.go `start` 接线，镜像 `StartModelRefresh` 启动即查 + ticker）：间隔 `version_sync.interval_seconds`（0 = 12h），单次 fetch 15s 超时。
- `fetchLatestVersion`：GET GitHub releases feed（UA `opencode2api-version-tracker`），JSON 解析，逐条过滤 → 首个稳定 client tag。
- `parseReleaseTag`：仅 `v2.3.0` / `2.3.0` / `V2.3.0` / `v2.3` 形态（可带空白）；拒 `process-daemon-*`、`-beta`、`+build`、`2.3.0.1`、超长段。
- `versionNewer`：三段数值比较，右补零（`2.3` == `2.3.0`）；任何一段解析失败 = 永不误判升级。
- `applyVersionState`（持锁）收敛序：
  1. latest ≤ active → Debug `version_check_current`，`synced=true`；
  2. latest > ceiling → **Warn `version_update_above_ceiling`**（含 hint：先验证再抬 ceiling），`pending=true`，不应用；
  3. latest > active 且 `auto_sync=false` → Info `version_update_detected`，不应用；
  4. `auto_sync=true` 且 ≤ ceiling → `SetV2ClientVersion`，Info `version_synced`。
- `VersionState()` 供 healthz 快照（Active 每次现读原子，防 ApplyConfig 重建 gateway 后旧值）。

### 3.3 config（internal/config/config.go）

- `PerformanceConfig.VersionSync VersionSyncConfig`：`interval_seconds`（0=12h，1..604800）、`auto_sync`（默认 false）、`ceiling`（空=编译期；`v` 前缀剥离；必须纯数字点分段）。
- `validateVersionSync`：区间校验 + ceiling 规范化；非法 ceiling error（不是静默放行）。
- `config.example.json` 补段（12h / false / ""）。

### 3.4 healthz（internal/gateway/health.go）

- `version_sync` 段（tracker 非 nil 才出现）：`latest` / `active` / `ceiling` / `synced` / `pending` / `checked_at` / `source` / `auto_sync`。tracker 随 Gateway 构造必建，实际总出现。

### 3.5 测试（versiontracker_test.go + config_test.go）

| 覆盖 | 断言 |
|---|---|
| `TestParseReleaseTag` | 接受 v/裸/大写 V/两段/空白；拒组件 tag、-beta、+build、四段、字母段 |
| `TestVersionNewer` | 严格大于；补零相等非"更新"；相等/畸形Operand 永不升级 |
| `TestFetchLatestVersionFiltersTags` | 混合 feed 返回首个稳定 client tag；无稳定 tag 报错 |
| `TestVersionStateTracksFeed` | 同版 synced；新版 detected 不改 claim |
| `TestVersionAutoSyncAppliesBelowCeiling` | 应用 + UA 反映 + state synced |
| `TestVersionAutoSyncClampedToCeiling` | 超 ceiling 不应用、pending=true、synced=false |
| `TestVersionDetectionWithoutAutoSync` | 不应用、state 记录漂移 |
| `TestVersionTrackerSeedClampsToCompileVersion` | 空 ceiling 种子=编译期 |
| `TestUserAgentAtomicSwap` | UA 跟原子；v1 UA 不受影响 |
| `TestV1HeadersKeepCapturedSDKPairing` | SDK 三常量钉死（防漂移指纹） |
| `TestLoadVersionSync`（config） | 默认 detect-only；显式值加载 + ceiling 剥 v；非法值拒绝 |

`setTestV2Version` helper：测试后恢复原子（package-global 污染防护）。

## 4. 版本与发布策略

- 版本 `v1.7.2`，`-ldflags "-X main.version=v1.7.2"`。
- 单 commit：`feat(gateway): track upstream client releases and auto-align the v2 masquerade version`。
- push main 触发 CI；tebi 生产走本地构建（影子法，同 v1.7.0/1.7.1 SOP）。

## 5. tebi 部署 SOP（影子法）

1. 备份：`cp /personal/cpa/opencode2api/opencode2api /tmp/mnt/opencode2api.v1.7.1_bak_$(date +%Y%m%d_%H%M%S)`。
2. Windows 交叉编译 `GOOS=linux GOARCH=amd64` → sha256 → scp → 本地盘 cp + 原子 mv。
3. `supervisorctl restart opencode2api` → healthz `ok v1.7.2` + `version_sync` 段出现 → 直连 200 → New-API 端到端 200。
4. 生产 config 不加 `version_sync` 段（默认 detect-only）：首轮即拉 GitHub feed 记录 latest，日志应出现 `version_check_current`（2.1.1 claim vs 2.3.0 latest 实际会是 detected——正常）。
5. 回滚：备份件回原位 → restart。

## 6. 风险登记册

| 风险 | 缓解 |
|---|---|
| 自动应用未验证版本触发上游指纹校验失败 | ceiling 钳制默认=编译期；auto_sync 默认 false；双闸 |
| feed 不可达 / GitHub 限流 | 失败只告警不致命，12h 重试；15s 超时不拖累其它循环 |
| 组件 tag / prerelease 误判为 client 新版 | parseReleaseTag 严格形态过滤（测试锁定） |
| 并发：ApplyConfig 重建 gateway 与 fetch 竞态 | tracker 随 gateway 重建；atomic.Value 并发安全；VersionState 现读 |
| 新 claim 与 SDK 常量矛盾指纹 | v1 lane 完全冻结 + 测试钉死 SDK 三常量 |
| 测试污染 package-global 原子 | setTestV2Version helper + t.Cleanup 恢复 |
