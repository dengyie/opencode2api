package httpx

import (
	"hash/fnv"
	"sync/atomic"
)

// v2ClientVersion is the OpenCode 2.x CLI release the gateway claims to be on
// the v2 lanes (/inference and /zen/go). The version number itself is not
// validated, so this only needs bumping when upstream starts rejecting older
// ones. It is mutable (v1.7.2): the version tracker rewrites it when it
// detects a newer official release and version_sync is enabled.
var v2ClientVersion atomic.Value

// compileV2ClientVersion is the release the gateway was built to claim. It
// seeds the atomic value and acts as the ceiling when
// performance.version_sync_ceiling is left at 0: an auto-synced version newer
// than the release the remaining masquerade constants were captured from is a
// contradiction fingerprint, exactly like the v1 SDK pairings below.
const compileV2ClientVersion = "2.1.1"

func init() { v2ClientVersion.Store(compileV2ClientVersion) }

// v1ClientVersion is the OpenCode 1.x release the gateway claims to be on the
// v1 lane (/zen), and v1BunVersion is the Bun runtime that release ships with.
// Bump them together with the provider-utils versions below, all taken from a
// captured request of the same release.
const (
	v1ClientVersion = "1.19.3"
	v1BunVersion    = "1.3.14"
)

// The AI SDK provider packages of OpenCode 1.19.3 each bundle their own
// provider-utils, whose version ends up in the User-Agent of every v1 model
// request, so the version depends on the wire protocol.
const (
	// V1ChatSDK is bundled by @ai-sdk/openai-compatible (chat completions).
	V1ChatSDK = "4.0.23"
	// V1ResponsesSDK is bundled by @ai-sdk/openai (responses).
	V1ResponsesSDK = "4.0.40"
	// V1AnthropicSDK is bundled by @ai-sdk/anthropic (messages).
	V1AnthropicSDK = "4.0.46"
)

// V2ClientVersion reports the release the v2 lane currently claims. Read on
// every call site, so a tracker update takes effect on new requests without a
// restart.
func V2ClientVersion() string {
	if v := v2ClientVersion.Load(); v != nil {
		return v.(string)
	}
	return compileV2ClientVersion
}

// SetV2ClientVersion installs a new claimed v2 release. The compile-time
// version acts as a floor for validity, not as a ceiling: callers enforce
// their own ceiling (the version tracker clamps to
// performance.version_sync_ceiling), and a restart restores the compile-time
// constant.
func SetV2ClientVersion(version string) { v2ClientVersion.Store(version) }

// v2FingerprintPool holds the v2 releases per-key User-Agents are drawn from.
// Every entry is a release the official client actually shipped (the compile
// version plus everything the version tracker has applied), so a per-key
// claim never invents a fingerprint upstream never saw. nil means every key
// shares the global claimed version.
var v2FingerprintPool atomic.Value

// fingerprintPoolSeed lists the real releases the initial pool is seeded
// with: the compile-time capture plus the two stable releases published
// since. Only versions at or below the tracker's ceiling survive (seeded in
// newVersionTracker), so the pool never claims ahead of the verified global.
var fingerprintPoolSeed = []string{"2.2.7", "2.3.0"}

// FingerprintPoolSeed returns the seed versions the per-key pool starts
// from. The caller (the version tracker) combines them with the compile-time
// version and prunes to its ceiling.
func FingerprintPoolSeed() []string {
	return append([]string(nil), fingerprintPoolSeed...)
}

// V2FingerprintPool returns the per-key version pool, empty when unset.
func V2FingerprintPool() []string {
	if v := v2FingerprintPool.Load(); v != nil {
		return v.([]string)
	}
	return nil
}

// SetV2FingerprintPool replaces the per-key version pool. Duplicate versions
// are dropped; an empty list clears the pool, collapsing every key back to
// the global claimed version.
func SetV2FingerprintPool(versions []string) {
	if len(versions) == 0 {
		v2FingerprintPool.Store([]string(nil))
		return
	}
	seen := make(map[string]bool, len(versions))
	deduped := make([]string, 0, len(versions))
	for _, version := range versions {
		if version == "" || seen[version] {
			continue
		}
		seen[version] = true
		deduped = append(deduped, version)
	}
	v2FingerprintPool.Store(deduped)
}

// V2VersionForKey deterministically picks the v2 release a key claims from
// the fingerprint pool. Same fingerprint, same version for the life of the
// process (and across restarts, since the pool seeds from constants before
// the tracker widens it deterministically); an empty or single-version pool
// collapses to the global claimed version, as does an unknown key fingerprint.
func V2VersionForKey(keyFingerprint string) string {
	pool := V2FingerprintPool()
	if len(pool) <= 1 {
		return V2ClientVersion()
	}
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(keyFingerprint))
	return pool[hash.Sum64()%uint64(len(pool))]
}

// V2UserAgent reports the gateway as an OpenCode 2.x CLI using the shape the
// v2 gate expects: "opencode/<channel>/<version>/<name>".
func V2UserAgent() string {
	return V2UserAgentForVersion(V2ClientVersion())
}

// V2UserAgentForVersion renders the v2 User-Agent for a specific claimed
// release. An empty version falls back to the global claim, the shared shape
// for the anonymous lane and any caller without a key-bound fingerprint.
func V2UserAgentForVersion(version string) string {
	if version == "" {
		version = V2ClientVersion()
	}
	return "opencode/latest/" + version + "/cli"
}

// V1UserAgent is the OpenCode 1.x client's own User-Agent, a bare
// "opencode/<version>", which it sends as-is on requests that bypass the AI
// SDK. The earlier "(<os> <arch>; <runtime>)" suffix carried the gateway's Go
// runtime, which no real OpenCode client sends.
func V1UserAgent() string {
	return "opencode/" + v1ClientVersion
}

// V1ModelUserAgent is the User-Agent of a v1 model request: the AI SDK appends
// the provider-utils version of the package serving the request and the
// runtime to the client's own User-Agent, e.g.
// "opencode/1.19.3 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14".
func V1ModelUserAgent(providerUtils string) string {
	return V1UserAgent() + " ai-sdk/provider-utils/" + providerUtils + " runtime/bun/" + v1BunVersion
}
