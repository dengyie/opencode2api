// Package tetherpool is the client for the optional TetherPool (IP-Pool v2.1)
// control-plane integration (Mode B). RuntimeController asks it to allocate
// one sticky egress binding per Zen key and reports upstream quota walls so
// the control plane can rotate a binding to a fresh exit. Every call is
// best-effort: the gateway degrades to the statically configured proxies
// whenever the control plane is unreachable, so the integration is an
// enhancement and never a dependency.
package tetherpool
