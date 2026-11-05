# Security Policy

## Reporting a vulnerability

Please open a [GitHub Security Advisory](https://github.com/dengyie/opencode2api/security/advisories/new)
rather than a public issue. Include the affected version, a minimal reproduction,
and your assessment of impact.

## Deployment notes

- This gateway is designed for **self-hosted, personal use**. Do not expose it
  to the public internet without your own authentication layer in front.
- The WebUI requires a non-default username and password (argon2id) — keep them
  set, and put the WebUI port behind TLS or an SSH tunnel.
- The upstream quota-farming features (WARP egress rotation, free-tier key
  pooling) may violate the terms of service of the upstream provider. Use at
  your own risk; this software is provided without warranty, see LICENSE.
