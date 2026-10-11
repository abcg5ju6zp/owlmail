# OwlMail 0.10.1

Released: 2026-10-11

This maintenance release fixes a plain SMTP listener shutdown race and refreshes the build toolchain and dependencies.

## Fixed

`Close` now closes the plain SMTP listener directly during the startup window before `smtp.Server.Serve` owns it. Rapid startup/shutdown no longer leaves the listener bound and the serving goroutine blocked.

## Changed

- Build with Go 1.27.2.
- Upgrade soulteary kits and third-party dependencies, including SQLite/modernc and AWS SDK packages.
- Publish binaries and a multi-architecture container image from the same immutable tag.

## Upgrade

Use `ghcr.io/soulteary/owlmail:0.10.1` or a verified image digest. Runtime APIs, configuration, and storage formats remain compatible with 0.10.0. The security protections introduced in 0.10.0 remain enabled; deployments upgrading from older releases should also read [the 0.10.0 migration notes](Release-0.10.0.md).
