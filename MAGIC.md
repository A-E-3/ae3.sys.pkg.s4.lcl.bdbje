# MAGIC.md — ae3.sys.pkg.s4.lcl.bdbje

## For keeper-ae3

- **Stale dependency removed** (2026-08-25): `Requires: ae3.sys` (and the matching `.classpath` entry) was declared but never actually used. Verified every real import in this package's source — all resolve to `ae3.api` (`Base`, `BaseList`, `Act`, `Transfer`, `TransferCopier`, `Console`, `Engine`, `Reflect`, `Report`, `Exec`) or `ae3.sys.pkg.s4` (`ru.myx.ae3.vfs.s4.common/driver/impl.*`), never to a class actually defined in `ae3.sys` itself. Matches the general architecture rule: plugins/storage-backends (`ae3.sys.pkg.*`) compile against `ae3.api`/`ae3.sdk` (the contract layer), never `ae3.sys` (the concrete implementation) directly. Removed from both `project.inf` and `.classpath`, in both the legacy (`/Volumes/workspace/myx`) and modern (`ws-myx.ae3-devel`) checkouts.
- **`ae3.pkg.lib.bdbje` gap**: this package's real dependency on `ae3.pkg.lib.bdbje` (Berkeley DB JE vendoring + real custom `ru.myx.bdbje.*` utility/upgrade code) has no git repo anywhere yet (legacy or modern) and isn't registered in `util.repository-ae3`'s checkout list — a real candidate for a new `lib.bdbje`-style repo, not yet created.
