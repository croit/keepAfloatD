# Third-Party License Inventory

This inventory summarizes the Rust dependency graph currently resolved from `Cargo.lock` for
`keepafloatd`.

It does not attempt to enumerate Debian or other OS packages pulled in by container images or host
installation paths; its scope is the Rust/Cargo dependency graph for this repository.

- Scope: `cargo metadata --format-version 1 --locked`
- Non-root crates observed: `153`
- Distinct third-party license expressions observed: `11`

Every third-party crate in the current lockfile is usable under a permissive license. The only
crate carrying a copyleft option does so inside an OR expression (`MIT OR Apache-2.0 OR
LGPL-2.1-or-later`), so it can be taken under its permissive alternative (MIT or Apache-2.0) and
imposes no copyleft obligation. No crate is copyleft-only, and none advertises SSPL or
proprietary-only metadata.

The authoritative policy for future changes lives in `deny.toml` and is enforced in CI with
`cargo deny check licenses`.

The project itself is licensed separately under `AGPL-3.0-only` for open-source/community usage,
with a commercial licensing path available from `croit.io`.

## Bundled License Notices

The BSD-3-Clause exception in `deny.toml` applies only to `subtle` version
`2.6.1`. Its full, unmodified copyright notices, conditions and disclaimer
are in [LICENSES/subtle-2.6.1.txt](LICENSES/subtle-2.6.1.txt), copied from
that crate's `LICENSE` file. Public source exports retain this file;
Debian and RPM packages and runtime Docker images install it at
`/usr/share/doc/keepafloatd/licenses/subtle-2.6.1.txt`.
Release downloads include the notice beside the standalone binaries:
GitHub attaches the file, and GitLab links to the exact release commit.

## Observed License Expressions

- `(MIT OR Apache-2.0) AND Unicode-3.0`
- `Apache-2.0`
- `Apache-2.0 OR BSL-1.0`
- `Apache-2.0 OR MIT`
- `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT`
- `BSD-3-Clause`
- `MIT`
- `MIT OR Apache-2.0`
- `MIT OR Apache-2.0 OR LGPL-2.1-or-later`
- `MIT/Apache-2.0`
- `Unlicense OR MIT`

## Packages by Expression

### `(MIT OR Apache-2.0) AND Unicode-3.0`

- `unicode-ident` `1.0.24`

### `Apache-2.0`

- `anyerror` `0.1.13`
- `backoff-series` `0.1.1`
- `base2histogram` `0.2.3`
- `display-more` `0.2.6`

### `Apache-2.0 OR BSL-1.0`

- `ryu` `1.0.23`

### `Apache-2.0 OR MIT`

- `autocfg` `1.5.0`
- `equivalent` `1.0.2`
- `indexmap` `2.14.0`
- `pin-project-lite` `0.2.17`
- `utf8parse` `0.2.2`

### `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT`

- `wasi` `0.11.1+wasi-snapshot-preview1`

### `BSD-3-Clause`

- `subtle` `2.6.1`

### `MIT`

- `bytes` `1.11.1`
- `convert_case` `0.10.0`
- `derive_more` `2.1.1`
- `derive_more-impl` `2.1.1`
- `generic-array` `0.14.7`
- `libyaml-rs` `0.3.0`
- `matchers` `0.2.0`
- `mio` `1.2.0`
- `nu-ansi-term` `0.50.3`
- `parse-size` `1.1.0`
- `parse-zoneinfo` `0.3.1`
- `peel-off` `0.1.1`
- `phf` `0.11.3`
- `phf_codegen` `0.11.3`
- `phf_generator` `0.11.3`
- `phf_shared` `0.11.3`
- `redox_syscall` `0.5.18`
- `sharded-slab` `0.1.7`
- `slab` `0.4.12`
- `strsim` `0.11.1`
- `tokio` `1.54.1`
- `tokio-macros` `2.7.0`
- `tracing` `0.1.44`
- `tracing-attributes` `0.1.31`
- `tracing-core` `0.1.36`
- `tracing-log` `0.2.0`
- `tracing-subscriber` `0.3.23`
- `valuable` `0.1.1`
- `zmij` `1.0.21`

### `MIT OR Apache-2.0`

- `anstream` `1.0.0`
- `anstyle` `1.0.14`
- `anstyle-parse` `1.0.0`
- `anstyle-query` `1.1.5`
- `anstyle-wincon` `3.0.11`
- `anyhow` `1.0.104`
- `bitflags` `2.11.1`
- `block-buffer` `0.10.4`
- `bumpalo` `3.20.2`
- `cc` `1.2.61`
- `cfg-if` `1.0.4`
- `chacha20` `0.10.2`
- `chrono` `0.4.44`
- `chrono-tz` `0.8.6`
- `chrono-tz-build` `0.2.1`
- `clap` `4.6.1`
- `clap_builder` `4.6.0`
- `clap_derive` `4.6.1`
- `clap_lex` `1.1.0`
- `colorchoice` `1.0.5`
- `core-foundation-sys` `0.8.7`
- `cpufeatures` `0.2.17`
- `cpufeatures` `0.3.0`
- `crypto-common` `0.1.7`
- `digest` `0.10.7`
- `either` `1.16.0`
- `errno` `0.3.14`
- `find-msvc-tools` `0.1.9`
- `futures` `0.3.32`
- `futures-channel` `0.3.32`
- `futures-core` `0.3.32`
- `futures-executor` `0.3.32`
- `futures-io` `0.3.32`
- `futures-macro` `0.3.32`
- `futures-sink` `0.3.32`
- `futures-task` `0.3.32`
- `futures-util` `0.3.32`
- `getrandom` `0.4.3`
- `hashbrown` `0.17.0`
- `heck` `0.5.0`
- `hmac` `0.12.1`
- `iana-time-zone` `0.1.65`
- `iana-time-zone-haiku` `0.1.2`
- `is_terminal_polyfill` `1.70.2`
- `itertools` `0.15.0`
- `itoa` `1.0.18`
- `js-sys` `0.3.97`
- `lazy_static` `1.5.0`
- `libc` `0.2.186`
- `lock_api` `0.4.14`
- `log` `0.4.29`
- `num-traits` `0.2.19`
- `once_cell` `1.21.4`
- `once_cell_polyfill` `1.70.2`
- `openraft` `0.10.0-alpha.37`
- `openraft-macros` `0.10.0-alpha.37`
- `openraft-rt` `0.10.0-alpha.37`
- `openraft-rt-tokio` `0.10.0-alpha.37`
- `parking_lot` `0.12.5`
- `parking_lot_core` `0.9.12`
- `proc-macro2` `1.0.106`
- `quorum-set` `0.2.2`
- `quote` `1.0.45`
- `rand` `0.10.1`
- `rand` `0.8.6`
- `rand_core` `0.10.1`
- `rand_core` `0.6.4`
- `regex` `1.12.3`
- `regex-automata` `0.4.14`
- `regex-syntax` `0.8.10`
- `rustc_version` `0.4.1`
- `rustversion` `1.0.22`
- `scopeguard` `1.2.0`
- `semver` `1.0.28`
- `serde` `1.0.228`
- `serde_core` `1.0.228`
- `serde_derive` `1.0.228`
- `serde_json` `1.0.149`
- `sha2` `0.10.9`
- `shlex` `1.3.0`
- `signal-hook-registry` `1.4.8`
- `smallvec` `1.15.1`
- `socket2` `0.6.3`
- `syn` `2.0.117`
- `syn` `3.0.4`
- `thiserror` `2.0.18`
- `thiserror-impl` `2.0.18`
- `thread_local` `1.1.9`
- `typenum` `1.20.1`
- `unicode-segmentation` `1.13.3`
- `unicode-xid` `0.2.6`
- `validit` `0.2.6`
- `wasm-bindgen` `0.2.120`
- `wasm-bindgen-macro` `0.2.120`
- `wasm-bindgen-macro-support` `0.2.120`
- `wasm-bindgen-shared` `0.2.120`
- `windows-core` `0.62.2`
- `windows-implement` `0.60.2`
- `windows-interface` `0.59.3`
- `windows-link` `0.2.1`
- `windows-result` `0.4.1`
- `windows-strings` `0.5.1`
- `windows-sys` `0.61.2`
- `yaml_serde` `0.10.7`

### `MIT OR Apache-2.0 OR LGPL-2.1-or-later`

- `r-efi` `6.0.0`

### `MIT/Apache-2.0`

- `android_system_properties` `0.1.5`
- `maplit` `1.0.2`
- `siphasher` `1.0.3`
- `version_check` `0.9.5`

### `Unlicense OR MIT`

- `aho-corasick` `1.1.4`
- `memchr` `2.8.0`
