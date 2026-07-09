# Third-Party License Inventory

This inventory summarizes the Rust dependency graph currently resolved from `Cargo.lock` for
`keepafloatd`.

It does not attempt to enumerate Debian or other OS packages pulled in by container images or host
installation paths; its scope is the Rust/Cargo dependency graph for this repository.

- Scope: `cargo metadata --format-version 1 --locked`
- Non-root crates observed: `182`
- Distinct third-party license expressions observed: `13`

Every third-party crate in the current lockfile is usable under a permissive license. The only
crate carrying a copyleft option does so inside an OR expression (`MIT OR Apache-2.0 OR
LGPL-2.1-or-later`), so it can be taken under its permissive alternative (MIT or Apache-2.0) and
imposes no copyleft obligation. No crate is copyleft-only, and none advertises SSPL or
proprietary-only metadata.

The authoritative policy for future changes lives in `deny.toml` and is enforced in CI with
`cargo deny check licenses`.

The project itself is licensed separately under `AGPL-3.0-only` for open-source/community usage,
with a commercial licensing path available from `croit.io`.

## Observed License Expressions

- `(MIT OR Apache-2.0) AND Unicode-3.0`
- `Apache-2.0`
- `Apache-2.0 OR BSL-1.0`
- `Apache-2.0 OR MIT`
- `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT`
- `BSD-2-Clause OR Apache-2.0 OR MIT`
- `MIT`
- `MIT OR Apache-2.0`
- `MIT OR Apache-2.0 OR LGPL-2.1-or-later`
- `MIT OR Apache-2.0 OR Zlib`
- `MIT/Apache-2.0`
- `Unlicense OR MIT`
- `Zlib OR Apache-2.0 OR MIT`

## Packages by Expression

### `(MIT OR Apache-2.0) AND Unicode-3.0`

- `unicode-ident` `1.0.24`

### `Apache-2.0`

- `anyerror` `0.1.13`
- `backoff-series` `0.1.1`
- `base2histogram` `0.2.3`
- `borsh-derive` `1.6.1`
- `display-more` `0.2.6`

### `Apache-2.0 OR BSL-1.0`

- `ryu` `1.0.23`

### `Apache-2.0 OR MIT`

- `autocfg` `1.5.0`
- `equivalent` `1.0.2`
- `indexmap` `2.14.0`
- `pin-project-lite` `0.2.17`
- `utf8parse` `0.2.2`
- `uuid` `1.23.1`

### `Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT`

- `wasi` `0.11.1+wasi-snapshot-preview1`

### `BSD-2-Clause OR Apache-2.0 OR MIT`

- `zerocopy` `0.8.48`
- `zerocopy-derive` `0.8.48`

### `MIT`

- `bitvec` `1.0.1`
- `bytecheck` `0.6.12`
- `bytecheck_derive` `0.6.12`
- `bytes` `1.11.1`
- `byte-unit` `5.2.0`
- `cfg_aliases` `0.2.1`
- `convert_case` `0.10.0`
- `derive_more` `2.1.1`
- `derive_more-impl` `2.1.1`
- `funty` `2.0.0`
- `matchers` `0.2.0`
- `mio` `1.2.0`
- `nu-ansi-term` `0.50.3`
- `parse-zoneinfo` `0.3.1`
- `peel-off` `0.1.1`
- `phf` `0.11.3`
- `phf_codegen` `0.11.3`
- `phf_generator` `0.11.3`
- `phf_shared` `0.11.3`
- `ptr_meta` `0.1.4`
- `ptr_meta_derive` `0.1.4`
- `radium` `0.7.0`
- `redox_syscall` `0.5.18`
- `rend` `0.4.2`
- `rkyv` `0.7.46`
- `rkyv_derive` `0.7.46`
- `rust_decimal` `1.41.0`
- `schemars` `1.2.1`
- `seahash` `4.1.0`
- `sharded-slab` `0.1.7`
- `slab` `0.4.12`
- `strsim` `0.11.1`
- `tap` `1.0.1`
- `tokio` `1.52.1`
- `tokio-macros` `2.7.0`
- `tracing` `0.1.44`
- `tracing-attributes` `0.1.31`
- `tracing-core` `0.1.36`
- `tracing-log` `0.2.0`
- `tracing-subscriber` `0.3.23`
- `unsafe-libyaml` `0.2.11`
- `utf8-width` `0.1.8`
- `valuable` `0.1.1`
- `winnow` `1.0.2`
- `wyz` `0.5.1`
- `zmij` `1.0.21`

### `MIT OR Apache-2.0`

- `ahash` `0.7.8`
- `anstream` `1.0.0`
- `anstyle` `1.0.14`
- `anstyle-parse` `1.0.0`
- `anstyle-query` `1.1.5`
- `anstyle-wincon` `3.0.11`
- `anyhow` `1.0.102`
- `arrayvec` `0.7.6`
- `bitflags` `2.11.1`
- `borsh` `1.6.1`
- `bumpalo` `3.20.2`
- `cc` `1.2.61`
- `cfg-if` `1.0.4`
- `chacha20` `0.10.1`
- `chrono` `0.4.44`
- `chrono-tz` `0.8.6`
- `chrono-tz-build` `0.2.1`
- `clap` `4.6.1`
- `clap_builder` `4.6.0`
- `clap_derive` `4.6.1`
- `clap_lex` `1.1.0`
- `colorchoice` `1.0.5`
- `core-foundation-sys` `0.8.7`
- `cpufeatures` `0.3.0`
- `dyn-clone` `1.0.20`
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
- `getrandom` `0.2.17`
- `getrandom` `0.4.3`
- `hashbrown` `0.12.3`
- `hashbrown` `0.17.0`
- `heck` `0.5.0`
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
- `openraft` `0.10.0-alpha.25`
- `openraft-macros` `0.10.0-alpha.25`
- `openraft-rt` `0.10.0-alpha.25`
- `openraft-rt-tokio` `0.10.0-alpha.25`
- `parking_lot` `0.12.5`
- `parking_lot_core` `0.9.12`
- `ppv-lite86` `0.2.21`
- `proc-macro2` `1.0.106`
- `proc-macro-crate` `3.5.0`
- `quote` `1.0.45`
- `rand` `0.10.1`
- `rand` `0.8.6`
- `rand_chacha` `0.3.1`
- `rand_core` `0.10.1`
- `rand_core` `0.6.4`
- `ref-cast` `1.0.25`
- `ref-cast-impl` `1.0.25`
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
- `serde_yaml` `0.9.34+deprecated`
- `shlex` `1.3.0`
- `signal-hook-registry` `1.4.8`
- `simdutf8` `0.1.5`
- `smallvec` `1.15.1`
- `socket2` `0.6.3`
- `syn` `1.0.109`
- `syn` `2.0.117`
- `thiserror` `2.0.18`
- `thiserror-impl` `2.0.18`
- `thread_local` `1.1.9`
- `toml_datetime` `1.1.1+spec-1.1.0`
- `toml_edit` `0.25.11+spec-1.1.0`
- `toml_parser` `1.1.2+spec-1.1.0`
- `unicode-segmentation` `1.13.3`
- `unicode-xid` `0.2.6`
- `validit` `0.2.5`
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

### `MIT OR Apache-2.0 OR LGPL-2.1-or-later`

- `r-efi` `6.0.0`

### `MIT OR Apache-2.0 OR Zlib`

- `tinyvec_macros` `0.1.1`

### `MIT/Apache-2.0`

- `android_system_properties` `0.1.5`
- `maplit` `1.0.2`
- `siphasher` `1.0.3`
- `version_check` `0.9.5`

### `Unlicense OR MIT`

- `aho-corasick` `1.1.4`
- `memchr` `2.8.0`

### `Zlib OR Apache-2.0 OR MIT`

- `tinyvec` `1.11.0`
