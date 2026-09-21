# Protocol v1 dependency record

Go codec: standard library only. Rust: serde_json exactly 1.0.151, default features disabled, features `std` and `raw_value`. No derive, arbitrary_precision, preserve_order or unbounded_depth feature is enabled. The raw-value parser avoids rounding unknown payload numbers; a bounded scanner rejects duplicate keys and ambiguous Unicode before decoding.

Five packages are in the normal/build dependency graph. Cargo.lock also records packages behind constant-false `cfg(any())` edges; those are listed separately and are not compiled by this feature selection. All archive checksums below were independently recomputed against the actual downloaded crate files and match Cargo.lock. Licenses/attributions in THIRD_PARTY_NOTICES.md belong to third-party code only, with no grant implied for NewIM itself.

| Package | Source revision | Compile graph | Crate SHA-256 | License |
| --- | --- | --- | --- | --- |
| itoa 1.0.18 | af77385d0daf4d0e949e81f2588be2e44f69f086 | normal/build | 8f42a60cbdf9a97f5d2305f08a87dc4e09308d1276d28c869c684d7777685682 | MIT OR Apache-2.0 |
| memchr 2.8.3 | 5fdb40c054e1fff359a2f7bdf7f87a13b34b465d | normal/build | cf8baf1c55e62ffcace7a9f06f4bd9cd3f0c4beb022d3b367256b91b87513d98 | Unlicense OR MIT |
| proc-macro2 1.0.107 | ed8a5497669cd63db33bf24646f261b012bbbc4a | lock only | 985e7ec9bb745e6ce6535b544d84d6cd6f7ad8bd711c398938ae983b91a766d9 | MIT OR Apache-2.0 |
| quote 1.0.47 | 723dcb47d3f0ddc896e17287c8a8d3f2ea2317d5 | lock only | 1fbf4db142a473a8d80c26bbf18454ed458bf8d26c8219c331daecfdbd079001 | MIT OR Apache-2.0 |
| serde 1.0.229 | 7fc3b4c30c94f73a96ebd1553f2b090d928fc3a8 | lock only | 4148590afebada386688f18773da617792bf2ef03ffc1e4cbd2b1d45b023e0ba | MIT OR Apache-2.0 |
| serde_core 1.0.229 | 7fc3b4c30c94f73a96ebd1553f2b090d928fc3a8 | normal/build | 67dca2c9c51e58a4791a4b1ed58308b39c64224d349a935ab5039aa360942a48 | MIT OR Apache-2.0 |
| serde_derive 1.0.229 | 7fc3b4c30c94f73a96ebd1553f2b090d928fc3a8 | lock only | e7a5d71263a5a7d47b41f6b3f06ba276f10cc18b0931f1799f710578e2309348 | MIT OR Apache-2.0 |
| serde_json 1.0.151 | de8500740cdcabffb9734f503e4889def823cf10 | normal/build | c841b55ecdae098c80dcae9cf767f6f8a0c2cdb3416bbef72181df4d0fe73f14 | MIT OR Apache-2.0 |
| syn 3.0.6 | 559cab55ef0c644f77660840edc3620d0d99d3d7 | lock only | 8593e8e72159ed2257d083c7a454a85cbf854f37a0966d8d483aff8c8a3ebcee | MIT OR Apache-2.0 |
| unicode-ident 1.0.26 | 48771a67a73f7e14ce802aaeee16770ba661035b | lock only | d245f478577f809a851594d02313b640fb437e0bb33866753cff937863096954 | (MIT OR Apache-2.0) AND Unicode-3.0 |
| zmij 1.0.23 | 7b7cc48b58028e8af7be87e94c0c1c8936f1a57c | normal/build | 29666d0abbfad1e3dc4dcf6144730dd3a3ab225bbbdac83319345b1b44ccfc1b | MIT |

Download endpoints are `https://static.crates.io/crates/<name>/<name>-<version>.crate`; Cargo verifies the lockfile checksums. All 11 archive sizes total 960357 bytes; this is source-download size, not linked binary size. serde_core and zmij have build scripts: those compiled locally without changing product source. Their upstream maintainers, repository revision and exact archived source are recorded above.

Security review used the official RustSec advisory-db snapshot d5c17953a895cf19e8d3ce66eaa42b6fcfe1fb16 (archive SHA-256 2c60504c44cefec6d32a089d252a5d635cf91f539ebb3b8bda420ddce3a4cc63) on 2026-09-21. Among 1231 advisory files under crates, no exact package-name match appeared for these 11 packages. This limited snapshot lookup is not a guarantee that no vulnerabilities exist. Recheck dependencies and advisories before later upgrades or distribution.

Native and wasm dependency compilation was executed with Rust 1.98.1. Product codec acceptance additionally runs malformed-input, Unicode, duplicate-key, size/depth and compatibility tests; dependency compilation alone does not establish codec correctness.
