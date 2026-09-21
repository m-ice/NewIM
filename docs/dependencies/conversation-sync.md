# Conversation sync dependency provenance

Task: NIM-SYN-003

The internal conversation sync adapter uses Go 1.27.1 with `github.com/jackc/pgx/v5`
v5.11.0. The actual imported closure selected for the product candidate is:

| Module | Version | Use | Zip SHA-256 | Go `h1` |
| --- | --- | --- | --- | --- |
| `github.com/jackc/pgx/v5` | `v5.11.0` | PostgreSQL driver and pool | `ada62e95244336484f86c274e04a6845f0c494c93b69d24f2ac4181b7c58b11d` | `IzBBtyK9AHqf98cctWFifYSci2hgQR/cd56wB4p+ogg=` |
| `github.com/jackc/pgpassfile` | `v1.0.0` | pgx password-file support | `1cc79fb0b80f54b568afd3f4648dd1c349f746ad7c379df8d7f9e0eb1cac938b` | `/6Hmqy13Ss2zCq62VdNG8tM1wchn8zjSGOBJ6icpsIM=` |
| `github.com/jackc/pgservicefile` | `v0.0.0-20240606120523-5a60cdf6a761` | pgx service-file support | `c9e31c91aebf96eb246bd410d1849cc7666d955a1e20ca2eba1c30b4eb89335f` | `iCEnooe7UlwOQYpKFhBabPMi4aNAfoODPEFNiAnClxo=` |
| `github.com/jackc/puddle/v2` | `v2.2.2` | bounded pgx pool | `f1f0789098a0bcb5ff3c7024f9ee387a6748446e1bc6713b13a63d763a9fb11e` | `PR8nw+E/1w0GLuRFSmiioY6UooMp6KJv0/61nB7icHo=` |
| `golang.org/x/sync` | `v0.21.0` | pgx transitive synchronization | `ee65459023de7f24836f6e2123144b5329bd0a4d05a87c3c448509378e2e6be7` | `HLII4xRRTtCRkxYp4HNFF0Js/Og6q2i++KXbg0gHCwM=` |
| `golang.org/x/text` | `v0.39.0` | pgx SCRAM/precis text normalization | `cbfa33111dfa6cbafef63103b82c544d35df425824ac94ea19629a12bdbf0523` | `UbZz4pLOvn600D6Oh6GGEI6VAmndrEBLv8/6BEXzyus=` |

The complete selected module graph also contains `x/mod v0.37.0`,
`x/tools v0.47.0`, `go-spew v1.1.1`, `kr/pretty v0.3.0`, `go-difflib v1.0.0`,
`objx v0.1.0`, `testify v1.11.1`, `check.v1 v1.0.0-20201130134442-10cb98267c6c`
and `yaml.v3 v3.0.1`. They are graph-only for this imported closure and are not
described as runtime linkage. Any import, version or toolchain change requires a
new security and license review before delivery.

## Admission evidence

Preparation material and independent role reviews are kept in the local
control-plane workspace, never copied into this product repository. The product candidate is admitted
only when the security and license reviewers have independently approved the
exact module graph and license/notice set. Their final reports bind the product
SHA and actual build/import graph.

The preparation records are:

- `independent-security.md`: `95d189e2f82753e0ac309b9de2519630b753a6cf7876be420d563928ad1822e5`
- `independent-license.md`: `0a07155a4b8cfe9d79c9c283613b9c5510ab4d1086261aec46006e5b78e772be`
- patched `go.mod`: `db4a4ab2024be44120cbb023ee7b9e3e74d54158dfbab749cd49ce46c11bc8fa`
- patched `go.sum`: `6974acf7327b212d91d510bd966b11c217637a45beb52820dc2d304e475efcd4`

These preparation hashes are provenance anchors, not substitutes for the final
product-SHA review. Independent security and license reviewers accepted this
exact dependency candidate before product implementation; that acceptance is not
a claim about the final product commit. The product runs `go mod verify`, a
complete module/import inventory, license/notice checks and pinned security
scanning on the final SHA as part of delivery acceptance.

## Security conditions

The previously selected unmodified graph used `golang.org/x/text v0.29.0`.
Official GO-2026-5970 identifies an invalid-UTF-8 normalization loop fixed in
v0.39.0. The product pins v0.39.0 and must not accept the older resolution.
Static reachability is not a claim that the issue was exploited; it is sufficient
reason to avoid the vulnerable version. Scanner exit success alone is not a
passing result because JSON output can contain findings.

The adapter enforces verified TCP TLS or an explicitly enabled local Unix socket,
a bounded pool, connection/acquire deadlines, a 1 MiB driver protocol-message
limit, parameterized SQL, redacted stable errors and no secret/DSN logging.

## License and notice obligations

The repository retains the exact license/notice text for the six imported
modules under `third_party/licenses/pgx/`. The jackc modules use their original
MIT copyright texts; Go modules retain BSD-3-Clause and PATENTS; Unicode License
V3 and the original CLDR 32 data notice apply to generated x/text tables.
Attribution does not imply ICU library linkage or Unicode endorsement.

If graph-only or future vendored sources are redistributed, the additional
mixed yaml MIT/Apache/NOTICE and inline yoyacc/check notices must be retained.
The root project still makes no software distribution license grant.
