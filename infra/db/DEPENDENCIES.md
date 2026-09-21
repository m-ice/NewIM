# Database tooling provenance

Reviewed input date: 2026-09-21. Independent security/license approval belongs to
the task acceptance record, not this implementation document.

The sole added executable dependency is the externally pulled official PostgreSQL
18.6 bookworm container, locked in `image-lock.json`. SQL/Python use PostgreSQL and
Python standard facilities; no driver, ORM, extension or Python package is added.
Root Go/Rust dependencies remain unchanged. Python only orchestrates tests and
renders locally checked migration bytes; it does not implement a service adapter.

Sources:

- Official tag/source map: https://github.com/docker-library/official-images/blob/master/library/postgres
- Pinned packaging: https://github.com/docker-library/postgres/tree/e00e1bd34ec5c8a8e7ad89b273b3d42efaf6d5bc/18/bookworm
- PostgreSQL release: https://www.postgresql.org/docs/release/18.6/
- Maintenance policy: https://www.postgresql.org/support/versioning/ (18 supported through 2030-11-14)
- PostgreSQL license: https://www.postgresql.org/about/licence/
- Packaging license: https://github.com/docker-library/postgres/blob/e00e1bd34ec5c8a8e7ad89b273b3d42efaf6d5bc/LICENSE
- Debian licensing: https://www.debian.org/legal/licenses/

Saved upstream index, arm64/amd64 manifests and config bytes are keyed by SHA-256
under `image-metadata/`. Runtime verifies their original bytes, platform membership,
config digest, layer descriptors and ordered diff_ids against the lock before
accepting a locally inspected image. Docker images are then run by immutable ID
with pull disabled. Merely naming an image with the expected tag cannot pass.
The local arm64 Docker 29 containerd store returns manifest as image ID; the
original config is verified through the hashed manifest/config chain, not claimed
to be a separately returned Docker inspect field.

Compressed layers total 155,248,016 bytes for arm64 and 157,251,669 for amd64.
The arm64 imported image reports 155,261,527 bytes; expanded ordered layer tar
bytes total 469,276,160. These are different measures, not VM disk-use promises.
Version is checked through actual SQL `server_version_num=180006` on every test
instance. The package version is 18.6-1.pgdg12+2. No alternate or latest image is
silently substituted; updates require an explicit reviewed lock change.

`licenses/packages-linux-arm64.tsv` records actual dpkg name/version/architecture
and source package identity from the locked image. `image-linux-arm64-files.json`
records original image paths, bytes and SHA-256 for collected package copyright
files and common license texts; the files are included as review material. These
are not product source implementations. The external image includes Debian
components under multiple licenses (including GPL/LGPL), not just PostgreSQL and
MIT. There is no blanket claim that the entire image is permissively licensed.

This task delivers NewIM SQL/tooling source and immutable pull references, not a
redistributed container, PostgreSQL binary or OS distribution. Image distribution
would require a separate complete package/component notices and corresponding
source obligation review. Source URLs and copyright snapshots are not by
themselves a GPL corresponding-source offer. NewIM's own product license remains
an unresolved product decision; this document grants no publication rights.

Security boundaries: containers have no network/host ports or user bind mounts,
bounded memory/CPU/PIDs and private disposable volumes. Image entrypoint runs as
root for owned-volume initialization then PostgreSQL runs as its unprivileged
user. Production deployments must independently harden role/secret/network/backup
configuration. Fixed version and digest prove identity, not absence of known or
future CVEs; source maintenance and OS package risks need independent review.
