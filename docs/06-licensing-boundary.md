# Licensing and product authorization boundary

## Open-source core

Recommended default for NewIM Core: Apache-2.0, subject to 澜遇科技 legal review. Core capabilities must remain usable for commercial deployment without contacting a licensing server.

## Premium / upgraded features

Features designated by product as upgraded may use a separate commercial license and a `LicenseProvider` extension interface. Rules:

- OSS core defines only stable extension points and capability discovery.
- Premium packages are not imported by core.
- Missing/expired premium authorization disables only the premium capability; core messaging remains operational.
- Authorization checks are explicit and testable, never hidden in unrelated code paths.
- No telemetry or remote kill switch is introduced implicitly.
- The exact premium feature list is a product/legal decision and must be documented before implementation.

## Third-party use

Record source repository, exact version/commit, license, files used, modifications and NOTICE obligations. Do not copy AGPL client code into a differently licensed NewIM client.
