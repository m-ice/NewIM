# S3 media dependency provenance

The gateway uses AWS SDK for Go v2 for S3 request signing, credential loading,
and conditional object calls. The adapter uses the S3 client directly. The
reviewed Go modules require Go 1.24 or newer. The product builds with its
pinned Go toolchain. [S3 media ADR](../adr/0035-s3-media-store.md) ·
[AWS SDK source](https://github.com/aws/aws-sdk-go-v2/tree/fab6ce7049daa93f9ace2bb14039f41c8a00504a) ·
[Smithy Go source](https://github.com/aws/smithy-go/tree/fea1abf749f7f7bcd06186e0197a0c7389fa9f91)

## Go modules

The reviewed import graph adds the following 18 modules. Their module ZIPs
total 3,244,261 bytes. The Go checksums are recorded in [go.sum](../../go.sum).

| Module | Version | Go checksum | Archive SHA-256 |
| --- | --- | --- | --- |
| `github.com/aws/aws-sdk-go-v2` | `v1.48.1` | `h1:j6ro6IvbxUe8BCulc/6uWMrhmx6+i5HNIc1GZEJRT9k=` | `f0f565a73afdd449be8089cbd5dba7c62efb09265c02674f9fb536cdfe7c0eca` |
| `github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream` | `v1.7.28` | `h1:/LppjvqoI0KYB6gDrG/b77okSNPrVJI7CJtK5ITYoZ0=` | `e892222b84c2c918633da3954346132e0033470a941242e520f46c65b690f095` |
| `github.com/aws/aws-sdk-go-v2/config` | `v1.33.22` | `h1:U4aevm9PINYwCsHE6WFmL2aCxtMM1XaB05v2nRAJzoI=` | `f4b81f4ae69b0361db0d234edd334a496000bc1b4df8620bf9cabf2990d3b417` |
| `github.com/aws/aws-sdk-go-v2/credentials` | `v1.21.4` | `h1:FWXJfHDIHV4r5GfgOqvkr+4EddYMT9ZRzIemxwoiPzg=` | `e2920a1c40b356499ac2deb656a3f72c229d255f2079500a5738aae8eebe3c5f` |
| `github.com/aws/aws-sdk-go-v2/feature/ec2/imds` | `v1.20.10` | `h1:EwVFH5FBnAzlLhJUtQkKMPDd/8D3duH3jka/Hy6wEdE=` | `c27c19503de1cd7dd3099af29e83c5b817f3ce1b25d88abf6b428eb4b1608ac5` |
| `github.com/aws/aws-sdk-go-v2/internal/configsources` | `v1.5.13` | `h1:ZFqKPVQYSIYOXVoG/YRGXYNPqcGslzu/N2J3kev/Djc=` | `d9f119cc6681e791c420349385896706c98ce2bcd01f5e17e7a7670596263d3f` |
| `github.com/aws/aws-sdk-go-v2/internal/endpoints/v2` | `v2.8.13` | `h1:0pHWAgLjLZWSc9GtdpMVFT7bvpUcm9xZYcaJ9bmyLGo=` | `09636040ac27654c1f0844509f30fb5ffe44d486c824f36a083cc5cc1f8b1edf` |
| `github.com/aws/aws-sdk-go-v2/internal/v4a` | `v1.5.13` | `h1:a6vUTTG7Vsf0LTCuy/GPJsLhz3L6YcS1WkwR5Nxyz8g=` | `162997cb2d41b3b1d5b288e4ef04744a28d729b4fca8475ee29902407ad81570` |
| `github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding` | `v1.13.26` | `h1:PgkTkbD4VhN7KX1fQXiNMsSZqPnEfXmi+UuISMDEuy8=` | `6acb13c78d68db01bde8cbc2b3f942509946acea86e6941252c84c60ef3c2e0c` |
| `github.com/aws/aws-sdk-go-v2/service/internal/checksum` | `v1.11.14` | `h1:xZolVZk8AKg8m9Axl7IZreaUEOrFmomgaXjWzpiAiWM=` | `81bd9ea46f875c4d716b8829d45a11f622db6291dc56e4efe1a02eecb9881b92` |
| `github.com/aws/aws-sdk-go-v2/service/internal/presigned-url` | `v1.14.13` | `h1:D67JeihKdQFL5Zks8rFmf0V0sOZO+iAOjQUW03O3R2E=` | `591602f0f82e534ae4b96299348b8fa2b3534381d4f1a288f3d43a26c928c046` |
| `github.com/aws/aws-sdk-go-v2/service/internal/s3shared` | `v1.20.13` | `h1:MJaFheEZeJDd2qGObzRjsCdDTvRzApb6EqwZ5yBHzwE=` | `07b3506070d4acf35628a97b591b39bfa1b9d921ba8bb626a0b24dbdc75ed548` |
| `github.com/aws/aws-sdk-go-v2/service/s3` | `v1.116.1` | `h1:qFQ1JjNGG8fdKBFNdQ641PXCaPdDulXlIpk3kntgqb0=` | `e5e5e5000e5199a9b283c3e7c47c5081e19253fb61be34a384b3bce79e5dad52` |
| `github.com/aws/aws-sdk-go-v2/service/signin` | `v1.14.1` | `h1:fHgVyc6g4vSYuPplgTrvCWaQQCb63y8tzUrM/6aAVzI=` | `b33fbade7b6545d14c704629a1705d7dbed86555faf8aed02541e6290567988a` |
| `github.com/aws/aws-sdk-go-v2/service/sso` | `v1.39.6` | `h1:9MWBzvq9u5G2USGms+OXYD9sERXyK1zqP6LBrsysvXU=` | `c8e798ec21abaf1a89dfe93135ed12dc28b4b148b5b4cf185b82db6235bd133b` |
| `github.com/aws/aws-sdk-go-v2/service/ssooidc` | `v1.44.6` | `h1:BNTt0xfMtfLF3/YpcPCPioL2EvDzSnECxIAYuaBAKF0=` | `3a2b9c876b93fc80ee4c8e7fff16d4a21dbd44174ee76d2d06b3506c3f850977` |
| `github.com/aws/aws-sdk-go-v2/service/sts` | `v1.53.1` | `h1:v6vt91IoL7PWx0TjIETO5IWtjAVQqOZ3oKUus6XykLY=` | `69fc7e0658a919d6eae1d7f37696e23692416b9d27a8009c19e39d60147b842c` |
| `github.com/aws/smithy-go` | `v1.28.10` | `h1:SfPnj8fGCVAI/P5YC4/1JjuMsucbPp2dROnSTYNo2PE=` | `f9976b043f0552dd77c8d0cb7e967200143c8464824ef1343989d2938e7d8b0a` |

S3 and EventStream are above the fixed versions in GO-2026-5764. S3, SSO,
SSO OIDC, and STS are above the fixed versions in GHSA-3jcv-796g-cpjg.
The module-level preparation scan reported findings in the existing Go 1.27.1
standard library. Final review verifies the product commit's dependency graph
and toolchain. [Go vulnerability report](https://pkg.go.dev/vuln/GO-2026-5764) ·
[AWS advisory](https://github.com/aws/aws-sdk-go-v2/security/advisories/GHSA-3jcv-796g-cpjg)

## License and notice archive

The SDK and Smithy Go use Apache-2.0. The table records the exact text from
each module archive. Where both modules include the same Go singleflight
license, one copy covers both. Distributions containing these modules retain
the applicable texts and notices. [Third-party notices](../../THIRD_PARTY_NOTICES.md)

| Coverage | Text | SHA-256 |
| --- | --- | --- |
| AWS SDK module family, `LICENSE.txt` | [AWS SDK license](../../third_party/licenses/media-s3/AWS-SDK-LICENSE.txt) | `cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30` |
| AWS SDK root module, `NOTICE.txt` | [AWS SDK notice](../../third_party/licenses/media-s3/AWS-SDK-NOTICE.txt) | `a6e830174d62dafad3a718384772ea1f4c0e27989f050932dc442a5e3ddad080` |
| Smithy Go, `LICENSE` | [Smithy Go license](../../third_party/licenses/media-s3/SMITHY-GO-LICENSE.txt) | `09e8a9bcec8067104652c168685ab0931e7868f9c8284b66f5ae6edae5f1130b` |
| Smithy Go, `NOTICE` | [Smithy Go notice](../../third_party/licenses/media-s3/SMITHY-GO-NOTICE.txt) | `d4290ed64c2edd0fce1d84e3f9dfb2881240fe534def76b8cd29ed6af683e287` |
| AWS SDK root and Smithy Go, `internal/sync/singleflight/LICENSE` | [Go singleflight BSD-3-Clause](../../third_party/licenses/media-s3/GO-SINGLEFLIGHT-LICENSE.txt) | `f5122de47ce62d57dcd3e57cdf778ceacc3a847645de3332fea58903def93ecb` |
| Smithy Go, `transport/http/protocol/internal/json/internal/stdlib/LICENSE` | [Go JSON standard-library BSD-3-Clause](../../third_party/licenses/media-s3/GO-JSON-STDLIB-LICENSE.txt) | `2d36597f7117c38b006835ae7f537487207d8ec407aa9d9980794b2030cbc067` |

## S3 test service

The integration gates run the SeaweedFS 4.55 image as an isolated external
service with a private bucket, generated test credentials, and verified TLS.
The registry index for `docker.io/chrislusf/seaweedfs:4.55` is pinned at
`sha256:7ca46aeae2f223b37e5d91593c17ab37738eec6df6ab69b48a0487f7643342b9`.
The registry's platform manifests and image config digests were verified from
the response bytes. Both image configs identify source revision
`a98c9e27185cf1003eca095761a39973c60734db`. [Registry metadata](https://registry.hub.docker.com/v2/repositories/chrislusf/seaweedfs/tags/4.55) ·
[Source release](https://github.com/seaweedfs/seaweedfs/releases/tag/4.55)

| Platform | Manifest digest | Config digest |
| --- | --- | --- |
| `linux/amd64` | `sha256:340bc0cbb1eb0da6472bd45a7225dd9a48bae6d72771c98ead52200b5abd4352` | `sha256:2645139b94f93a221f28d7d29704a365c2e836b5f06fc910bbe17752be21576b` |
| `linux/arm64` | `sha256:1d857dc0ff7f0829e5f0228c7dd8d6907b4d1e7b01b553eb5c05c17c9a9a7b49` | `sha256:92e8a2755d9ce667b8bd0f65d62a048901eb74f48bf650d7b15ac41ce2028f7d` |

SeaweedFS source uses Apache-2.0. Image components carry their respective
licenses. The test runner fetches the publisher's image separately from the
product build, which uses the Go client modules above. [SeaweedFS source license](https://github.com/seaweedfs/seaweedfs/blob/a98c9e27185cf1003eca095761a39973c60734db/LICENSE)
