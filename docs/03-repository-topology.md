# Planned NewIM repository topology

The product monorepo is located at `/Users/luckyice/Desktop/NewIM工程/NewIM`, with origin `git@github.com:m-ice/NewIM.git`. The control-plane baseline lives at its root after import; the original Agent directory is retained as a source copy. See `docs/16-project-location.md`.

Planned product boundaries (not yet implemented):

```text
newim/
  core/
    domain/
    protocol/
  server/
    gateway/
    auth/
    conversation/
    message/
    sync/
    presence/
    push/
    media/
    webhook/
    moderation/
    platform-api/
  sdk/
    core/
    flutter/
    ios/
    android/
    harmony/
    uniapp/
    web/
    desktop/
  apps/
    reference-flutter/
    admin-console/
  infra/
    db/
    deploy/
    observability/
  tests/
    compatibility/
    integration/
    chaos/
    performance/
```

Language/toolchain choices are ADRs, not Agent assumptions. Shared domain models should not force a platform to adopt an unsuitable runtime.
