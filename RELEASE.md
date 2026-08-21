# Release procedure

SynexIM xray-core releases use the upstream-compatible core version in the
binary and a fork-specific Git tag. For example, a build based on Xray
`26.7.28` may use the tag `v26.7.28-synexim.1`.

Every published release must include:

- the exact source commit and tag;
- platform archives and their `.dgst` and SHA256 files;
- the matching multi-architecture GHCR image;
- a changelog entry describing fork-specific behavior;
- a compatibility note for the 3x-ui release that consumes it.

Release xray-core before releasing 3x-ui. The panel release workflow must pin
the exact xray-core release tag instead of downloading an upstream binary.

Never move an existing stable tag or overwrite a published release asset. To
roll back, select the previous immutable release and its matching image tag.
