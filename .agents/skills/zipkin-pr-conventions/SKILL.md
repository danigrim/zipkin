---
name: zipkin-pr-conventions
description: Conventions for making changes and opening PRs in the Zipkin repo. Use whenever editing code or preparing a PR.
---

# Zipkin PR conventions

- Target the `master` branch for PRs.
- Keep each change scoped to a single module where possible.
- Every new source file must begin with the OpenZipkin license header:
  /*
   * Copyright The OpenZipkin Authors
   * SPDX-License-Identifier: Apache-2.0
   */
- Build with the Maven wrapper on Java 17. Run `./mvnw verify` (or the scoped `./mvnw -q -pl <module> -am verify`) before pushing.
- Do not hand-edit generated code; prefer minimal, focused diffs.
