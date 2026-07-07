---
name: adding-a-storage-backend
description: How to add a new StorageComponent-based storage backend module to Zipkin. Use when creating or modifying any zipkin-storage/* module.
---

# Adding a storage backend

1. Create a Maven module under `zipkin-storage/<name>` mirroring `zipkin-storage/mysql-v1` (its `pom.xml`, package layout, and Builder pattern). Register the module in the root `pom.xml` <modules> list.
2. Implement a `<Name>Storage` class extending `zipkin2.storage.StorageComponent` with a static `Builder`, exposing `spanStore()`, `spanConsumer()`, and `check()`.
3. Use `zipkin2.storage.InMemoryStorage` as the behavioral reference for correctness; add unit tests alongside the module.
4. Start every new source file with the OpenZipkin license header:
   /*
    * Copyright The OpenZipkin Authors
    * SPDX-License-Identifier: Apache-2.0
    */
5. Verify the module builds: `./mvnw -q -pl zipkin-storage/<name> -am install` (Java 17).
