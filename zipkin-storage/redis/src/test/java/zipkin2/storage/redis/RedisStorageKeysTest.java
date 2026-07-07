/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RedisStorageKeysTest {
  @Test void lowTraceId_truncates128Bit() {
    assertThat(RedisStorageKeys.lowTraceId("463ac35c9f6413ad48485a3953bb6124"))
      .isEqualTo("48485a3953bb6124");
  }

  @Test void lowTraceId_passesThrough64Bit() {
    assertThat(RedisStorageKeys.lowTraceId("48485a3953bb6124"))
      .isEqualTo("48485a3953bb6124");
  }

  @Test void timestampMember_roundTrips() {
    String member = RedisStorageKeys.timestampMember("48485a3953bb6124", 1234567890L);

    assertThat(member).isEqualTo("48485a3953bb6124:1234567890");
    assertThat(RedisStorageKeys.lowTraceIdFromMember(member)).isEqualTo("48485a3953bb6124");
  }

  @Test void keysArePrefixed() {
    assertThat(RedisStorageKeys.trace("abc")).isEqualTo("zipkin:trace:abc");
    assertThat(RedisStorageKeys.serviceTraces("foo")).isEqualTo("zipkin:service:foo:traces");
    assertThat(RedisStorageKeys.spanNames("foo")).isEqualTo("zipkin:service:foo:spans");
    assertThat(RedisStorageKeys.remoteServiceNames("foo")).isEqualTo("zipkin:service:foo:remote");
    assertThat(RedisStorageKeys.autocompleteValues("http.host"))
      .isEqualTo("zipkin:autocomplete:http.host");
  }
}
