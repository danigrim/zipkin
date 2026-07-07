/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RedisStorageTest {
  @Test void defaults() {
    RedisStorage storage = RedisStorage.newBuilder().build();

    assertThat(storage.strictTraceId).isTrue();
    assertThat(storage.searchEnabled).isTrue();
    assertThat(storage.host).isEqualTo("localhost");
    assertThat(storage.port).isEqualTo(6379);
    assertThat(storage.ttlSeconds).isEqualTo(TimeUnit.DAYS.toSeconds(7));
    assertThat(storage.closePool).isTrue();
  }

  @Test void appliesBuilderSettings() {
    RedisStorage storage = RedisStorage.newBuilder()
      .strictTraceId(false)
      .searchEnabled(false)
      .host("redis.example.com")
      .port(1234)
      .dataTtl(60)
      .autocompleteKeys(List.of("http.host"))
      .build();

    assertThat(storage.strictTraceId).isFalse();
    assertThat(storage.searchEnabled).isFalse();
    assertThat(storage.host).isEqualTo("redis.example.com");
    assertThat(storage.port).isEqualTo(1234);
    assertThat(storage.ttlSeconds).isEqualTo(60);
    assertThat(storage.autocompleteKeysSet).containsExactly("http.host");
  }

  @Test void autocompleteKeys_null_throws() {
    assertThatThrownBy(() -> RedisStorage.newBuilder().autocompleteKeys(null))
      .isInstanceOf(NullPointerException.class);
  }

  @Test void host_null_throws() {
    assertThatThrownBy(() -> RedisStorage.newBuilder().host(null))
      .isInstanceOf(NullPointerException.class);
  }

  @Test void dataTtl_negative_throws() {
    assertThatThrownBy(() -> RedisStorage.newBuilder().dataTtl(-1))
      .isInstanceOf(IllegalArgumentException.class);
  }

  @Test void jedisPool_null_throws() {
    assertThatThrownBy(() -> RedisStorage.newBuilder().jedisPool(null))
      .isInstanceOf(NullPointerException.class);
  }

  @Test void toStringContainsHostAndPort() {
    assertThat(RedisStorage.newBuilder().host("h").port(1).build())
      .hasToString("RedisStorage{host=h, port=1}");
  }
}
