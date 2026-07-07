/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import zipkin2.CheckResult;
import zipkin2.Component;
import zipkin2.Endpoint;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

class RedisStorageTest {

  @Test void check_ok() {
    try (RedisStorage storage = RedisStorage.newBuilder().build()) {
      CheckResult result = storage.check();
      assertThat(result.ok()).isTrue();
    }
  }

  @Test void spanStoreNotNull() {
    try (RedisStorage storage = RedisStorage.newBuilder().build()) {
      assertThat(storage.spanStore()).isNotNull();
    }
  }

  @Test void spanConsumerNotNull() {
    try (RedisStorage storage = RedisStorage.newBuilder().build()) {
      assertThat(storage.spanConsumer()).isNotNull();
    }
  }

  @Test void tracesNotNull() {
    try (RedisStorage storage = RedisStorage.newBuilder().build()) {
      assertThat(storage.traces()).isNotNull();
    }
  }

  @Test void serviceAndSpanNamesNotNull() {
    try (RedisStorage storage = RedisStorage.newBuilder().build()) {
      assertThat(storage.serviceAndSpanNames()).isNotNull();
    }
  }

  @Test void autocompleteTagsNotNull() {
    try (RedisStorage storage = RedisStorage.newBuilder().build()) {
      assertThat(storage.autocompleteTags()).isNotNull();
    }
  }

  @Test void builderDefaults() {
    try (RedisStorage storage = RedisStorage.newBuilder().build()) {
      assertThat(storage.check().ok()).isTrue();
    }
  }

  @Test void acceptAndRetrieveSpans() throws IOException {
    try (RedisStorage storage = RedisStorage.newBuilder().build()) {
      Span span = Span.newBuilder()
        .traceId("1")
        .id("1")
        .name("get")
        .localEndpoint(Endpoint.newBuilder().serviceName("frontend").build())
        .timestamp(System.currentTimeMillis() * 1000)
        .duration(100L)
        .build();

      storage.spanConsumer().accept(List.of(span)).execute();

      assertThat(storage.spanStore().getServiceNames().execute())
        .containsExactly("frontend");
      assertThat(storage.spanStore().getSpanNames("frontend").execute())
        .containsExactly("get");
    }
  }

  @Test void returns_autocompleteKeys() throws IOException {
    try (RedisStorage storage = RedisStorage.newBuilder()
      .autocompleteKeys(List.of("http.method"))
      .build()) {
      assertThat(storage.autocompleteTags().getKeys().execute())
        .containsOnlyOnce("http.method");
    }
  }

  @Test void emptyByDefault() throws IOException {
    try (RedisStorage storage = RedisStorage.newBuilder().build()) {
      assertThat(storage.spanStore().getServiceNames().execute()).isEmpty();
    }
  }

  /**
   * The {@code toString()} of {@link Component} implementations appear in health check endpoints.
   * Since these are likely to be exposed in logs and other monitoring tools, care should be taken
   * to ensure {@code toString()} output is a reasonable length and does not contain sensitive
   * information.
   */
  @Test void toStringContainsOnlySummaryInformation() {
    try (RedisStorage storage = RedisStorage.newBuilder().build()) {
      assertThat(storage).hasToString("RedisStorage{}");
    }
  }
}
