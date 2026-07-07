/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import zipkin2.CheckResult;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.InMemoryStorage;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;

public final class RedisStorage extends StorageComponent {
  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {
    boolean strictTraceId = true, searchEnabled = true;
    List<String> autocompleteKeys = new ArrayList<>();

    @Override public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    @Override public Builder searchEnabled(boolean searchEnabled) {
      this.searchEnabled = searchEnabled;
      return this;
    }

    @Override public Builder autocompleteKeys(List<String> keys) {
      if (keys == null) throw new NullPointerException("keys == null");
      this.autocompleteKeys = keys;
      return this;
    }

    @Override public RedisStorage build() {
      return new RedisStorage(this);
    }

    Builder() {
    }
  }

  final InMemoryStorage delegate;

  RedisStorage(Builder builder) {
    delegate = InMemoryStorage.newBuilder()
      .strictTraceId(builder.strictTraceId)
      .searchEnabled(builder.searchEnabled)
      .autocompleteKeys(builder.autocompleteKeys)
      .build();
  }

  @Override public SpanStore spanStore() {
    return delegate.spanStore();
  }

  @Override public Traces traces() {
    return delegate.traces();
  }

  @Override public ServiceAndSpanNames serviceAndSpanNames() {
    return delegate.serviceAndSpanNames();
  }

  @Override public AutocompleteTags autocompleteTags() {
    return delegate.autocompleteTags();
  }

  @Override public SpanConsumer spanConsumer() {
    return delegate.spanConsumer();
  }

  @Override public CheckResult check() {
    return CheckResult.OK;
  }

  @Override public final String toString() {
    return "RedisStorage{}";
  }

  @Override public void close() {
    delegate.close();
  }
}
