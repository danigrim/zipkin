/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.storage.StorageComponent.Builder;

@Testcontainers
@Tag("docker")
class ITMongoDBStorage {

  @Container static MongoDBContainer mongo = new MongoDBContainer();

  @Nested
  class ITTraces extends zipkin2.storage.ITTraces<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongo.clear(storage);
    }
  }

  @Nested
  class ITSpanStore extends zipkin2.storage.ITSpanStore<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongo.clear(storage);
    }
  }

  @Nested
  class ITSearchEnabledFalse extends zipkin2.storage.ITSearchEnabledFalse<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongo.clear(storage);
    }
  }

  @Nested
  class ITStrictTraceIdFalse extends zipkin2.storage.ITStrictTraceIdFalse<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongo.clear(storage);
    }
  }

  @Nested
  class ITServiceAndSpanNames extends zipkin2.storage.ITServiceAndSpanNames<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongo.clear(storage);
    }
  }

  @Nested
  class ITAutocompleteTags extends zipkin2.storage.ITAutocompleteTags<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongo.clear(storage);
    }
  }

  @Nested
  class ITDependencies extends zipkin2.storage.ITDependencies<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongo.clear(storage);
    }
  }
}
