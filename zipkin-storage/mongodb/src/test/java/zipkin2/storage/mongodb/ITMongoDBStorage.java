/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.Span;
import zipkin2.storage.StorageComponent;

@Testcontainers
@Tag("docker")
class ITMongoDBStorage {

  @Container static MongoDBContainer mongodb = new MongoDBContainer();

  @Nested
  class ITTraces extends zipkin2.storage.ITTraces<MongoDBStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongodb.newStorageBuilder();
    }

    @Override protected void configureStorageForTest(StorageComponent.Builder storage) {
      // default configuration is fine
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongodb.clear(storage);
    }
  }

  @Nested
  class ITSpanStore extends zipkin2.storage.ITSpanStore<MongoDBStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongodb.newStorageBuilder();
    }

    @Override protected void configureStorageForTest(StorageComponent.Builder storage) {
      // default configuration is fine
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongodb.clear(storage);
    }
  }

  @Nested
  class ITSearchEnabledFalse extends zipkin2.storage.ITSearchEnabledFalse<MongoDBStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongodb.newStorageBuilder();
    }

    @Override protected void configureStorageForTest(StorageComponent.Builder storage) {
      storage.searchEnabled(false);
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongodb.clear(storage);
    }
  }

  @Nested
  class ITStrictTraceIdFalse extends zipkin2.storage.ITStrictTraceIdFalse<MongoDBStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongodb.newStorageBuilder();
    }

    @Override protected void configureStorageForTest(StorageComponent.Builder storage) {
      storage.strictTraceId(false);
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongodb.clear(storage);
    }
  }

  @Nested
  class ITServiceAndSpanNames extends zipkin2.storage.ITServiceAndSpanNames<MongoDBStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongodb.newStorageBuilder();
    }

    @Override protected void configureStorageForTest(StorageComponent.Builder storage) {
      // default configuration is fine
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongodb.clear(storage);
    }
  }

  @Nested
  class ITAutocompleteTags extends zipkin2.storage.ITAutocompleteTags<MongoDBStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongodb.newStorageBuilder();
    }

    @Override protected void configureStorageForTest(StorageComponent.Builder storage) {
      // default configuration is fine
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongodb.clear(storage);
    }
  }

  @Nested
  class ITDependencies extends zipkin2.storage.ITDependencies<MongoDBStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongodb.newStorageBuilder();
    }

    @Override protected void configureStorageForTest(StorageComponent.Builder storage) {
      // default configuration is fine
    }

    @Override protected boolean returnsRawSpans() {
      return false;
    }

    @Override public void clear() {
      mongodb.clear(storage);
    }
  }
}
