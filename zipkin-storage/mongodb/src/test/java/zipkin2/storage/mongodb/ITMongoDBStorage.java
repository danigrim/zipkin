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
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.storage.StorageComponent.Builder;

import static zipkin2.storage.ITDependencies.aggregateLinks;

@Testcontainers
@Tag("docker")
class ITMongoDBStorage {

  @Container static MongoDBContainer mongo = new MongoDBContainer();

  @Nested
  class ITTraces extends zipkin2.storage.ITTraces<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
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

    @Override public void clear() {
      mongo.clear(storage);
    }
  }

  @Nested
  class ITSpanStoreHeavy extends zipkin2.storage.ITSpanStoreHeavy<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
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

    @Override public void clear() {
      mongo.clear(storage);
    }
  }

  @Nested
  class ITStrictTraceIdFalse extends zipkin2.storage.ITStrictTraceIdFalse<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
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

    @Override public void clear() {
      mongo.clear(storage);
    }
  }

  @Nested
  class ITAutocompleteTags extends zipkin2.storage.ITAutocompleteTags<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
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

    @Override public void clear() {
      mongo.clear(storage);
    }

    @Override protected void processDependencies(List<Span> spans) {
      aggregateLinks(spans).forEach(
        (midnight, links) -> {
          for (DependencyLink link : links) {
            org.bson.Document doc = new org.bson.Document()
              .append("day", midnight)
              .append("parent", link.parent())
              .append("child", link.child())
              .append("callCount", link.callCount())
              .append("errorCount", link.errorCount());
            storage.dependencies().insertOne(doc);
          }
        });
    }
  }

  @Nested
  class ITDependenciesHeavy extends zipkin2.storage.ITDependenciesHeavy<MongoDBStorage> {
    @Override protected Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.newStorageBuilder();
    }

    @Override public void clear() {
      mongo.clear(storage);
    }

    @Override protected void processDependencies(List<Span> spans) {
      aggregateLinks(spans).forEach(
        (midnight, links) -> {
          for (DependencyLink link : links) {
            org.bson.Document doc = new org.bson.Document()
              .append("day", midnight)
              .append("parent", link.parent())
              .append("child", link.child())
              .append("callCount", link.callCount())
              .append("errorCount", link.errorCount());
            storage.dependencies().insertOne(doc);
          }
        });
    }
  }
}
