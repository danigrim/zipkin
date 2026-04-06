/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import zipkin2.Span;
import zipkin2.storage.StorageComponent;

import static zipkin2.storage.ITDependencies.aggregateLinks;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Tag("docker")
class ITMongoDBStorage {

  @RegisterExtension static MongoDBExtension mongo = new MongoDBExtension();

  @Nested
  class ITTraces extends zipkin2.storage.ITTraces<MongoStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.computeStorageBuilder();
    }

    @Override public void clear() {
      storage.clear();
    }
  }

  @Nested
  class ITSpanStore extends zipkin2.storage.ITSpanStore<MongoStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.computeStorageBuilder();
    }

    @Override public void clear() {
      storage.clear();
    }
  }

  @Nested
  class ITSpanStoreHeavy extends zipkin2.storage.ITSpanStoreHeavy<MongoStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.computeStorageBuilder();
    }

    @Override public void clear() {
      storage.clear();
    }
  }

  @Nested
  class ITStrictTraceIdFalse extends zipkin2.storage.ITStrictTraceIdFalse<MongoStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.computeStorageBuilder();
    }

    @Override public void clear() {
      storage.clear();
    }
  }

  @Nested
  class ITSearchEnabledFalse extends zipkin2.storage.ITSearchEnabledFalse<MongoStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.computeStorageBuilder();
    }

    @Override public void clear() {
      storage.clear();
    }
  }

  @Nested
  class ITServiceAndSpanNames extends zipkin2.storage.ITServiceAndSpanNames<MongoStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.computeStorageBuilder();
    }

    @Override public void clear() {
      storage.clear();
    }
  }

  @Nested
  class ITAutocompleteTags extends zipkin2.storage.ITAutocompleteTags<MongoStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.computeStorageBuilder();
    }

    @Override public void clear() {
      storage.clear();
    }
  }

  @Nested
  class ITDependencies extends zipkin2.storage.ITDependencies<MongoStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.computeStorageBuilder();
    }

    @Override public void clear() {
      storage.clear();
    }

    /**
     * The current implementation does not include dependency aggregation. It includes retrieval of
     * pre-aggregated links, usually made via zipkin-dependencies
     */
    @Override protected void processDependencies(List<Span> spans) throws Exception {
      aggregateLinks(spans).forEach(
        (midnight, links) -> {
          for (zipkin2.DependencyLink link : links) {
            org.bson.Document doc = new org.bson.Document()
              .append("day_millis", midnight)
              .append("parent", link.parent())
              .append("child", link.child())
              .append("call_count", link.callCount())
              .append("error_count", link.errorCount());
            storage.dependencies().insertOne(doc);
          }
        });
    }
  }

  @Nested
  class ITDependenciesHeavy extends zipkin2.storage.ITDependenciesHeavy<MongoStorage> {
    @Override protected StorageComponent.Builder newStorageBuilder(TestInfo testInfo) {
      return mongo.computeStorageBuilder();
    }

    @Override public void clear() {
      storage.clear();
    }

    /**
     * The current implementation does not include dependency aggregation. It includes retrieval of
     * pre-aggregated links, usually made via zipkin-dependencies
     */
    @Override protected void processDependencies(List<Span> spans) throws Exception {
      aggregateLinks(spans).forEach(
        (midnight, links) -> {
          for (zipkin2.DependencyLink link : links) {
            org.bson.Document doc = new org.bson.Document()
              .append("day_millis", midnight)
              .append("parent", link.parent())
              .append("child", link.child())
              .append("call_count", link.callCount())
              .append("error_count", link.errorCount());
            storage.dependencies().insertOne(doc);
          }
        });
    }
  }
}
