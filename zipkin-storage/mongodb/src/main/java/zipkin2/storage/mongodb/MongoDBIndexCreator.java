/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;

final class MongoDBIndexCreator {
  static final String SPANS_COLLECTION = "spans";
  static final String DEPENDENCIES_COLLECTION = "dependencies";
  static final String AUTOCOMPLETE_COLLECTION = "autocomplete_tags";

  static void ensureIndexes(MongoDatabase db) {
    ensureSpanIndexes(db.getCollection(SPANS_COLLECTION));
    ensureDependencyIndexes(db.getCollection(DEPENDENCIES_COLLECTION));
    ensureAutocompleteIndexes(db.getCollection(AUTOCOMPLETE_COLLECTION));
  }

  static void ensureSpanIndexes(MongoCollection<Document> spans) {
    spans.createIndex(Indexes.ascending("traceId"));
    spans.createIndex(Indexes.descending("timestamp_millis"));
    spans.createIndex(
      Indexes.compoundIndex(
        Indexes.ascending("localEndpoint.serviceName"),
        Indexes.descending("timestamp_millis")
      )
    );
    spans.createIndex(
      Indexes.compoundIndex(
        Indexes.ascending("localEndpoint.serviceName"),
        Indexes.ascending("name"),
        Indexes.descending("timestamp_millis")
      )
    );
    spans.createIndex(Indexes.ascending("annotations.value"));
    spans.createIndex(Indexes.ascending("tags.key"));
    spans.createIndex(
      Indexes.compoundIndex(
        Indexes.ascending("tags.key"),
        Indexes.ascending("tags.value")
      )
    );
    spans.createIndex(Indexes.ascending("duration"));
    spans.createIndex(Indexes.ascending("remoteEndpoint.serviceName"));
  }

  static void ensureDependencyIndexes(MongoCollection<Document> deps) {
    deps.createIndex(
      Indexes.compoundIndex(Indexes.ascending("day"), Indexes.ascending("parent"),
        Indexes.ascending("child")),
      new IndexOptions().unique(true)
    );
  }

  static void ensureAutocompleteIndexes(MongoCollection<Document> tags) {
    tags.createIndex(
      Indexes.compoundIndex(Indexes.ascending("key"), Indexes.ascending("value")),
      new IndexOptions().unique(true)
    );
    tags.createIndex(Indexes.ascending("key"));
  }

  private MongoDBIndexCreator() {
  }
}
