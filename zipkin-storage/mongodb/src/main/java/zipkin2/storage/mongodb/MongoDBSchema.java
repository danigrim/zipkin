/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MongoDBSchema {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDBSchema.class);

  private MongoDBSchema() {
  }

  static void ensureIndexes(MongoDatabase db, String collectionName, int ttlSeconds) {
    MongoCollection<Document> spans = db.getCollection(collectionName);

    spans.createIndex(
      Indexes.ascending("timestamp_millis"),
      new IndexOptions()
        .name("ttl_timestamp")
        .expireAfter((long) ttlSeconds, TimeUnit.SECONDS)
        .sparse(true)
    );
    LOG.debug("Ensured TTL index on {}.{}.timestamp_millis (expiry={}s)",
      db.getName(), collectionName, ttlSeconds);

    spans.createIndex(
      Indexes.ascending("traceId", "id"),
      new IndexOptions().name("trace_id_lookup")
    );
    LOG.debug("Ensured trace ID lookup index on {}.{}", db.getName(), collectionName);

    spans.createIndex(
      Indexes.ascending("localEndpoint.serviceName", "name", "timestamp_millis"),
      new IndexOptions().name("service_span_ts").sparse(true)
    );
    LOG.debug("Ensured service/span/ts search index on {}.{}", db.getName(), collectionName);

    spans.createIndex(
      Indexes.ascending(
        "localEndpoint.serviceName",
        "remoteEndpoint.serviceName",
        "timestamp_millis"),
      new IndexOptions().name("remote_service_ts").sparse(true)
    );
    LOG.debug("Ensured remote-service search index on {}.{}", db.getName(), collectionName);

    spans.createIndex(
      Indexes.ascending("tags.key", "tags.value"),
      new IndexOptions().name("tags_kv").sparse(true)
    );
    LOG.debug("Ensured tags key/value index on {}.{}", db.getName(), collectionName);

    spans.createIndex(
      Indexes.ascending("annotations.value"),
      new IndexOptions().name("annotations_value").sparse(true)
    );
    LOG.debug("Ensured annotations value index on {}.{}", db.getName(), collectionName);
  }
}
