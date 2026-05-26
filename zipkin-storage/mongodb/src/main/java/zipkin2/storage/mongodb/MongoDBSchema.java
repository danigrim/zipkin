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

/**
 * Ensures that the required MongoDB indexes exist on the span collection.
 *
 * <h2>Indexes created</h2>
 * <ol>
 *   <li><b>TTL index</b> on {@code timestamp_millis} – removes documents older than
 *       {@code ttlSeconds}.</li>
 *   <li><b>traceId lookup</b> on {@code (traceId, id)} – supports
 *       {@link MongoDBSpanStore#getTrace(String)} and
 *       {@link MongoDBSpanStore#getTraces(Iterable)}.</li>
 *   <li><b>Service + span search</b> on
 *       {@code (localEndpoint.serviceName, name, timestamp_millis)} – supports
 *       {@link MongoDBSpanStore#getTraces(zipkin2.storage.QueryRequest)} when search is
 *       enabled.</li>
 *   <li><b>Remote service search</b> on
 *       {@code (localEndpoint.serviceName, remoteEndpoint.serviceName, timestamp_millis)} –
 *       supports {@code remoteServiceName} queries.</li>
 * </ol>
 *
 * <p>All index creations use {@code background: true} (implicit in MongoDB 4.2+) and
 * {@code sparse: true} where the field may be absent to minimise index size.
 */
final class MongoDBSchema {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDBSchema.class);

  private MongoDBSchema() {
  }

  /**
   * Creates indexes on {@code spanCollection} inside {@code db} if they do not already exist.
   *
   * @param db           the target database
   * @param collectionName name of the span collection (usually {@code "span"})
   * @param ttlSeconds   document TTL; spans older than this are automatically deleted
   */
  static void ensureIndexes(MongoDatabase db, String collectionName, int ttlSeconds) {
    MongoCollection<Document> spans = db.getCollection(collectionName);

    // 1. TTL index – drives automatic data expiry
    spans.createIndex(
      Indexes.ascending("timestamp_millis"),
      new IndexOptions()
        .name("ttl_timestamp")
        .expireAfter((long) ttlSeconds, TimeUnit.SECONDS)
        .sparse(true)
    );
    LOG.debug("Ensured TTL index on {}.{}.timestamp_millis (expiry={}s)",
      db.getName(), collectionName, ttlSeconds);

    // 2. Trace ID lookup (primary read path)
    spans.createIndex(
      Indexes.ascending("traceId", "id"),
      new IndexOptions().name("trace_id_lookup")
    );
    LOG.debug("Ensured trace ID lookup index on {}.{}", db.getName(), collectionName);

    // 3. Service + span name + timestamp (search path)
    spans.createIndex(
      Indexes.ascending("localEndpoint.serviceName", "name", "timestamp_millis"),
      new IndexOptions().name("service_span_ts").sparse(true)
    );
    LOG.debug("Ensured service/span/ts search index on {}.{}", db.getName(), collectionName);

    // 4. Remote service name + timestamp (remoteServiceName query path)
    spans.createIndex(
      Indexes.ascending(
        "localEndpoint.serviceName",
        "remoteEndpoint.serviceName",
        "timestamp_millis"),
      new IndexOptions().name("remote_service_ts").sparse(true)
    );
    LOG.debug("Ensured remote-service search index on {}.{}", db.getName(), collectionName);
  }
}
