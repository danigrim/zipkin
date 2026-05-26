/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoCollection;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import zipkin2.Annotation;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;

class MongoDBSpanConsumer implements SpanConsumer {

  final MongoDBStorage storage;

  MongoDBSpanConsumer(MongoDBStorage storage) {
    this.storage = storage;
  }

  @Override public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    return new InsertSpansCall(storage, spans);
  }

  static Document toDocument(Span span) {
    Document doc = new Document();
    doc.put("traceId", span.traceId());
    doc.put("id", span.id());
    if (span.parentId() != null) doc.put("parentId", span.parentId());
    if (span.kind() != null) doc.put("kind", span.kind().name());
    if (span.name() != null) doc.put("name", span.name());
    if (span.timestampAsLong() > 0) {
      doc.put("timestamp", span.timestampAsLong());
      doc.put("timestamp_millis", Instant.ofEpochMilli(span.timestampAsLong() / 1000));
    }
    if (span.durationAsLong() > 0) doc.put("duration", span.durationAsLong());
    if (span.localEndpoint() != null) {
      doc.put("localEndpoint", endpointToDocument(span.localEndpoint()));
    }
    if (span.remoteEndpoint() != null) {
      doc.put("remoteEndpoint", endpointToDocument(span.remoteEndpoint()));
    }
    if (!span.annotations().isEmpty()) {
      List<Document> annotations = new ArrayList<>(span.annotations().size());
      for (Annotation a : span.annotations()) {
        Document aDoc = new Document();
        aDoc.put("timestamp", a.timestamp());
        aDoc.put("value", a.value());
        annotations.add(aDoc);
      }
      doc.put("annotations", annotations);
    }
    if (!span.tags().isEmpty()) {
      List<Document> tagDocs = new ArrayList<>(span.tags().size());
      for (Map.Entry<String, String> entry : span.tags().entrySet()) {
        tagDocs.add(new Document("key", entry.getKey()).append("value", entry.getValue()));
      }
      doc.put("tags", tagDocs);
    }
    if (Boolean.TRUE.equals(span.debug())) doc.put("debug", true);
    if (Boolean.TRUE.equals(span.shared())) doc.put("shared", true);
    return doc;
  }

  static Document endpointToDocument(Endpoint endpoint) {
    Document doc = new Document();
    if (endpoint.serviceName() != null) doc.put("serviceName", endpoint.serviceName());
    if (endpoint.ipv4() != null) doc.put("ipv4", endpoint.ipv4());
    if (endpoint.ipv6() != null) doc.put("ipv6", endpoint.ipv6());
    if (endpoint.portAsInt() != 0) doc.put("port", endpoint.portAsInt());
    return doc;
  }

  static final class InsertSpansCall extends Call.Base<Void> {
    final MongoDBStorage storage;
    final List<Span> spans;

    InsertSpansCall(MongoDBStorage storage, List<Span> spans) {
      this.storage = storage;
      this.spans = spans;
    }

    @Override protected Void doExecute() throws IOException {
      try {
        MongoCollection<Document> collection =
          storage.database().getCollection(storage.spanCollection);
        for (Span span : spans) {
          collection.insertOne(toDocument(span));
        }
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<Void> clone() {
      return new InsertSpansCall(storage, spans);
    }
  }
}
