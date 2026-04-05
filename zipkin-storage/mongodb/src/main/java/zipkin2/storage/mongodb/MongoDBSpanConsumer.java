/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import java.io.IOException;
import java.util.ArrayList;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import zipkin2.Annotation;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;

final class MongoDBSpanConsumer implements SpanConsumer {

  final MongoDBStorage storage;

  MongoDBSpanConsumer(MongoDBStorage storage) {
    this.storage = storage;
  }

  @Override public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    return new StoreSpansCall(this, spans);
  }

  void storeSpans(List<Span> spans) {
    MongoCollection<Document> spansCollection = storage.spansCollection();
    Set<String> serviceNames = new LinkedHashSet<>();
    Set<String> spanNames = new LinkedHashSet<>();
    Set<String> remoteServiceNames = new LinkedHashSet<>();

    for (Span span : spans) {
      Document doc = spanToDocument(span);
      spansCollection.replaceOne(
        Filters.and(
          Filters.eq("trace_id", doc.getString("trace_id")),
          Filters.eq("id", doc.getString("id")),
          Filters.eq("kind", doc.getString("kind"))
        ),
        doc,
        new ReplaceOptions().upsert(true)
      );

      // Track service names, span names, and remote service names
      String localServiceName = span.localServiceName();
      if (localServiceName != null) {
        serviceNames.add(localServiceName);
        if (span.name() != null) {
          spanNames.add(localServiceName + ":" + span.name());
        }
        String remoteServiceName = span.remoteServiceName();
        if (remoteServiceName != null) {
          remoteServiceNames.add(localServiceName + ":" + remoteServiceName);
        }
      }

      // Store autocomplete tags
      if (!storage.autocompleteKeys.isEmpty()) {
        for (Map.Entry<String, String> tag : span.tags().entrySet()) {
          if (storage.autocompleteKeys.contains(tag.getKey())) {
            storeAutocompleteTag(tag.getKey(), tag.getValue());
          }
        }
      }
    }

    if (storage.searchEnabled) {
      storeServiceNames(serviceNames);
      storeSpanNames(spanNames);
      storeRemoteServiceNames(remoteServiceNames);
    }
  }

  void storeServiceNames(Set<String> serviceNames) {
    MongoCollection<Document> collection = storage.serviceNamesCollection();
    for (String name : serviceNames) {
      collection.updateOne(
        Filters.eq("service_name", name),
        Updates.setOnInsert("service_name", name),
        new UpdateOptions().upsert(true)
      );
    }
  }

  void storeSpanNames(Set<String> spanNames) {
    MongoCollection<Document> collection = storage.spanNamesCollection();
    for (String entry : spanNames) {
      int idx = entry.indexOf(':');
      String serviceName = entry.substring(0, idx);
      String spanName = entry.substring(idx + 1);
      collection.updateOne(
        Filters.and(
          Filters.eq("service_name", serviceName),
          Filters.eq("span_name", spanName)
        ),
        Updates.combine(
          Updates.setOnInsert("service_name", serviceName),
          Updates.setOnInsert("span_name", spanName)
        ),
        new UpdateOptions().upsert(true)
      );
    }
  }

  void storeRemoteServiceNames(Set<String> remoteServiceNames) {
    MongoCollection<Document> collection = storage.remoteServiceNamesCollection();
    for (String entry : remoteServiceNames) {
      int idx = entry.indexOf(':');
      String serviceName = entry.substring(0, idx);
      String remoteServiceName = entry.substring(idx + 1);
      collection.updateOne(
        Filters.and(
          Filters.eq("service_name", serviceName),
          Filters.eq("remote_service_name", remoteServiceName)
        ),
        Updates.combine(
          Updates.setOnInsert("service_name", serviceName),
          Updates.setOnInsert("remote_service_name", remoteServiceName)
        ),
        new UpdateOptions().upsert(true)
      );
    }
  }

  void storeAutocompleteTag(String key, String value) {
    MongoCollection<Document> collection = storage.autocompleteTagsCollection();
    collection.updateOne(
      Filters.and(
        Filters.eq("key", key),
        Filters.eq("value", value)
      ),
      Updates.combine(
        Updates.setOnInsert("key", key),
        Updates.setOnInsert("value", value),
        Updates.currentDate("ts")
      ),
      new UpdateOptions().upsert(true)
    );
  }

  static Document spanToDocument(Span span) {
    Document doc = new Document();
    doc.put("trace_id", span.traceId());
    if (span.parentId() != null) doc.put("parent_id", span.parentId());
    doc.put("id", span.id());
    if (span.kind() != null) doc.put("kind", span.kind().name());
    if (span.name() != null) doc.put("name", span.name());
    if (span.timestampAsLong() > 0) {
      doc.put("ts", Instant.ofEpochMilli(span.timestampAsLong() / 1000));
      doc.put("ts_micro", span.timestampAsLong());
    }
    if (span.durationAsLong() > 0) doc.put("duration", span.durationAsLong());
    if (span.localEndpoint() != null) {
      doc.put("local_endpoint", endpointToDocument(span.localEndpoint()));
      if (span.localServiceName() != null) {
        doc.put("local_service_name", span.localServiceName());
      }
    }
    if (span.remoteEndpoint() != null) {
      doc.put("remote_endpoint", endpointToDocument(span.remoteEndpoint()));
      if (span.remoteServiceName() != null) {
        doc.put("remote_service_name", span.remoteServiceName());
      }
    }
    if (!span.annotations().isEmpty()) {
      List<Document> annotations = new ArrayList<>();
      for (Annotation a : span.annotations()) {
        annotations.add(new Document("ts", a.timestamp()).append("value", a.value()));
      }
      doc.put("annotations", annotations);
    }
    if (!span.tags().isEmpty()) {
      doc.put("tags", new Document(span.tags()));
    }
    if (Boolean.TRUE.equals(span.debug())) doc.put("debug", true);
    if (Boolean.TRUE.equals(span.shared())) doc.put("shared", true);

    // Build annotations_query field for tag/annotation search
    List<String> annotationsQuery = new ArrayList<>();
    for (Annotation a : span.annotations()) {
      annotationsQuery.add(a.value());
    }
    for (Map.Entry<String, String> tag : span.tags().entrySet()) {
      annotationsQuery.add(tag.getKey());
      annotationsQuery.add(tag.getKey() + "=" + tag.getValue());
    }
    if (!annotationsQuery.isEmpty()) {
      doc.put("annotations_query", annotationsQuery);
    }

    return doc;
  }

  static Document endpointToDocument(Endpoint endpoint) {
    Document doc = new Document();
    if (endpoint.serviceName() != null) doc.put("service_name", endpoint.serviceName());
    if (endpoint.ipv4() != null) doc.put("ipv4", endpoint.ipv4());
    if (endpoint.ipv6() != null) doc.put("ipv6", endpoint.ipv6());
    if (endpoint.portAsInt() != 0) doc.put("port", endpoint.portAsInt());
    return doc;
  }

  @Override public String toString() {
    return "MongoDBSpanConsumer{" + storage + "}";
  }

  static final class StoreSpansCall extends Call.Base<Void> {
    final MongoDBSpanConsumer consumer;
    final List<Span> spans;

    StoreSpansCall(MongoDBSpanConsumer consumer, List<Span> spans) {
      this.consumer = consumer;
      this.spans = spans;
    }

    @Override protected Void doExecute() throws IOException {
      try {
        consumer.storeSpans(spans);
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        consumer.storeSpans(spans);
        callback.onSuccess(null);
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<Void> clone() {
      return new StoreSpansCall(consumer, spans);
    }

    @Override public String toString() {
      return "StoreSpans{spans=" + spans.size() + "}";
    }
  }
}
