/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import zipkin2.Annotation;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.DelayLimiter;
import zipkin2.storage.SpanConsumer;

import static zipkin2.internal.RecyclableBuffers.SHORT_STRING_LENGTH;

final class MongoSpanConsumer implements SpanConsumer {
  final MongoStorage storage;
  final Set<String> autocompleteKeys;
  final boolean searchEnabled;
  final DelayLimiter<AutocompleteContext> delayLimiter;

  MongoSpanConsumer(MongoStorage storage) {
    this.storage = storage;
    this.autocompleteKeys = new LinkedHashSet<>(storage.autocompleteKeys);
    this.searchEnabled = storage.searchEnabled;
    this.delayLimiter = DelayLimiter.newBuilder()
      .ttl(storage.autocompleteTtl, TimeUnit.MILLISECONDS)
      .cardinality(storage.autocompleteCardinality).build();
  }

  @Override public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    return new StoreSpansCall(this, spans);
  }

  static Document spanToDocument(Span span, boolean searchEnabled) {
    Document doc = new Document();
    doc.put("trace_id", span.traceId());
    doc.put("id", span.id());
    if (span.parentId() != null) doc.put("parent_id", span.parentId());
    if (span.kind() != null) doc.put("kind", span.kind().name());
    if (span.name() != null) doc.put("name", span.name());

    long timestamp = span.timestampAsLong();
    if (timestamp != 0L) {
      doc.put("timestamp", timestamp);
      doc.put("timestamp_millis", timestamp / 1000L);
    }

    long duration = span.durationAsLong();
    if (duration != 0L) doc.put("duration", duration);

    if (span.localEndpoint() != null) {
      doc.put("local_endpoint", endpointToDocument(span.localEndpoint()));
      if (span.localServiceName() != null) {
        doc.put("local_endpoint_service_name", span.localServiceName());
      }
    }

    if (span.remoteEndpoint() != null) {
      doc.put("remote_endpoint", endpointToDocument(span.remoteEndpoint()));
      if (span.remoteServiceName() != null) {
        doc.put("remote_endpoint_service_name", span.remoteServiceName());
      }
    }

    if (!span.annotations().isEmpty()) {
      List<Document> annotations = new ArrayList<>();
      for (Annotation a : span.annotations()) {
        annotations.add(new Document("timestamp", a.timestamp()).append("value", a.value()));
      }
      doc.put("annotations", annotations);
    }

    if (!span.tags().isEmpty()) {
      doc.put("tags", new Document(span.tags()));
    }

    if (span.debug() != null) doc.put("debug", span.debug());
    if (span.shared() != null) doc.put("shared", span.shared());

    // Build the search index field for annotation queries (like Elasticsearch's _q field)
    if (searchEnabled) {
      List<String> annotationsQuery = new ArrayList<>();
      for (Annotation a : span.annotations()) {
        annotationsQuery.add(a.value());
      }
      for (Map.Entry<String, String> tag : span.tags().entrySet()) {
        annotationsQuery.add(tag.getKey());
        annotationsQuery.add(tag.getKey() + "=" + tag.getValue());
      }
      if (!annotationsQuery.isEmpty()) doc.put("annotations_query", annotationsQuery);
    }

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

  static final class StoreSpansCall extends Call.Base<Void> {
    final MongoSpanConsumer consumer;
    final List<Span> spans;

    StoreSpansCall(MongoSpanConsumer consumer, List<Span> spans) {
      this.consumer = consumer;
      this.spans = spans;
    }

    @Override protected Void doExecute() throws IOException {
      MongoCollection<Document> spansCol = consumer.storage.spans();
      MongoCollection<Document> tagsCol = consumer.storage.autocompleteTagsCollection();

      for (Span span : spans) {
        Document doc = spanToDocument(span, consumer.searchEnabled);

        // Upsert by trace_id + id + shared to handle duplicates
        Document filter = new Document("trace_id", span.traceId())
          .append("id", span.id());
        if (Boolean.TRUE.equals(span.shared())) {
          filter.append("shared", true);
        } else {
          filter.append("shared", new Document("$ne", true));
        }
        spansCol.replaceOne(filter, doc, new ReplaceOptions().upsert(true));

        // Index autocomplete tags
        if (consumer.searchEnabled && !span.tags().isEmpty()) {
          for (Map.Entry<String, String> tag : span.tags().entrySet()) {
            int length = tag.getKey().length() + tag.getValue().length() + 1;
            if (length > SHORT_STRING_LENGTH) continue;
            if (!consumer.autocompleteKeys.contains(tag.getKey())) continue;

            AutocompleteContext context = new AutocompleteContext(tag.getKey(), tag.getValue());
            if (!consumer.delayLimiter.shouldInvoke(context)) continue;

            Document tagDoc = new Document("key", tag.getKey()).append("value", tag.getValue());
            Document tagFilter = new Document("key", tag.getKey()).append("value", tag.getValue());
            tagsCol.replaceOne(tagFilter, tagDoc, new ReplaceOptions().upsert(true));
          }
        }
      }
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<Void> clone() {
      return new StoreSpansCall(consumer, spans);
    }

    @Override public String toString() {
      return "StoreSpans{spans=" + spans + "}";
    }
  }

  static final class AutocompleteContext {
    final String key, value;

    AutocompleteContext(String key, String value) {
      this.key = key;
      this.value = value;
    }

    @Override public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof AutocompleteContext that)) return false;
      return key.equals(that.key) && value.equals(that.value);
    }

    @Override public int hashCode() {
      int h$ = 1;
      h$ *= 1000003;
      h$ ^= key.hashCode();
      h$ *= 1000003;
      h$ ^= value.hashCode();
      return h$;
    }
  }
}
