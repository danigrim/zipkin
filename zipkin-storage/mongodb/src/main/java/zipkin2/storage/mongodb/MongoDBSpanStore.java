/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import org.bson.conversions.Bson;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;
import zipkin2.storage.Traces;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Sorts.descending;

class MongoDBSpanStore implements SpanStore, Traces, ServiceAndSpanNames {

  final MongoDBStorage storage;

  MongoDBSpanStore(MongoDBStorage storage) {
    this.storage = storage;
  }

  MongoCollection<Document> spans() {
    return storage.database().getCollection(storage.spanCollection);
  }

  // --- Traces ------------------------------------------------------------------

  @Override public Call<List<Span>> getTrace(String traceId) {
    if (traceId.length() != 16 && traceId.length() != 32) return Call.emptyList();
    String normalizedTraceId = traceId.toLowerCase(Locale.ROOT);
    return new GetTraceCall(this, normalizedTraceId);
  }

  @Override public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
    Set<String> normalizedIds = new LinkedHashSet<>();
    for (String traceId : traceIds) {
      normalizedIds.add(traceId.toLowerCase(Locale.ROOT));
    }
    if (normalizedIds.isEmpty()) return Call.emptyList();
    return new GetTracesCall(this, normalizedIds);
  }

  // --- SpanStore ---------------------------------------------------------------

  @Override public Call<List<List<Span>>> getTraces(QueryRequest request) {
    if (!storage.searchEnabled) return Call.emptyList();
    return new SearchTracesCall(this, request);
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    if (endTs <= 0) throw new IllegalArgumentException("endTs <= 0");
    if (lookback <= 0) throw new IllegalArgumentException("lookback <= 0");
    return new GetDependenciesCall(this, endTs, lookback);
  }

  // --- ServiceAndSpanNames ----------------------------------------------------

  @Override public Call<List<String>> getServiceNames() {
    if (!storage.searchEnabled) return Call.emptyList();
    return new DistinctCall(this, "localEndpoint.serviceName");
  }

  @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
    if (!storage.searchEnabled) return Call.emptyList();
    return new DistinctCall(this, "remoteEndpoint.serviceName",
      eq("localEndpoint.serviceName", serviceName.toLowerCase(Locale.ROOT)));
  }

  @Override public Call<List<String>> getSpanNames(String serviceName) {
    if (!storage.searchEnabled) return Call.emptyList();
    return new DistinctCall(this, "name",
      eq("localEndpoint.serviceName", serviceName.toLowerCase(Locale.ROOT)));
  }

  // --- Document to Span conversion ---

  static Span documentToSpan(Document doc) {
    Span.Builder builder = Span.newBuilder()
      .traceId(doc.getString("traceId"))
      .id(doc.getString("id"));

    if (doc.containsKey("parentId")) builder.parentId(doc.getString("parentId"));
    if (doc.containsKey("kind")) builder.kind(Span.Kind.valueOf(doc.getString("kind")));
    if (doc.containsKey("name")) builder.name(doc.getString("name"));
    if (doc.containsKey("timestamp")) {
      builder.timestamp(doc.get("timestamp") instanceof Long
        ? doc.getLong("timestamp")
        : ((Number) doc.get("timestamp")).longValue());
    }
    if (doc.containsKey("duration")) {
      builder.duration(doc.get("duration") instanceof Long
        ? doc.getLong("duration")
        : ((Number) doc.get("duration")).longValue());
    }
    if (doc.containsKey("localEndpoint")) {
      builder.localEndpoint(documentToEndpoint(doc.get("localEndpoint", Document.class)));
    }
    if (doc.containsKey("remoteEndpoint")) {
      builder.remoteEndpoint(documentToEndpoint(doc.get("remoteEndpoint", Document.class)));
    }
    if (doc.containsKey("annotations")) {
      List<Document> annotations = doc.getList("annotations", Document.class);
      for (Document aDoc : annotations) {
        long ts = aDoc.get("timestamp") instanceof Long
          ? aDoc.getLong("timestamp")
          : ((Number) aDoc.get("timestamp")).longValue();
        builder.addAnnotation(ts, aDoc.getString("value"));
      }
    }
    if (doc.containsKey("tags")) {
      Document tags = doc.get("tags", Document.class);
      for (Map.Entry<String, Object> entry : tags.entrySet()) {
        builder.putTag(entry.getKey(), String.valueOf(entry.getValue()));
      }
    }
    if (doc.containsKey("debug") && Boolean.TRUE.equals(doc.getBoolean("debug"))) {
      builder.debug(true);
    }
    if (doc.containsKey("shared") && Boolean.TRUE.equals(doc.getBoolean("shared"))) {
      builder.shared(true);
    }
    return builder.build();
  }

  static Endpoint documentToEndpoint(Document doc) {
    Endpoint.Builder builder = Endpoint.newBuilder();
    if (doc.containsKey("serviceName")) builder.serviceName(doc.getString("serviceName"));
    if (doc.containsKey("ipv4")) builder.ip(doc.getString("ipv4"));
    if (doc.containsKey("ipv6")) builder.ip(doc.getString("ipv6"));
    if (doc.containsKey("port")) builder.port(doc.getInteger("port"));
    return builder.build();
  }

  // --- Call implementations ---

  static final class GetTraceCall extends Call.Base<List<Span>> {
    final MongoDBSpanStore store;
    final String traceId;

    GetTraceCall(MongoDBSpanStore store, String traceId) {
      this.store = store;
      this.traceId = traceId;
    }

    @Override protected List<Span> doExecute() throws IOException {
      try {
        Bson filter;
        if (!store.storage.strictTraceId && traceId.length() == 32) {
          filter = regex("traceId", traceId.substring(16) + "$");
        } else {
          filter = eq("traceId", traceId);
        }
        List<Span> result = new ArrayList<>();
        try (MongoCursor<Document> cursor = store.spans().find(filter).iterator()) {
          while (cursor.hasNext()) {
            result.add(documentToSpan(cursor.next()));
          }
        }
        return result.isEmpty() ? List.of() : result;
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<Span>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<Span>> clone() {
      return new GetTraceCall(store, traceId);
    }
  }

  static final class GetTracesCall extends Call.Base<List<List<Span>>> {
    final MongoDBSpanStore store;
    final Set<String> traceIds;

    GetTracesCall(MongoDBSpanStore store, Set<String> traceIds) {
      this.store = store;
      this.traceIds = traceIds;
    }

    @Override protected List<List<Span>> doExecute() throws IOException {
      try {
        Map<String, List<Span>> grouped = new LinkedHashMap<>();
        Bson filter;
        if (!store.storage.strictTraceId) {
          Set<String> suffixes = new LinkedHashSet<>();
          for (String id : traceIds) {
            suffixes.add(id.length() == 32 ? id.substring(16) : id);
          }
          List<Bson> regexFilters = new ArrayList<>();
          for (String suffix : suffixes) {
            regexFilters.add(regex("traceId", suffix + "$"));
          }
          filter = or(regexFilters);
        } else {
          filter = in("traceId", traceIds);
        }
        try (MongoCursor<Document> cursor = store.spans().find(filter).iterator()) {
          while (cursor.hasNext()) {
            Span span = documentToSpan(cursor.next());
            String key = span.traceId();
            if (!store.storage.strictTraceId && key.length() == 32) {
              key = key.substring(16);
            }
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(span);
          }
        }
        List<List<Span>> result = new ArrayList<>();
        for (String id : traceIds) {
          String key = id;
          if (!store.storage.strictTraceId && key.length() == 32) {
            key = key.substring(16);
          }
          List<Span> trace = grouped.get(key);
          if (trace != null) result.add(trace);
        }
        return result;
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<List<Span>>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetTracesCall(store, traceIds);
    }
  }

  static final class SearchTracesCall extends Call.Base<List<List<Span>>> {
    final MongoDBSpanStore store;
    final QueryRequest request;

    SearchTracesCall(MongoDBSpanStore store, QueryRequest request) {
      this.store = store;
      this.request = request;
    }

    @Override protected List<List<Span>> doExecute() throws IOException {
      try {
        List<Bson> filters = new ArrayList<>();

        if (request.serviceName() != null) {
          filters.add(eq("localEndpoint.serviceName", request.serviceName()));
        }
        if (request.remoteServiceName() != null) {
          filters.add(eq("remoteEndpoint.serviceName", request.remoteServiceName()));
        }
        if (request.spanName() != null) {
          filters.add(eq("name", request.spanName()));
        }

        // Time range filter using microsecond timestamps
        long endTs = request.endTs() * 1000; // convert millis to micros
        long startTs = endTs - (request.lookback() * 1000);
        filters.add(gte("timestamp", startTs));
        filters.add(lte("timestamp", endTs));

        // Duration filter
        if (request.minDuration() != null) {
          filters.add(gte("duration", request.minDuration()));
        }
        if (request.maxDuration() != null) {
          filters.add(lte("duration", request.maxDuration()));
        }

        // Annotation query
        Map<String, String> annotationQuery = request.annotationQuery();
        if (annotationQuery != null && !annotationQuery.isEmpty()) {
          for (Map.Entry<String, String> entry : annotationQuery.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (value.isEmpty()) {
              filters.add(new Document("$or", List.of(
                new Document("annotations.value", key),
                new Document("tags." + key, new Document("$exists", true))
              )));
            } else {
              filters.add(eq("tags." + key, value));
            }
          }
        }

        Bson filter = filters.isEmpty() ? new Document() : and(filters);

        // First find matching traceIds from root spans matching the filter
        Set<String> matchingTraceIds = new LinkedHashSet<>();
        try (MongoCursor<Document> cursor = store.spans()
          .find(filter)
          .sort(descending("timestamp"))
          .iterator()) {
          while (cursor.hasNext()) {
            Document doc = cursor.next();
            String traceId = doc.getString("traceId");
            if (!store.storage.strictTraceId && traceId.length() == 32) {
              traceId = traceId.substring(16);
            }
            matchingTraceIds.add(traceId);
          }
        }

        if (matchingTraceIds.isEmpty()) return List.of();

        // Limit the number of traces
        List<String> limitedTraceIds = new ArrayList<>();
        for (String id : matchingTraceIds) {
          if (limitedTraceIds.size() >= request.limit()) break;
          limitedTraceIds.add(id);
        }

        // Now fetch all spans for each matched trace
        Map<String, List<Span>> grouped = new LinkedHashMap<>();
        Bson traceFilter;
        if (!store.storage.strictTraceId) {
          List<Bson> regexFilters = new ArrayList<>();
          for (String suffix : limitedTraceIds) {
            regexFilters.add(regex("traceId", suffix + "$"));
          }
          traceFilter = or(regexFilters);
        } else {
          traceFilter = in("traceId", limitedTraceIds);
        }

        try (MongoCursor<Document> cursor = store.spans().find(traceFilter).iterator()) {
          while (cursor.hasNext()) {
            Span span = documentToSpan(cursor.next());
            String key = span.traceId();
            if (!store.storage.strictTraceId && key.length() == 32) {
              key = key.substring(16);
            }
            grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(span);
          }
        }

        List<List<Span>> result = new ArrayList<>();
        for (String id : limitedTraceIds) {
          List<Span> trace = grouped.get(id);
          if (trace != null) result.add(trace);
        }
        return result;
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<List<Span>>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<List<Span>>> clone() {
      return new SearchTracesCall(store, request);
    }
  }

  static final class GetDependenciesCall extends Call.Base<List<DependencyLink>> {
    final MongoDBSpanStore store;
    final long endTs;
    final long lookback;

    GetDependenciesCall(MongoDBSpanStore store, long endTs, long lookback) {
      this.store = store;
      this.endTs = endTs;
      this.lookback = lookback;
    }

    @Override protected List<DependencyLink> doExecute() throws IOException {
      try {
        long startMicros = (endTs - lookback) * 1000;
        long endMicros = endTs * 1000;

        // Find all traceIds that have at least one span in the time range
        Set<String> traceIds = new LinkedHashSet<>();
        try (MongoCursor<Document> cursor = store.spans().find(
          and(gte("timestamp", startMicros), lte("timestamp", endMicros))
        ).iterator()) {
          while (cursor.hasNext()) {
            traceIds.add(cursor.next().getString("traceId"));
          }
        }

        if (traceIds.isEmpty()) return List.of();

        // Fetch ALL spans for those traces (including ones without timestamps)
        Map<String, List<Span>> traces = new LinkedHashMap<>();
        try (MongoCursor<Document> cursor =
               store.spans().find(in("traceId", traceIds)).iterator()) {
          while (cursor.hasNext()) {
            Span span = documentToSpan(cursor.next());
            traces.computeIfAbsent(span.traceId(), k -> new ArrayList<>()).add(span);
          }
        }
        DependencyLinker linker = new DependencyLinker();
        for (List<Span> trace : traces.values()) {
          linker.putTrace(trace);
        }
        return linker.link();
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<DependencyLink>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<DependencyLink>> clone() {
      return new GetDependenciesCall(store, endTs, lookback);
    }
  }

  static final class DistinctCall extends Call.Base<List<String>> {
    final MongoDBSpanStore store;
    final String fieldName;
    final Bson filter;

    DistinctCall(MongoDBSpanStore store, String fieldName) {
      this(store, fieldName, null);
    }

    DistinctCall(MongoDBSpanStore store, String fieldName, Bson filter) {
      this.store = store;
      this.fieldName = fieldName;
      this.filter = filter;
    }

    @Override protected List<String> doExecute() throws IOException {
      try {
        List<String> result = new ArrayList<>();
        if (filter != null) {
          store.spans().distinct(fieldName, filter, String.class).into(result);
        } else {
          store.spans().distinct(fieldName, String.class).into(result);
        }
        result.removeIf(v -> v == null);
        Collections.sort(result);
        return result;
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<String>> clone() {
      return new DistinctCall(store, fieldName, filter);
    }
  }
}
