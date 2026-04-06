/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.storage.GroupByTraceId;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StrictTraceId;
import zipkin2.storage.Traces;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Sorts.descending;

final class MongoSpanStore implements SpanStore, Traces, ServiceAndSpanNames {

  final MongoStorage storage;
  final boolean strictTraceId, searchEnabled;
  final Call.Mapper<List<Span>, List<List<Span>>> groupByTraceId;

  MongoSpanStore(MongoStorage storage) {
    this.storage = storage;
    this.strictTraceId = storage.strictTraceId;
    this.searchEnabled = storage.searchEnabled;
    this.groupByTraceId = GroupByTraceId.create(strictTraceId);
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest request) {
    if (!searchEnabled) return Call.emptyList();
    return new GetTracesCall(this, request);
  }

  @Override public Call<List<Span>> getTrace(String traceId) {
    // make sure we have a 16 or 32 character trace ID
    traceId = Span.normalizeTraceId(traceId);

    // Unless we are strict, truncate the trace ID to 64bit (encoded as 16 characters)
    if (!strictTraceId && traceId.length() == 32) traceId = traceId.substring(16);

    return new GetTraceCall(this, traceId);
  }

  @Override public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
    Set<String> normalizedTraceIds = new LinkedHashSet<>();
    for (String traceId : traceIds) {
      traceId = Span.normalizeTraceId(traceId);
      if (!strictTraceId && traceId.length() == 32) traceId = traceId.substring(16);
      normalizedTraceIds.add(traceId);
    }
    if (normalizedTraceIds.isEmpty()) return Call.emptyList();
    return new GetTracesByIdsCall(this, normalizedTraceIds);
  }

  @Override public Call<List<String>> getServiceNames() {
    if (!searchEnabled) return Call.emptyList();
    return new GetServiceNamesCall(storage);
  }

  @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
    if (serviceName.isEmpty() || !searchEnabled) return Call.emptyList();
    return new GetRemoteServiceNamesCall(storage, serviceName);
  }

  @Override public Call<List<String>> getSpanNames(String serviceName) {
    if (serviceName.isEmpty() || !searchEnabled) return Call.emptyList();
    return new GetSpanNamesCall(storage, serviceName);
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    if (endTs <= 0) throw new IllegalArgumentException("endTs <= 0");
    if (lookback <= 0) throw new IllegalArgumentException("lookback <= 0");
    return new GetDependenciesCall(storage, endTs, lookback);
  }

  static Span documentToSpan(Document doc) {
    Span.Builder builder = Span.newBuilder()
      .traceId(doc.getString("trace_id"))
      .id(doc.getString("id"));

    if (doc.containsKey("parent_id")) builder.parentId(doc.getString("parent_id"));
    if (doc.containsKey("kind")) builder.kind(Span.Kind.valueOf(doc.getString("kind")));
    if (doc.containsKey("name")) builder.name(doc.getString("name"));

    if (doc.containsKey("timestamp")) {
      builder.timestamp(doc.getLong("timestamp"));
    }
    if (doc.containsKey("duration")) {
      builder.duration(doc.getLong("duration"));
    }

    if (doc.containsKey("local_endpoint")) {
      builder.localEndpoint(documentToEndpoint(doc.get("local_endpoint", Document.class)));
    }
    if (doc.containsKey("remote_endpoint")) {
      builder.remoteEndpoint(documentToEndpoint(doc.get("remote_endpoint", Document.class)));
    }

    if (doc.containsKey("annotations")) {
      List<Document> annotations = doc.getList("annotations", Document.class);
      for (Document a : annotations) {
        builder.addAnnotation(a.getLong("timestamp"), a.getString("value"));
      }
    }

    if (doc.containsKey("tags")) {
      Document tags = doc.get("tags", Document.class);
      for (Map.Entry<String, Object> entry : tags.entrySet()) {
        builder.putTag(entry.getKey(), String.valueOf(entry.getValue()));
      }
    }

    if (doc.containsKey("debug")) builder.debug(doc.getBoolean("debug"));
    if (doc.containsKey("shared")) builder.shared(doc.getBoolean("shared"));

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

  static final class GetTracesCall extends Call.Base<List<List<Span>>> {
    final MongoSpanStore spanStore;
    final QueryRequest request;

    GetTracesCall(MongoSpanStore spanStore, QueryRequest request) {
      this.spanStore = spanStore;
      this.request = request;
    }

    @Override protected List<List<Span>> doExecute() throws IOException {
      MongoCollection<Document> col = spanStore.storage.spans();

      long endMillis = request.endTs();
      long beginMillis = endMillis - request.lookback();

      List<org.bson.conversions.Bson> filters = new ArrayList<>();
      filters.add(gte("timestamp_millis", beginMillis));
      filters.add(lte("timestamp_millis", endMillis));

      if (request.serviceName() != null) {
        filters.add(eq("local_endpoint_service_name", request.serviceName()));
      }
      if (request.remoteServiceName() != null) {
        filters.add(eq("remote_endpoint_service_name", request.remoteServiceName()));
      }
      if (request.spanName() != null) {
        filters.add(eq("name", request.spanName()));
      }

      for (Map.Entry<String, String> kv : request.annotationQuery().entrySet()) {
        if (kv.getValue().isEmpty()) {
          filters.add(eq("annotations_query", kv.getKey()));
        } else {
          filters.add(eq("annotations_query", kv.getKey() + "=" + kv.getValue()));
        }
      }

      if (request.minDuration() != null) {
        filters.add(gte("duration", request.minDuration()));
        if (request.maxDuration() != null) {
          filters.add(lte("duration", request.maxDuration()));
        }
      }

      // First, find distinct trace IDs matching the query, ordered by timestamp desc
      // Use aggregation to group by trace_id and limit results
      List<org.bson.conversions.Bson> pipeline = new ArrayList<>();
      pipeline.add(com.mongodb.client.model.Aggregates.match(combineFilters(filters)));
      pipeline.add(com.mongodb.client.model.Aggregates.sort(descending("timestamp_millis")));
      pipeline.add(com.mongodb.client.model.Aggregates.group("$trace_id",
        com.mongodb.client.model.Accumulators.max("max_ts", "$timestamp_millis")));
      pipeline.add(com.mongodb.client.model.Aggregates.sort(descending("max_ts")));
      pipeline.add(com.mongodb.client.model.Aggregates.limit(request.limit()));

      AggregateIterable<Document> aggregate = col.aggregate(pipeline);
      List<String> traceIds = new ArrayList<>();
      try (MongoCursor<Document> cursor = aggregate.iterator()) {
        while (cursor.hasNext()) {
          traceIds.add(cursor.next().getString("_id"));
        }
      }

      if (traceIds.isEmpty()) return List.of();

      // Now fetch all spans for the matched trace IDs
      FindIterable<Document> spanDocs = col.find(in("trace_id", traceIds));
      List<Span> spans = new ArrayList<>();
      try (MongoCursor<Document> cursor = spanDocs.iterator()) {
        while (cursor.hasNext()) {
          spans.add(documentToSpan(cursor.next()));
        }
      }

      List<List<Span>> result = spanStore.groupByTraceId.map(spans);
      return spanStore.strictTraceId
        ? StrictTraceId.filterTraces(request).map(result)
        : result;
    }

    static org.bson.conversions.Bson combineFilters(List<org.bson.conversions.Bson> filters) {
      if (filters.size() == 1) return filters.get(0);
      return and(filters);
    }

    @Override protected void doEnqueue(Callback<List<List<Span>>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetTracesCall(spanStore, request);
    }
  }

  static final class GetTraceCall extends Call.Base<List<Span>> {
    final MongoSpanStore spanStore;
    final String traceId;

    GetTraceCall(MongoSpanStore spanStore, String traceId) {
      this.spanStore = spanStore;
      this.traceId = traceId;
    }

    @Override protected List<Span> doExecute() throws IOException {
      MongoCollection<Document> col = spanStore.storage.spans();

      org.bson.conversions.Bson filter;
      if (!spanStore.strictTraceId) {
        // When not strict, match on the lower 64 bits using regex
        filter = com.mongodb.client.model.Filters.regex("trace_id", traceId + "$");
      } else {
        filter = eq("trace_id", traceId);
      }

      FindIterable<Document> docs = col.find(filter);
      List<Span> result = new ArrayList<>();
      try (MongoCursor<Document> cursor = docs.iterator()) {
        while (cursor.hasNext()) {
          result.add(documentToSpan(cursor.next()));
        }
      }

      return spanStore.strictTraceId
        ? StrictTraceId.filterSpans(traceId).map(result)
        : result;
    }

    @Override protected void doEnqueue(Callback<List<Span>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<Span>> clone() {
      return new GetTraceCall(spanStore, traceId);
    }
  }

  static final class GetTracesByIdsCall extends Call.Base<List<List<Span>>> {
    final MongoSpanStore spanStore;
    final Set<String> traceIds;

    GetTracesByIdsCall(MongoSpanStore spanStore, Set<String> traceIds) {
      this.spanStore = spanStore;
      this.traceIds = traceIds;
    }

    @Override protected List<List<Span>> doExecute() throws IOException {
      MongoCollection<Document> col = spanStore.storage.spans();

      FindIterable<Document> docs = col.find(in("trace_id", traceIds));
      List<Span> spans = new ArrayList<>();
      try (MongoCursor<Document> cursor = docs.iterator()) {
        while (cursor.hasNext()) {
          spans.add(documentToSpan(cursor.next()));
        }
      }

      return spanStore.groupByTraceId.map(spans);
    }

    @Override protected void doEnqueue(Callback<List<List<Span>>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetTracesByIdsCall(spanStore, traceIds);
    }
  }

  static final class GetServiceNamesCall extends Call.Base<List<String>> {
    final MongoStorage storage;

    GetServiceNamesCall(MongoStorage storage) {
      this.storage = storage;
    }

    @Override protected List<String> doExecute() throws IOException {
      MongoCollection<Document> col = storage.spans();
      Set<String> result = new LinkedHashSet<>();
      try (MongoCursor<String> cursor =
        col.distinct("local_endpoint_service_name", String.class).iterator()) {
        while (cursor.hasNext()) {
          String name = cursor.next();
          if (name != null && !name.isEmpty()) result.add(name);
        }
      }
      List<String> sorted = new ArrayList<>(result);
      sorted.sort(String::compareTo);
      return sorted;
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetServiceNamesCall(storage);
    }
  }

  static final class GetRemoteServiceNamesCall extends Call.Base<List<String>> {
    final MongoStorage storage;
    final String serviceName;

    GetRemoteServiceNamesCall(MongoStorage storage, String serviceName) {
      this.storage = storage;
      this.serviceName = serviceName;
    }

    @Override protected List<String> doExecute() throws IOException {
      MongoCollection<Document> col = storage.spans();
      Set<String> result = new LinkedHashSet<>();
      try (MongoCursor<String> cursor =
        col.distinct("remote_endpoint_service_name", String.class)
          .filter(eq("local_endpoint_service_name", serviceName.toLowerCase(Locale.ROOT)))
          .iterator()) {
        while (cursor.hasNext()) {
          String name = cursor.next();
          if (name != null && !name.isEmpty()) result.add(name);
        }
      }
      List<String> sorted = new ArrayList<>(result);
      sorted.sort(String::compareTo);
      return sorted;
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetRemoteServiceNamesCall(storage, serviceName);
    }
  }

  static final class GetSpanNamesCall extends Call.Base<List<String>> {
    final MongoStorage storage;
    final String serviceName;

    GetSpanNamesCall(MongoStorage storage, String serviceName) {
      this.storage = storage;
      this.serviceName = serviceName;
    }

    @Override protected List<String> doExecute() throws IOException {
      MongoCollection<Document> col = storage.spans();
      Set<String> result = new LinkedHashSet<>();
      try (MongoCursor<String> cursor =
        col.distinct("name", String.class)
          .filter(eq("local_endpoint_service_name", serviceName.toLowerCase(Locale.ROOT)))
          .iterator()) {
        while (cursor.hasNext()) {
          String name = cursor.next();
          if (name != null && !name.isEmpty()) result.add(name);
        }
      }
      List<String> sorted = new ArrayList<>(result);
      sorted.sort(String::compareTo);
      return sorted;
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetSpanNamesCall(storage, serviceName);
    }
  }

  static final class GetDependenciesCall extends Call.Base<List<DependencyLink>> {
    final MongoStorage storage;
    final long endTs, lookback;

    GetDependenciesCall(MongoStorage storage, long endTs, long lookback) {
      this.storage = storage;
      this.endTs = endTs;
      this.lookback = lookback;
    }

    @Override protected List<DependencyLink> doExecute() throws IOException {
      MongoCollection<Document> col = storage.dependencies();

      long beginMillis = endTs - lookback;

      FindIterable<Document> docs = col.find(
        and(gte("day_millis", beginMillis), lte("day_millis", endTs)));

      // Aggregate dependency links with same parent-child using count maps
      Map<String, long[]> countMap = new LinkedHashMap<>(); // [callCount, errorCount]
      Map<String, String[]> nameMap = new LinkedHashMap<>(); // [parent, child]
      try (MongoCursor<Document> cursor = docs.iterator()) {
        while (cursor.hasNext()) {
          Document doc = cursor.next();
          String parent = doc.getString("parent");
          String child = doc.getString("child");
          String key = parent + "->" + child;

          long callCount = doc.containsKey("call_count") ? doc.getLong("call_count") : 0L;
          long errorCount = doc.containsKey("error_count") ? doc.getLong("error_count") : 0L;

          long[] counts = countMap.get(key);
          if (counts == null) {
            countMap.put(key, new long[] {callCount, errorCount});
            nameMap.put(key, new String[] {parent, child});
          } else {
            counts[0] += callCount;
            counts[1] += errorCount;
          }
        }
      }

      List<DependencyLink> result = new ArrayList<>();
      for (Map.Entry<String, long[]> entry : countMap.entrySet()) {
        String[] names = nameMap.get(entry.getKey());
        long[] counts = entry.getValue();
        result.add(DependencyLink.newBuilder()
          .parent(names[0])
          .child(names[1])
          .callCount(counts[0])
          .errorCount(counts[1])
          .build());
      }
      return result;
    }

    @Override protected void doEnqueue(Callback<List<DependencyLink>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<DependencyLink>> clone() {
      return new GetDependenciesCall(storage, endTs, lookback);
    }
  }
}
