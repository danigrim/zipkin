/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.bson.Document;
import org.bson.conversions.Bson;
import zipkin2.Call;
import zipkin2.DependencyLink;
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
import static com.mongodb.client.model.Sorts.descending;
import static zipkin2.storage.mongodb.MongoDBIndexCreator.DEPENDENCIES_COLLECTION;
import static zipkin2.storage.mongodb.MongoDBIndexCreator.SPANS_COLLECTION;

final class MongoDBSpanStore implements SpanStore, Traces, ServiceAndSpanNames {
  final MongoDBStorage storage;

  MongoDBSpanStore(MongoDBStorage storage) {
    this.storage = storage;
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest request) {
    return new GetTracesCall(this, request);
  }

  @Override public Call<List<Span>> getTrace(String traceId) {
    traceId = Span.normalizeTraceId(traceId);
    return new GetTraceCall(this, traceId);
  }

  @Override public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
    Set<String> normalized = new LinkedHashSet<>();
    for (String traceId : traceIds) {
      normalized.add(Span.normalizeTraceId(traceId));
    }
    if (normalized.isEmpty()) return Call.emptyList();
    return new GetTracesByIdsCall(this, normalized);
  }

  @Override public Call<List<String>> getServiceNames() {
    return new GetServiceNamesCall(this);
  }

  @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
    return new GetRemoteServiceNamesCall(this, serviceName);
  }

  @Override public Call<List<String>> getSpanNames(String serviceName) {
    return new GetSpanNamesCall(this, serviceName);
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    return new GetDependenciesCall(this, endTs, lookback);
  }

  @Override public String toString() {
    return "MongoDBSpanStore{" + storage + "}";
  }

  // --- Internal query methods ---

  List<List<Span>> queryTraces(QueryRequest request) {
    MongoCollection<Document> spans = storage.db().getCollection(SPANS_COLLECTION);

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
    if (request.minDuration() != null) {
      filters.add(gte("duration", request.minDuration()));
    }
    if (request.maxDuration() != null) {
      filters.add(lte("duration", request.maxDuration()));
    }

    long endTsMicros = request.endTs() * 1000;
    long startTsMicros = endTsMicros - (request.lookback() * 1000);
    filters.add(gte("timestamp", startTsMicros));
    filters.add(lte("timestamp", endTsMicros));

    for (Map.Entry<String, String> entry : request.annotationQuery().entrySet()) {
      if (entry.getValue().isEmpty()) {
        filters.add(eq("annotations.value", entry.getKey()));
      } else {
        filters.add(and(eq("tags.key", entry.getKey()), eq("tags.value", entry.getValue())));
      }
    }

    Bson filter = filters.isEmpty() ? new Document() : and(filters);

    // First, find distinct trace IDs matching the filter
    Set<String> traceIds = new LinkedHashSet<>();
    try (MongoCursor<Document> cursor = spans.find(filter)
      .sort(descending("timestamp"))
      .iterator()) {
      while (cursor.hasNext() && traceIds.size() < request.limit()) {
        Document doc = cursor.next();
        String traceId = doc.getString("traceId");
        if (!storage.strictTraceId && traceId.length() == 32) {
          traceId = traceId.substring(16);
        }
        traceIds.add(traceId);
      }
    }

    if (traceIds.isEmpty()) return Collections.emptyList();

    return getTracesByIds(traceIds);
  }

  List<Span> getTraceById(String traceId) {
    MongoCollection<Document> spans = storage.db().getCollection(SPANS_COLLECTION);

    Bson filter;
    if (!storage.strictTraceId && traceId.length() <= 16) {
      filter = new Document("traceId", new Document("$regex", traceId + "$"));
    } else {
      filter = eq("traceId", traceId);
    }

    List<Span> result = new ArrayList<>();
    try (MongoCursor<Document> cursor = spans.find(filter).iterator()) {
      while (cursor.hasNext()) {
        result.add(MongoDBSpanCodec.fromDocument(cursor.next()));
      }
    }
    return result;
  }

  List<List<Span>> getTracesByIds(Set<String> traceIds) {
    MongoCollection<Document> spans = storage.db().getCollection(SPANS_COLLECTION);

    Bson filter;
    if (!storage.strictTraceId) {
      List<Bson> regexFilters = new ArrayList<>();
      for (String id : traceIds) {
        if (id.length() <= 16) {
          regexFilters.add(new Document("traceId", new Document("$regex", id + "$")));
        } else {
          regexFilters.add(eq("traceId", id));
        }
      }
      filter = new Document("$or", regexFilters);
    } else {
      filter = in("traceId", traceIds);
    }

    Map<String, List<Span>> grouped = new LinkedHashMap<>();
    try (MongoCursor<Document> cursor = spans.find(filter).iterator()) {
      while (cursor.hasNext()) {
        Span span = MongoDBSpanCodec.fromDocument(cursor.next());
        String groupKey = storage.strictTraceId ? span.traceId() : span.traceId().length() == 32
          ? span.traceId().substring(16) : span.traceId();
        grouped.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(span);
      }
    }

    List<List<Span>> result = new ArrayList<>(grouped.values());
    // Sort traces by the most recent timestamp first
    result.sort(Comparator.<List<Span>, Long>comparing(
      trace -> trace.stream()
        .map(Span::timestamp)
        .filter(t -> t != null)
        .max(Long::compareTo)
        .orElse(0L)
    ).reversed());
    return result;
  }

  List<String> queryServiceNames() {
    MongoCollection<Document> spans = storage.db().getCollection(SPANS_COLLECTION);
    Set<String> result = new TreeSet<>();
    for (String name : spans.distinct("localEndpoint.serviceName", String.class)) {
      if (name != null && !name.isEmpty()) result.add(name);
    }
    return new ArrayList<>(result);
  }

  List<String> queryRemoteServiceNames(String serviceName) {
    MongoCollection<Document> spans = storage.db().getCollection(SPANS_COLLECTION);
    Set<String> result = new TreeSet<>();
    for (String name : spans.distinct("remoteEndpoint.serviceName",
      eq("localEndpoint.serviceName", serviceName), String.class)) {
      if (name != null && !name.isEmpty()) result.add(name);
    }
    return new ArrayList<>(result);
  }

  List<String> querySpanNames(String serviceName) {
    MongoCollection<Document> spans = storage.db().getCollection(SPANS_COLLECTION);
    Set<String> result = new TreeSet<>();
    for (String name : spans.distinct("name",
      eq("localEndpoint.serviceName", serviceName), String.class)) {
      if (name != null && !name.isEmpty()) result.add(name);
    }
    return new ArrayList<>(result);
  }

  List<DependencyLink> queryDependencies(long endTs, long lookback) {
    // Try stored dependencies first
    MongoCollection<Document> depsCollection =
      storage.db().getCollection(DEPENDENCIES_COLLECTION);

    long startTs = endTs - lookback;
    long startDay = midnightUTC(startTs);
    long endDay = midnightUTC(endTs);

    List<Bson> filters = new ArrayList<>();
    filters.add(gte("day", startDay));
    filters.add(lte("day", endDay));

    List<DependencyLink> links = new ArrayList<>();
    try (MongoCursor<Document> cursor = depsCollection.find(and(filters)).iterator()) {
      while (cursor.hasNext()) {
        Document doc = cursor.next();
        links.add(DependencyLink.newBuilder()
          .parent(doc.getString("parent"))
          .child(doc.getString("child"))
          .callCount(doc.getLong("callCount"))
          .errorCount(doc.containsKey("errorCount") ? doc.getLong("errorCount") : 0L)
          .build());
      }
    }

    if (!links.isEmpty()) return DependencyLinker.merge(links);

    // Fall back to deriving links from spans
    MongoCollection<Document> spansCollection = storage.db().getCollection(SPANS_COLLECTION);
    long startTsMicros = startTs * 1000;
    long endTsMicros = endTs * 1000;

    List<Span> spans = new ArrayList<>();
    try (MongoCursor<Document> cursor = spansCollection.find(
      and(gte("timestamp", startTsMicros), lte("timestamp", endTsMicros))).iterator()) {
      while (cursor.hasNext()) {
        spans.add(MongoDBSpanCodec.fromDocument(cursor.next()));
      }
    }

    if (spans.isEmpty()) return Collections.emptyList();

    // Group by trace ID
    Map<String, List<Span>> grouped = new LinkedHashMap<>();
    for (Span span : spans) {
      String traceId = storage.strictTraceId ? span.traceId()
        : span.traceId().length() == 32 ? span.traceId().substring(16) : span.traceId();
      grouped.computeIfAbsent(traceId, k -> new ArrayList<>()).add(span);
    }

    DependencyLinker linker = new DependencyLinker();
    for (List<Span> trace : grouped.values()) {
      linker.putTrace(trace);
    }
    return linker.link();
  }

  static long midnightUTC(long epochMillis) {
    return epochMillis / 86400000L * 86400000L;
  }

  // --- Call implementations ---

  static final class GetTracesCall extends MongoDBCall<List<List<Span>>> {
    final MongoDBSpanStore store;
    final QueryRequest request;

    GetTracesCall(MongoDBSpanStore store, QueryRequest request) {
      this.store = store;
      this.request = request;
    }

    @Override List<List<Span>> doExecute() {
      return store.queryTraces(request);
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetTracesCall(store, request);
    }

    @Override public String toString() {
      return "GetTraces{request=" + request + "}";
    }
  }

  static final class GetTraceCall extends MongoDBCall<List<Span>> {
    final MongoDBSpanStore store;
    final String traceId;

    GetTraceCall(MongoDBSpanStore store, String traceId) {
      this.store = store;
      this.traceId = traceId;
    }

    @Override List<Span> doExecute() {
      return store.getTraceById(traceId);
    }

    @Override public Call<List<Span>> clone() {
      return new GetTraceCall(store, traceId);
    }

    @Override public String toString() {
      return "GetTrace{traceId=" + traceId + "}";
    }
  }

  static final class GetTracesByIdsCall extends MongoDBCall<List<List<Span>>> {
    final MongoDBSpanStore store;
    final Set<String> traceIds;

    GetTracesByIdsCall(MongoDBSpanStore store, Set<String> traceIds) {
      this.store = store;
      this.traceIds = traceIds;
    }

    @Override List<List<Span>> doExecute() {
      return store.getTracesByIds(traceIds);
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetTracesByIdsCall(store, traceIds);
    }

    @Override public String toString() {
      return "GetTracesByIds{traceIds=" + traceIds + "}";
    }
  }

  static final class GetServiceNamesCall extends MongoDBCall<List<String>> {
    final MongoDBSpanStore store;

    GetServiceNamesCall(MongoDBSpanStore store) {
      this.store = store;
    }

    @Override List<String> doExecute() {
      return store.queryServiceNames();
    }

    @Override public Call<List<String>> clone() {
      return new GetServiceNamesCall(store);
    }

    @Override public String toString() {
      return "GetServiceNames{}";
    }
  }

  static final class GetRemoteServiceNamesCall extends MongoDBCall<List<String>> {
    final MongoDBSpanStore store;
    final String serviceName;

    GetRemoteServiceNamesCall(MongoDBSpanStore store, String serviceName) {
      this.store = store;
      this.serviceName = serviceName;
    }

    @Override List<String> doExecute() {
      return store.queryRemoteServiceNames(serviceName);
    }

    @Override public Call<List<String>> clone() {
      return new GetRemoteServiceNamesCall(store, serviceName);
    }

    @Override public String toString() {
      return "GetRemoteServiceNames{serviceName=" + serviceName + "}";
    }
  }

  static final class GetSpanNamesCall extends MongoDBCall<List<String>> {
    final MongoDBSpanStore store;
    final String serviceName;

    GetSpanNamesCall(MongoDBSpanStore store, String serviceName) {
      this.store = store;
      this.serviceName = serviceName;
    }

    @Override List<String> doExecute() {
      return store.querySpanNames(serviceName);
    }

    @Override public Call<List<String>> clone() {
      return new GetSpanNamesCall(store, serviceName);
    }

    @Override public String toString() {
      return "GetSpanNames{serviceName=" + serviceName + "}";
    }
  }

  static final class GetDependenciesCall extends MongoDBCall<List<DependencyLink>> {
    final MongoDBSpanStore store;
    final long endTs;
    final long lookback;

    GetDependenciesCall(MongoDBSpanStore store, long endTs, long lookback) {
      this.store = store;
      this.endTs = endTs;
      this.lookback = lookback;
    }

    @Override List<DependencyLink> doExecute() {
      return store.queryDependencies(endTs, lookback);
    }

    @Override public Call<List<DependencyLink>> clone() {
      return new GetDependenciesCall(store, endTs, lookback);
    }

    @Override public String toString() {
      return "GetDependencies{endTs=" + endTs + ", lookback=" + lookback + "}";
    }
  }
}
