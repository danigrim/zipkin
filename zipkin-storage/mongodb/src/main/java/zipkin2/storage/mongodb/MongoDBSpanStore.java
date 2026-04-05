/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import org.bson.conversions.Bson;
import zipkin2.Annotation;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;
import zipkin2.internal.Nullable;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;
import zipkin2.storage.Traces;

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
    Set<String> normalizedIds = new LinkedHashSet<>();
    for (String traceId : traceIds) {
      normalizedIds.add(Span.normalizeTraceId(traceId));
    }
    if (normalizedIds.isEmpty()) return Call.emptyList();
    return new GetTracesByIdsCall(this, normalizedIds);
  }

  @Override public Call<List<String>> getServiceNames() {
    return new GetServiceNamesCall(this);
  }

  @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
    if (serviceName == null || serviceName.isEmpty()) return Call.emptyList();
    return new GetRemoteServiceNamesCall(this, serviceName);
  }

  @Override public Call<List<String>> getSpanNames(String serviceName) {
    if (serviceName == null || serviceName.isEmpty()) return Call.emptyList();
    return new GetSpanNamesCall(this, serviceName);
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    return new GetDependenciesCall(this, endTs, lookback);
  }

  List<List<Span>> executeGetTraces(QueryRequest request) {
    MongoCollection<Document> spans = storage.spansCollection();

    List<Bson> filters = new ArrayList<>();

    // Time range filter (timestamps are in microseconds, endTs/lookback are in milliseconds)
    long endTsMicros = request.endTs() * 1000;
    long startTsMicros = (request.endTs() - request.lookback()) * 1000;
    filters.add(Filters.gte("ts_micro", startTsMicros));
    filters.add(Filters.lte("ts_micro", endTsMicros));

    if (request.serviceName() != null) {
      filters.add(Filters.eq("local_service_name", request.serviceName()));
    }
    if (request.remoteServiceName() != null) {
      filters.add(Filters.eq("remote_service_name", request.remoteServiceName()));
    }
    if (request.spanName() != null) {
      filters.add(Filters.eq("name", request.spanName()));
    }
    if (request.minDuration() != null) {
      filters.add(Filters.gte("duration", request.minDuration()));
    }
    if (request.maxDuration() != null) {
      filters.add(Filters.lte("duration", request.maxDuration()));
    }

    // Annotation/tag query
    for (Map.Entry<String, String> entry : request.annotationQuery().entrySet()) {
      if (entry.getValue().isEmpty()) {
        // Annotation or tag key exists
        filters.add(Filters.in("annotations_query", entry.getKey()));
      } else {
        // Tag key=value
        filters.add(Filters.in("annotations_query", entry.getKey() + "=" + entry.getValue()));
      }
    }

    Bson query = Filters.and(filters);

    // First, find distinct matching trace IDs, sorted by most recent
    FindIterable<Document> result = spans.find(query)
      .sort(Sorts.descending("ts"))
      .limit(storage.maxTraceCols);

    Set<String> traceIds = new LinkedHashSet<>();
    try (MongoCursor<Document> cursor = result.iterator()) {
      while (cursor.hasNext() && traceIds.size() < request.limit()) {
        Document doc = cursor.next();
        String traceId = doc.getString("trace_id");
        if (traceId != null) traceIds.add(traceId);
      }
    }

    if (traceIds.isEmpty()) return Collections.emptyList();

    // Fetch all spans for matching trace IDs
    return getTracesByIds(traceIds);
  }

  List<Span> executeGetTrace(String traceId) {
    MongoCollection<Document> spans = storage.spansCollection();
    List<Bson> filters = new ArrayList<>();
    if (!storage.strictTraceId && traceId.length() == 32) {
      // When strict trace ID is disabled, match on the right-most 16 characters
      filters.add(Filters.regex("trace_id", traceId.substring(16) + "$"));
    } else {
      filters.add(Filters.eq("trace_id", traceId));
    }

    List<Span> result = new ArrayList<>();
    try (MongoCursor<Document> cursor = spans.find(Filters.and(filters)).iterator()) {
      while (cursor.hasNext()) {
        result.add(documentToSpan(cursor.next()));
      }
    }
    return result;
  }

  List<List<Span>> getTracesByIds(Set<String> traceIds) {
    MongoCollection<Document> spans = storage.spansCollection();
    List<Bson> filters = new ArrayList<>();

    if (!storage.strictTraceId) {
      // When strict trace ID is disabled, match on the right-most 16 characters
      List<Bson> orFilters = new ArrayList<>();
      for (String traceId : traceIds) {
        if (traceId.length() == 32) {
          orFilters.add(Filters.regex("trace_id", traceId.substring(16) + "$"));
        } else {
          orFilters.add(Filters.eq("trace_id", traceId));
        }
      }
      filters.add(Filters.or(orFilters));
    } else {
      filters.add(Filters.in("trace_id", traceIds));
    }

    Map<String, List<Span>> grouped = new LinkedHashMap<>();
    try (MongoCursor<Document> cursor = spans.find(Filters.and(filters)).iterator()) {
      while (cursor.hasNext()) {
        Span span = documentToSpan(cursor.next());
        String groupKey = storage.strictTraceId ? span.traceId()
          : span.traceId().length() == 32 ? span.traceId().substring(16) : span.traceId();
        grouped.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(span);
      }
    }
    return new ArrayList<>(grouped.values());
  }

  List<String> executeGetServiceNames() {
    MongoCollection<Document> collection = storage.serviceNamesCollection();
    List<String> result = new ArrayList<>();
    try (MongoCursor<Document> cursor = collection.find()
      .sort(Sorts.ascending("service_name")).iterator()) {
      while (cursor.hasNext()) {
        String name = cursor.next().getString("service_name");
        if (name != null) result.add(name);
      }
    }
    return result;
  }

  List<String> executeGetRemoteServiceNames(String serviceName) {
    MongoCollection<Document> collection = storage.remoteServiceNamesCollection();
    List<String> result = new ArrayList<>();
    try (MongoCursor<Document> cursor = collection.find(
      Filters.eq("service_name", serviceName))
      .sort(Sorts.ascending("remote_service_name")).iterator()) {
      while (cursor.hasNext()) {
        String name = cursor.next().getString("remote_service_name");
        if (name != null) result.add(name);
      }
    }
    return result;
  }

  List<String> executeGetSpanNames(String serviceName) {
    MongoCollection<Document> collection = storage.spanNamesCollection();
    List<String> result = new ArrayList<>();
    try (MongoCursor<Document> cursor = collection.find(
      Filters.eq("service_name", serviceName))
      .sort(Sorts.ascending("span_name")).iterator()) {
      while (cursor.hasNext()) {
        String name = cursor.next().getString("span_name");
        if (name != null) result.add(name);
      }
    }
    return result;
  }

  List<DependencyLink> executeGetDependencies(long endTs, long lookback) {
    // Use DependencyLinker to compute dependencies from spans
    MongoCollection<Document> spans = storage.spansCollection();

    long endTsMicros = endTs * 1000;
    long startTsMicros = (endTs - lookback) * 1000;

    Bson query = Filters.and(
      Filters.gte("ts_micro", startTsMicros),
      Filters.lte("ts_micro", endTsMicros)
    );

    // Group spans by trace ID
    Map<String, List<Span>> traceIdToSpans = new LinkedHashMap<>();
    try (MongoCursor<Document> cursor = spans.find(query).iterator()) {
      while (cursor.hasNext()) {
        Span span = documentToSpan(cursor.next());
        // Use right-most 16 characters for grouping, as specified in SpanStore docs
        String groupKey = span.traceId().length() == 32
          ? span.traceId().substring(16) : span.traceId();
        traceIdToSpans.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(span);
      }
    }

    // Use DependencyLinker to compute dependency links
    List<DependencyLink> allLinks = new ArrayList<>();
    for (List<Span> trace : traceIdToSpans.values()) {
      DependencyLinker linker = new DependencyLinker();
      linker.putTrace(trace);
      allLinks.addAll(linker.link());
    }

    // Merge links with the same parent and child
    return DependencyLinker.merge(allLinks);
  }

  static Span documentToSpan(Document doc) {
    Span.Builder builder = Span.newBuilder();
    builder.traceId(doc.getString("trace_id"));
    if (doc.containsKey("parent_id")) builder.parentId(doc.getString("parent_id"));
    builder.id(doc.getString("id"));
    if (doc.containsKey("kind")) {
      builder.kind(Span.Kind.valueOf(doc.getString("kind")));
    }
    if (doc.containsKey("name")) builder.name(doc.getString("name"));
    if (doc.containsKey("ts_micro")) {
      builder.timestamp(doc.getLong("ts_micro"));
    }
    if (doc.containsKey("duration")) builder.duration(doc.getLong("duration"));
    if (doc.containsKey("local_endpoint")) {
      builder.localEndpoint(documentToEndpoint(doc.get("local_endpoint", Document.class)));
    }
    if (doc.containsKey("remote_endpoint")) {
      builder.remoteEndpoint(documentToEndpoint(doc.get("remote_endpoint", Document.class)));
    }
    if (doc.containsKey("annotations")) {
      List<Document> annotations = doc.getList("annotations", Document.class);
      for (Document a : annotations) {
        builder.addAnnotation(a.getLong("ts"), a.getString("value"));
      }
    }
    if (doc.containsKey("tags")) {
      Document tags = doc.get("tags", Document.class);
      for (Map.Entry<String, Object> tag : tags.entrySet()) {
        builder.putTag(tag.getKey(), String.valueOf(tag.getValue()));
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
    if (doc == null) return null;
    Endpoint.Builder builder = Endpoint.newBuilder();
    if (doc.containsKey("service_name")) builder.serviceName(doc.getString("service_name"));
    if (doc.containsKey("ipv4")) builder.ip(doc.getString("ipv4"));
    if (doc.containsKey("ipv6")) builder.ip(doc.getString("ipv6"));
    if (doc.containsKey("port")) builder.port(doc.getInteger("port"));
    return builder.build();
  }

  @Override public String toString() {
    return "MongoDBSpanStore{" + storage + "}";
  }

  // Call implementations

  static final class GetTracesCall extends Call.Base<List<List<Span>>> {
    final MongoDBSpanStore store;
    final QueryRequest request;

    GetTracesCall(MongoDBSpanStore store, QueryRequest request) {
      this.store = store;
      this.request = request;
    }

    @Override protected List<List<Span>> doExecute() throws IOException {
      try {
        return store.executeGetTraces(request);
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<List<Span>>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetTracesCall(store, request);
    }

    @Override public String toString() {
      return "GetTraces{request=" + request + "}";
    }
  }

  static final class GetTraceCall extends Call.Base<List<Span>> {
    final MongoDBSpanStore store;
    final String traceId;

    GetTraceCall(MongoDBSpanStore store, String traceId) {
      this.store = store;
      this.traceId = traceId;
    }

    @Override protected List<Span> doExecute() throws IOException {
      try {
        return store.executeGetTrace(traceId);
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<Span>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<List<Span>> clone() {
      return new GetTraceCall(store, traceId);
    }

    @Override public String toString() {
      return "GetTrace{traceId=" + traceId + "}";
    }
  }

  static final class GetTracesByIdsCall extends Call.Base<List<List<Span>>> {
    final MongoDBSpanStore store;
    final Set<String> traceIds;

    GetTracesByIdsCall(MongoDBSpanStore store, Set<String> traceIds) {
      this.store = store;
      this.traceIds = traceIds;
    }

    @Override protected List<List<Span>> doExecute() throws IOException {
      try {
        return store.getTracesByIds(traceIds);
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<List<Span>>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<List<List<Span>>> clone() {
      return new GetTracesByIdsCall(store, traceIds);
    }

    @Override public String toString() {
      return "GetTracesByIds{traceIds=" + traceIds + "}";
    }
  }

  static final class GetServiceNamesCall extends Call.Base<List<String>> {
    final MongoDBSpanStore store;

    GetServiceNamesCall(MongoDBSpanStore store) {
      this.store = store;
    }

    @Override protected List<String> doExecute() throws IOException {
      try {
        return store.executeGetServiceNames();
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetServiceNamesCall(store);
    }

    @Override public String toString() {
      return "GetServiceNames{}";
    }
  }

  static final class GetRemoteServiceNamesCall extends Call.Base<List<String>> {
    final MongoDBSpanStore store;
    final String serviceName;

    GetRemoteServiceNamesCall(MongoDBSpanStore store, String serviceName) {
      this.store = store;
      this.serviceName = serviceName;
    }

    @Override protected List<String> doExecute() throws IOException {
      try {
        return store.executeGetRemoteServiceNames(serviceName);
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetRemoteServiceNamesCall(store, serviceName);
    }

    @Override public String toString() {
      return "GetRemoteServiceNames{serviceName=" + serviceName + "}";
    }
  }

  static final class GetSpanNamesCall extends Call.Base<List<String>> {
    final MongoDBSpanStore store;
    final String serviceName;

    GetSpanNamesCall(MongoDBSpanStore store, String serviceName) {
      this.store = store;
      this.serviceName = serviceName;
    }

    @Override protected List<String> doExecute() throws IOException {
      try {
        return store.executeGetSpanNames(serviceName);
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetSpanNamesCall(store, serviceName);
    }

    @Override public String toString() {
      return "GetSpanNames{serviceName=" + serviceName + "}";
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
        return store.executeGetDependencies(endTs, lookback);
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<DependencyLink>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<List<DependencyLink>> clone() {
      return new GetDependenciesCall(store, endTs, lookback);
    }

    @Override public String toString() {
      return "GetDependencies{endTs=" + endTs + ", lookback=" + lookback + "}";
    }
  }
}
