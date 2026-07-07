/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.internal.DependencyLinker;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanStore;
import zipkin2.storage.Traces;

import static java.nio.charset.StandardCharsets.UTF_8;
import static zipkin2.storage.redis.RedisStorageKeys.SERVICE_NAMES;
import static zipkin2.storage.redis.RedisStorageKeys.TRACES_BY_TIMESTAMP;
import static zipkin2.storage.redis.RedisStorageKeys.lowTraceId;
import static zipkin2.storage.redis.RedisStorageKeys.lowTraceIdFromMember;
import static zipkin2.storage.redis.RedisStorageKeys.remoteServiceNames;
import static zipkin2.storage.redis.RedisStorageKeys.serviceTraces;
import static zipkin2.storage.redis.RedisStorageKeys.spanNames;
import static zipkin2.storage.redis.RedisStorageKeys.trace;

final class RedisSpanStore implements SpanStore, Traces, ServiceAndSpanNames {
  final JedisPool pool;
  final boolean strictTraceId, searchEnabled;

  RedisSpanStore(JedisPool pool, boolean strictTraceId, boolean searchEnabled) {
    this.pool = pool;
    this.strictTraceId = strictTraceId;
    this.searchEnabled = searchEnabled;
  }

  @Override public Call<List<List<Span>>> getTraces(QueryRequest request) {
    if (!searchEnabled) return Call.emptyList();
    return new RedisCall<>(pool, jedis -> {
      Set<String> lowTraceIds = lowTraceIdsInRange(jedis,
        request.serviceName() != null ? serviceTraces(request.serviceName()) : TRACES_BY_TIMESTAMP,
        request.endTs(), request.lookback());
      if (lowTraceIds.isEmpty()) return List.of();

      List<List<Span>> result = new ArrayList<>();
      for (Iterator<String> i = lowTraceIds.iterator();
        i.hasNext() && result.size() < request.limit(); ) {
        List<Span> next = spansByTraceId(jedis, i.next());
        if (next.isEmpty() || !request.test(next)) continue;
        if (!strictTraceId) {
          result.add(next);
          continue;
        }
        for (List<Span> strictTrace : strictByTraceId(next)) {
          if (request.test(strictTrace)) result.add(strictTrace);
        }
      }
      return result;
    });
  }

  @Override public Call<List<Span>> getTrace(String traceId) {
    String normalized = Span.normalizeTraceId(traceId);
    return new RedisCall<>(pool, jedis -> {
      List<Span> spans = spansByTraceId(jedis, lowTraceId(normalized));
      if (spans.isEmpty()) return List.of();
      if (!strictTraceId) return spans;

      List<Span> filtered = new ArrayList<>(spans);
      filtered.removeIf(span -> !span.traceId().equals(normalized));
      return filtered;
    });
  }

  @Override public Call<List<List<Span>>> getTraces(Iterable<String> traceIds) {
    Set<String> normalized = new LinkedHashSet<>();
    for (String traceId : traceIds) {
      normalized.add(Span.normalizeTraceId(traceId));
    }
    Set<String> lower64Bit = new LinkedHashSet<>();
    for (String traceId : normalized) {
      lower64Bit.add(lowTraceId(traceId));
    }
    return new RedisCall<>(pool, jedis -> {
      List<List<Span>> result = new ArrayList<>();
      for (String lowTraceId : lower64Bit) {
        List<Span> sameTraceId = spansByTraceId(jedis, lowTraceId);
        if (sameTraceId.isEmpty()) continue;
        if (strictTraceId) {
          for (List<Span> trace : strictByTraceId(sameTraceId)) {
            if (normalized.contains(trace.get(0).traceId())) result.add(trace);
          }
        } else {
          result.add(sameTraceId);
        }
      }
      return result;
    });
  }

  @Override public Call<List<String>> getServiceNames() {
    if (!searchEnabled) return Call.emptyList();
    return new RedisCall<>(pool, jedis -> sortedMembers(jedis, SERVICE_NAMES));
  }

  @Override public Call<List<String>> getRemoteServiceNames(String serviceName) {
    if (serviceName.isEmpty() || !searchEnabled) return Call.emptyList();
    String service = serviceName.toLowerCase(Locale.ROOT);
    return new RedisCall<>(pool, jedis -> sortedMembers(jedis, remoteServiceNames(service)));
  }

  @Override public Call<List<String>> getSpanNames(String serviceName) {
    if (serviceName.isEmpty() || !searchEnabled) return Call.emptyList();
    String service = serviceName.toLowerCase(Locale.ROOT);
    return new RedisCall<>(pool, jedis -> sortedMembers(jedis, spanNames(service)));
  }

  @Override public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    if (endTs <= 0) throw new IllegalArgumentException("endTs <= 0");
    if (lookback <= 0) throw new IllegalArgumentException("lookback <= 0");
    return new RedisCall<>(pool, jedis -> {
      Set<String> lowTraceIds = lowTraceIdsInRange(jedis, TRACES_BY_TIMESTAMP, endTs, lookback);
      if (lowTraceIds.isEmpty()) return List.of();
      DependencyLinker linker = new DependencyLinker();
      for (String lowTraceId : lowTraceIds) {
        List<Span> spans = spansByTraceId(jedis, lowTraceId);
        if (!spans.isEmpty()) linker.putTrace(spans);
      }
      return linker.link();
    });
  }

  /** Distinct lowTraceIds ordered by descending timestamp within (endTs - lookback, endTs]. */
  static Set<String> lowTraceIdsInRange(Jedis jedis, String key, long endTs, long lookback) {
    long beginTs = endTs - lookback;
    List<String> members = jedis.zrevrangeByScore(key, endTs, beginTs);
    Set<String> result = new LinkedHashSet<>();
    for (String member : members) {
      result.add(lowTraceIdFromMember(member));
    }
    return result;
  }

  static List<Span> spansByTraceId(Jedis jedis, String lowTraceId) {
    Set<String> encoded = jedis.smembers(trace(lowTraceId));
    List<Span> spans = new ArrayList<>(encoded.size());
    for (String json : encoded) {
      Span span = SpanBytesDecoder.JSON_V2.decodeOne(json.getBytes(UTF_8));
      if (span != null) spans.add(span);
    }
    return spans;
  }

  static Collection<List<Span>> strictByTraceId(List<Span> next) {
    Map<String, List<Span>> groupedByTraceId = new LinkedHashMap<>();
    for (Span span : next) {
      groupedByTraceId.computeIfAbsent(span.traceId(), k -> new ArrayList<>()).add(span);
    }
    return groupedByTraceId.values();
  }

  static List<String> sortedMembers(Jedis jedis, String key) {
    return new ArrayList<>(new TreeSet<>(jedis.smembers(key)));
  }
}
