/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.List;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.storage.SpanConsumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static zipkin2.storage.redis.RedisStorageKeys.SERVICE_NAMES;
import static zipkin2.storage.redis.RedisStorageKeys.TRACES_BY_TIMESTAMP;
import static zipkin2.storage.redis.RedisStorageKeys.autocompleteValues;
import static zipkin2.storage.redis.RedisStorageKeys.lowTraceId;
import static zipkin2.storage.redis.RedisStorageKeys.remoteServiceNames;
import static zipkin2.storage.redis.RedisStorageKeys.serviceTraces;
import static zipkin2.storage.redis.RedisStorageKeys.spanNames;
import static zipkin2.storage.redis.RedisStorageKeys.timestampMember;
import static zipkin2.storage.redis.RedisStorageKeys.trace;

final class RedisSpanConsumer implements SpanConsumer {
  final JedisPool pool;
  final boolean searchEnabled;
  final Set<String> autocompleteKeys;
  final long ttlSeconds;

  RedisSpanConsumer(JedisPool pool, boolean searchEnabled, Set<String> autocompleteKeys,
    long ttlSeconds) {
    this.pool = pool;
    this.searchEnabled = searchEnabled;
    this.autocompleteKeys = autocompleteKeys;
    this.ttlSeconds = ttlSeconds;
  }

  @Override public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    return new RedisCall<>(pool, jedis -> {
      Pipeline pipeline = jedis.pipelined();
      for (Span span : spans) {
        store(pipeline, span);
      }
      pipeline.sync();
      return null;
    });
  }

  void store(Pipeline pipeline, Span span) {
    String lowTraceId = lowTraceId(span.traceId());
    long timestampMillis = span.timestampAsLong() / 1000L;
    String member = timestampMember(lowTraceId, timestampMillis);
    String json = new String(SpanBytesEncoder.JSON_V2.encode(span), UTF_8);

    String traceKey = trace(lowTraceId);
    pipeline.sadd(traceKey, json);
    expire(pipeline, traceKey);

    pipeline.zadd(TRACES_BY_TIMESTAMP, timestampMillis, member);
    expire(pipeline, TRACES_BY_TIMESTAMP);

    if (!searchEnabled) return;

    String serviceName = span.localServiceName();
    if (serviceName != null) {
      pipeline.sadd(SERVICE_NAMES, serviceName);
      expire(pipeline, SERVICE_NAMES);

      String serviceTracesKey = serviceTraces(serviceName);
      pipeline.zadd(serviceTracesKey, timestampMillis, member);
      expire(pipeline, serviceTracesKey);

      String remoteServiceName = span.remoteServiceName();
      if (remoteServiceName != null) {
        String remoteKey = remoteServiceNames(serviceName);
        pipeline.sadd(remoteKey, remoteServiceName);
        expire(pipeline, remoteKey);
      }

      String spanName = span.name();
      if (spanName != null) {
        String spanNamesKey = spanNames(serviceName);
        pipeline.sadd(spanNamesKey, spanName);
        expire(pipeline, spanNamesKey);
      }
    }

    for (Map.Entry<String, String> tag : span.tags().entrySet()) {
      if (autocompleteKeys.contains(tag.getKey())) {
        String key = autocompleteValues(tag.getKey());
        pipeline.sadd(key, tag.getValue());
        expire(pipeline, key);
      }
    }
  }

  void expire(Pipeline pipeline, String key) {
    if (ttlSeconds > 0) pipeline.expire(key, ttlSeconds);
  }
}
