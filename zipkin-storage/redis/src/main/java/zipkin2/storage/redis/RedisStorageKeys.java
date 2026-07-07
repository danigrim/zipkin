/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

/** Central definition of the Redis key layout used by {@link RedisStorage}. */
final class RedisStorageKeys {
  static final String PREFIX = "zipkin:";

  /** Set of json-encoded spans sharing the same lower 64-bits of a trace ID. */
  static String trace(String lowTraceId) {
    return PREFIX + "trace:" + lowTraceId;
  }

  /** Sorted set of {@code lowTraceId:timestamp} members scored by epoch millis. */
  static final String TRACES_BY_TIMESTAMP = PREFIX + "traces";

  /** Sorted set of {@code lowTraceId:timestamp} members for a given local service name. */
  static String serviceTraces(String serviceName) {
    return PREFIX + "service:" + serviceName + ":traces";
  }

  /** Set of all local service names. */
  static final String SERVICE_NAMES = PREFIX + "services";

  /** Set of span names recorded by a local service. */
  static String spanNames(String serviceName) {
    return PREFIX + "service:" + serviceName + ":spans";
  }

  /** Set of remote service names recorded by a local service. */
  static String remoteServiceNames(String serviceName) {
    return PREFIX + "service:" + serviceName + ":remote";
  }

  /** Set of values seen for an autocomplete tag key. */
  static String autocompleteValues(String key) {
    return PREFIX + "autocomplete:" + key;
  }

  /** The sorted-set member that indexes a trace at a given timestamp. */
  static String timestampMember(String lowTraceId, long timestampMillis) {
    return lowTraceId + ":" + timestampMillis;
  }

  /** Extracts the {@code lowTraceId} from a {@link #timestampMember(String, long)}. */
  static String lowTraceIdFromMember(String member) {
    int i = member.lastIndexOf(':');
    return i == -1 ? member : member.substring(0, i);
  }

  /** Returns the right-most 16 characters of a 128-bit trace ID, else the input. */
  static String lowTraceId(String traceId) {
    return traceId.length() == 32 ? traceId.substring(16) : traceId;
  }

  RedisStorageKeys() {
  }
}
