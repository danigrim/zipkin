/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;

/**
 * A {@link StorageComponent} that persists spans to <a href="https://redis.io">Redis</a>.
 *
 * <p>Spans are stored as v2 JSON in per-trace sets, indexed by sorted sets for time-based lookup
 * and by sets for service, span and remote service names. Data expires after {@link
 * Builder#dataTtl(long)} to bound memory use.
 */
public final class RedisStorage extends StorageComponent {
  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {
    boolean strictTraceId = true, searchEnabled = true;
    List<String> autocompleteKeys = new ArrayList<>();
    String host = "localhost";
    int port = 6379;
    long ttlSeconds = TimeUnit.DAYS.toSeconds(7);
    JedisPool pool;

    @Override public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    @Override public Builder searchEnabled(boolean searchEnabled) {
      this.searchEnabled = searchEnabled;
      return this;
    }

    @Override public Builder autocompleteKeys(List<String> autocompleteKeys) {
      if (autocompleteKeys == null) throw new NullPointerException("autocompleteKeys == null");
      this.autocompleteKeys = autocompleteKeys;
      return this;
    }

    /** The Redis host to connect to. Defaults to "localhost". Ignored when {@link #jedisPool} set. */
    public Builder host(String host) {
      if (host == null) throw new NullPointerException("host == null");
      this.host = host;
      return this;
    }

    /** The Redis port to connect to. Defaults to 6379. Ignored when {@link #jedisPool} set. */
    public Builder port(int port) {
      this.port = port;
      return this;
    }

    /** How long, in seconds, spans and indexes live before expiring. 0 disables expiry. */
    public Builder dataTtl(long ttlSeconds) {
      if (ttlSeconds < 0) throw new IllegalArgumentException("ttlSeconds < 0");
      this.ttlSeconds = ttlSeconds;
      return this;
    }

    /** Uses a pre-configured pool instead of connecting via {@link #host} and {@link #port}. */
    public Builder jedisPool(JedisPool pool) {
      if (pool == null) throw new NullPointerException("pool == null");
      this.pool = pool;
      return this;
    }

    @Override public RedisStorage build() {
      return new RedisStorage(this);
    }

    Builder() {
    }
  }

  final boolean strictTraceId, searchEnabled;
  final List<String> autocompleteKeys;
  final Set<String> autocompleteKeysSet;
  final long ttlSeconds;
  final String host;
  final int port;
  final boolean closePool;
  volatile JedisPool pool;

  RedisStorage(Builder builder) {
    this.strictTraceId = builder.strictTraceId;
    this.searchEnabled = builder.searchEnabled;
    this.autocompleteKeys = builder.autocompleteKeys;
    this.autocompleteKeysSet = new LinkedHashSet<>(builder.autocompleteKeys);
    this.ttlSeconds = builder.ttlSeconds;
    this.host = builder.host;
    this.port = builder.port;
    this.pool = builder.pool;
    this.closePool = builder.pool == null; // only close pools we created
  }

  /** Lazily creates the connection pool to avoid eager I/O. */
  JedisPool pool() {
    if (pool == null) {
      synchronized (this) {
        if (pool == null) {
          pool = new JedisPool(host, port);
        }
      }
    }
    return pool;
  }

  @Override public SpanStore spanStore() {
    return new RedisSpanStore(pool(), strictTraceId, searchEnabled);
  }

  @Override public Traces traces() {
    return new RedisSpanStore(pool(), strictTraceId, searchEnabled);
  }

  @Override public ServiceAndSpanNames serviceAndSpanNames() {
    return new RedisSpanStore(pool(), strictTraceId, searchEnabled);
  }

  @Override public AutocompleteTags autocompleteTags() {
    return new RedisAutocompleteTags(pool(), searchEnabled, autocompleteKeys);
  }

  @Override public SpanConsumer spanConsumer() {
    return new RedisSpanConsumer(pool(), searchEnabled, autocompleteKeysSet, ttlSeconds);
  }

  @Override public CheckResult check() {
    try (Jedis jedis = pool().getResource()) {
      jedis.ping();
    } catch (Throwable e) {
      Call.propagateIfFatal(e);
      return CheckResult.failed(e);
    }
    return CheckResult.OK;
  }

  /** Visible for testing: removes all data in the current Redis database. */
  void clear() {
    try (Jedis jedis = pool().getResource()) {
      jedis.flushDB();
    }
  }

  @Override public void close() {
    if (closePool && pool != null) pool.close();
  }

  @Override public String toString() {
    return "RedisStorage{host=" + host + ", port=" + port + "}";
  }
}
