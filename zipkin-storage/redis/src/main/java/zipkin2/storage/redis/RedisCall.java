/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import zipkin2.Call;
import zipkin2.Callback;

/**
 * Adapts a blocking Redis operation to the async {@link Call} contract. The supplied function is
 * invoked with a pooled {@link Jedis} connection that is returned to the pool when the call
 * completes.
 */
final class RedisCall<V> extends Call.Base<V> {
  interface RedisFunction<V> {
    V apply(Jedis jedis);
  }

  final JedisPool pool;
  final RedisFunction<V> function;

  RedisCall(JedisPool pool, RedisFunction<V> function) {
    this.pool = pool;
    this.function = function;
  }

  @Override protected V doExecute() {
    try (Jedis jedis = pool.getResource()) {
      return function.apply(jedis);
    }
  }

  @Override protected void doEnqueue(Callback<V> callback) {
    try {
      callback.onSuccess(doExecute());
    } catch (Throwable t) {
      propagateIfFatal(t);
      callback.onError(t);
    }
  }

  @Override public Call<V> clone() {
    return new RedisCall<>(pool, function);
  }

  @Override public String toString() {
    return "RedisCall{" + function + "}";
  }
}
