/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.JedisPool;
import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;

import static zipkin2.storage.redis.RedisStorageKeys.autocompleteValues;

final class RedisAutocompleteTags implements AutocompleteTags {
  final JedisPool pool;
  final boolean searchEnabled;
  final List<String> autocompleteKeys;

  RedisAutocompleteTags(JedisPool pool, boolean searchEnabled, List<String> autocompleteKeys) {
    this.pool = pool;
    this.searchEnabled = searchEnabled;
    this.autocompleteKeys = autocompleteKeys;
  }

  @Override public Call<List<String>> getKeys() {
    if (!searchEnabled) return Call.emptyList();
    return Call.create(new ArrayList<>(autocompleteKeys));
  }

  @Override public Call<List<String>> getValues(String key) {
    if (key == null) throw new NullPointerException("key == null");
    if (key.isEmpty()) throw new IllegalArgumentException("key was empty");
    if (!searchEnabled) return Call.emptyList();
    return new RedisCall<>(pool, jedis -> RedisSpanStore.sortedMembers(jedis,
      autocompleteValues(key)));
  }
}
