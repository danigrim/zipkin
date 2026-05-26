/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoCollection;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.bson.Document;
import zipkin2.Call;
import zipkin2.storage.AutocompleteTags;

import static com.mongodb.client.model.Filters.eq;
import static zipkin2.storage.mongodb.MongoDBIndexCreator.AUTOCOMPLETE_COLLECTION;

final class MongoDBAutocompleteTags implements AutocompleteTags {
  final MongoDBStorage storage;

  MongoDBAutocompleteTags(MongoDBStorage storage) {
    this.storage = storage;
  }

  @Override public Call<List<String>> getKeys() {
    if (storage.autocompleteKeys.isEmpty()) return Call.emptyList();
    return new GetKeysCall(this);
  }

  @Override public Call<List<String>> getValues(String key) {
    if (key == null) throw new IllegalArgumentException("key was null");
    if (key.isEmpty()) throw new IllegalArgumentException("key was empty");
    if (storage.autocompleteKeys.isEmpty() || !storage.autocompleteKeys.contains(key)) {
      return Call.emptyList();
    }
    return new GetValuesCall(this, key);
  }

  List<String> queryKeys() {
    MongoCollection<Document> collection =
      storage.db().getCollection(AUTOCOMPLETE_COLLECTION);
    TreeSet<String> keys = new TreeSet<>();
    for (String key : collection.distinct("key", String.class)) {
      if (storage.autocompleteKeys.contains(key)) keys.add(key);
    }
    return new ArrayList<>(keys);
  }

  List<String> queryValues(String key) {
    MongoCollection<Document> collection =
      storage.db().getCollection(AUTOCOMPLETE_COLLECTION);
    TreeSet<String> values = new TreeSet<>();
    for (String value : collection.distinct("value", eq("key", key), String.class)) {
      if (value != null && !value.isEmpty()) values.add(value);
    }
    return new ArrayList<>(values);
  }

  @Override public String toString() {
    return "MongoDBAutocompleteTags{" + storage + "}";
  }

  static final class GetKeysCall extends MongoDBCall<List<String>> {
    final MongoDBAutocompleteTags tags;

    GetKeysCall(MongoDBAutocompleteTags tags) {
      this.tags = tags;
    }

    @Override List<String> doExecute() {
      return tags.queryKeys();
    }

    @Override public Call<List<String>> clone() {
      return new GetKeysCall(tags);
    }

    @Override public String toString() {
      return "GetKeys{}";
    }
  }

  static final class GetValuesCall extends MongoDBCall<List<String>> {
    final MongoDBAutocompleteTags tags;
    final String key;

    GetValuesCall(MongoDBAutocompleteTags tags, String key) {
      this.tags = tags;
      this.key = key;
    }

    @Override List<String> doExecute() {
      return tags.queryValues(key);
    }

    @Override public Call<List<String>> clone() {
      return new GetValuesCall(tags, key);
    }

    @Override public String toString() {
      return "GetValues{key=" + key + "}";
    }
  }
}
