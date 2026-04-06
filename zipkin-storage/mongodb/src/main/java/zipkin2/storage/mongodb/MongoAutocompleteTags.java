/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import org.bson.Document;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.storage.AutocompleteTags;

import static com.mongodb.client.model.Filters.eq;

final class MongoAutocompleteTags implements AutocompleteTags {
  final boolean enabled;
  final LinkedHashSet<String> autocompleteKeys;
  final Call<List<String>> keysCall;
  final MongoStorage storage;

  MongoAutocompleteTags(MongoStorage storage) {
    this.storage = storage;
    this.enabled = storage.searchEnabled && !storage.autocompleteKeys.isEmpty();
    this.autocompleteKeys = new LinkedHashSet<>(storage.autocompleteKeys);
    this.keysCall = Call.create(storage.autocompleteKeys);
  }

  @Override public Call<List<String>> getKeys() {
    if (!enabled) return Call.emptyList();
    return keysCall.clone();
  }

  @Override public Call<List<String>> getValues(String key) {
    if (key == null) throw new NullPointerException("key == null");
    if (key.isEmpty()) throw new IllegalArgumentException("key was empty");
    if (!enabled || !autocompleteKeys.contains(key)) return Call.emptyList();
    return new GetValuesCall(storage, key);
  }

  static final class GetValuesCall extends Call.Base<List<String>> {
    final MongoStorage storage;
    final String key;

    GetValuesCall(MongoStorage storage, String key) {
      this.storage = storage;
      this.key = key;
    }

    @Override protected List<String> doExecute() throws IOException {
      MongoCollection<Document> col = storage.autocompleteTagsCollection();
      List<String> result = new ArrayList<>();
      try (MongoCursor<String> cursor =
        col.distinct("value", String.class).filter(eq("key", key)).iterator()) {
        while (cursor.hasNext()) {
          String value = cursor.next();
          if (value != null && !value.isEmpty()) result.add(value);
        }
      }
      result.sort(String::compareTo);
      return result;
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetValuesCall(storage, key);
    }
  }
}
