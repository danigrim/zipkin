/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.storage.AutocompleteTags;

final class MongoDBAutocompleteTags implements AutocompleteTags {

  final MongoDBStorage storage;

  MongoDBAutocompleteTags(MongoDBStorage storage) {
    this.storage = storage;
  }

  @Override public Call<List<String>> getKeys() {
    if (storage.autocompleteKeys.isEmpty()) return Call.emptyList();
    return Call.create(List.copyOf(storage.autocompleteKeys));
  }

  @Override public Call<List<String>> getValues(String key) {
    if (key == null) throw new NullPointerException("key == null");
    if (key.isEmpty()) throw new IllegalArgumentException("key was empty");
    if (!storage.autocompleteKeys.contains(key)) return Call.emptyList();
    return new GetValuesCall(this, key);
  }

  List<String> executeGetValues(String key) {
    MongoCollection<Document> collection = storage.autocompleteTagsCollection();
    List<String> result = new ArrayList<>();
    try (MongoCursor<Document> cursor = collection.find(Filters.eq("key", key))
      .sort(Sorts.ascending("value"))
      .limit(storage.autocompleteCardinality)
      .iterator()) {
      while (cursor.hasNext()) {
        String value = cursor.next().getString("value");
        if (value != null) result.add(value);
      }
    }
    return result;
  }

  @Override public String toString() {
    return "MongoDBAutocompleteTags{" + storage + "}";
  }

  static final class GetValuesCall extends Call.Base<List<String>> {
    final MongoDBAutocompleteTags tags;
    final String key;

    GetValuesCall(MongoDBAutocompleteTags tags, String key) {
      this.tags = tags;
      this.key = key;
    }

    @Override protected List<String> doExecute() throws IOException {
      try {
        return tags.executeGetValues(key);
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
      return new GetValuesCall(tags, key);
    }

    @Override public String toString() {
      return "GetValues{key=" + key + "}";
    }
  }
}
