/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.bson.Document;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.storage.AutocompleteTags;

import static com.mongodb.client.model.Filters.eq;

class MongoDBAutocompleteTags implements AutocompleteTags {

  final MongoDBStorage storage;

  MongoDBAutocompleteTags(MongoDBStorage storage) {
    this.storage = storage;
  }

  MongoCollection<Document> spans() {
    return storage.database().getCollection(storage.spanCollection);
  }

  @Override public Call<List<String>> getKeys() {
    return new GetKeysCall(this);
  }

  @Override public Call<List<String>> getValues(String key) {
    if (!storage.autocompleteKeys.contains(key)) return Call.emptyList();
    return new GetValuesCall(this, key);
  }

  static final class GetKeysCall extends Call.Base<List<String>> {
    final MongoDBAutocompleteTags tags;

    GetKeysCall(MongoDBAutocompleteTags tags) {
      this.tags = tags;
    }

    @Override protected List<String> doExecute() throws IOException {
      try {
        Set<String> autocompleteKeys = tags.storage.autocompleteKeys;
        if (autocompleteKeys.isEmpty()) return List.of();

        List<String> result = new ArrayList<>();
        for (String key : autocompleteKeys) {
          Document found = tags.spans().find(eq("tags.key", key)).first();
          if (found != null) result.add(key);
        }
        Collections.sort(result);
        return result;
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetKeysCall(tags);
    }
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
        Set<String> values = new LinkedHashSet<>();
        List<Document> pipeline = Arrays.asList(
          new Document("$unwind", "$tags"),
          new Document("$match", new Document("tags.key", key)),
          new Document("$group", new Document("_id", "$tags.value"))
        );
        try (MongoCursor<Document> cursor = tags.spans().aggregate(pipeline).iterator()) {
          while (cursor.hasNext()) {
            String val = cursor.next().getString("_id");
            if (val != null) values.add(val);
          }
        }
        List<String> result = new ArrayList<>(values);
        Collections.sort(result);
        return result;
      } catch (RuntimeException e) {
        throw new IOException(e);
      }
    }

    @Override protected void doEnqueue(Callback<List<String>> callback) {
      try {
        callback.onSuccess(doExecute());
      } catch (IOException e) {
        callback.onError(e);
      }
    }

    @Override public Call<List<String>> clone() {
      return new GetValuesCall(tags, key);
    }
  }
}
