/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.storage.SpanConsumer;

import static zipkin2.storage.mongodb.MongoDBIndexCreator.AUTOCOMPLETE_COLLECTION;
import static zipkin2.storage.mongodb.MongoDBIndexCreator.SPANS_COLLECTION;

final class MongoDBSpanConsumer implements SpanConsumer {
  final MongoDBStorage storage;

  MongoDBSpanConsumer(MongoDBStorage storage) {
    this.storage = storage;
  }

  @Override public Call<Void> accept(List<Span> spans) {
    if (spans.isEmpty()) return Call.create(null);
    return new StoreSpansCall(this, spans);
  }

  void storeSpans(List<Span> spans) {
    MongoCollection<Document> collection = storage.db().getCollection(SPANS_COLLECTION);
    List<Document> docs = new ArrayList<>(spans.size());
    Set<Document> autocompleteDocs = new LinkedHashSet<>();

    for (Span span : spans) {
      docs.add(MongoDBSpanCodec.toDocument(span));
      if (!storage.autocompleteKeys.isEmpty() && !span.tags().isEmpty()) {
        for (Map.Entry<String, String> tag : span.tags().entrySet()) {
          if (storage.autocompleteKeys.contains(tag.getKey())) {
            autocompleteDocs.add(
              new Document("key", tag.getKey()).append("value", tag.getValue())
            );
          }
        }
      }
    }

    collection.insertMany(docs);

    if (!autocompleteDocs.isEmpty()) {
      MongoCollection<Document> autocomplete =
        storage.db().getCollection(AUTOCOMPLETE_COLLECTION);
      List<WriteModel<Document>> updates = new ArrayList<>(autocompleteDocs.size());
      for (Document tag : autocompleteDocs) {
        updates.add(new UpdateOneModel<>(
          tag,
          new Document("$set", tag),
          new UpdateOptions().upsert(true)
        ));
      }
      autocomplete.bulkWrite(updates);
    }
  }

  @Override public String toString() {
    return "MongoDBSpanConsumer{" + storage + "}";
  }

  static final class StoreSpansCall extends MongoDBCall<Void> {
    final MongoDBSpanConsumer consumer;
    final List<Span> spans;

    StoreSpansCall(MongoDBSpanConsumer consumer, List<Span> spans) {
      this.consumer = consumer;
      this.spans = spans;
    }

    @Override Void doExecute() {
      consumer.storeSpans(spans);
      return null;
    }

    @Override public Call<Void> clone() {
      return new StoreSpansCall(consumer, spans);
    }

    @Override public String toString() {
      return "StoreSpans{spans=" + spans.size() + "}";
    }
  }
}
