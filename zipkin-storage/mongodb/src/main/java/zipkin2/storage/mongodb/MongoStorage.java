/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.internal.Nullable;
import zipkin2.storage.AutocompleteTags;
import zipkin2.storage.ServiceAndSpanNames;
import zipkin2.storage.SpanConsumer;
import zipkin2.storage.SpanStore;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.Traces;

/**
 * MongoDB implementation of zipkin storage.
 *
 * <p>Spans are stored as documents in the "spans" collection, dependencies in "dependencies",
 * and autocomplete tags in "autocomplete_tags".
 *
 * <p>Schema is installed on first access via {@link #ensureSchema()}.
 */
public final class MongoStorage extends StorageComponent {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {
    boolean strictTraceId = true;
    boolean searchEnabled = true;
    List<String> autocompleteKeys = List.of();
    int autocompleteTtl = (int) TimeUnit.HOURS.toMillis(1);
    int autocompleteCardinality = 5 * 4000;
    String host = "localhost";
    int port = 27017;
    String database = "zipkin";
    @Nullable String username;
    @Nullable String password;

    @Override public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    @Override public Builder searchEnabled(boolean searchEnabled) {
      this.searchEnabled = searchEnabled;
      return this;
    }

    @Override public Builder autocompleteKeys(List<String> keys) {
      if (keys == null) throw new NullPointerException("keys == null");
      this.autocompleteKeys = keys;
      return this;
    }

    @Override public Builder autocompleteTtl(int autocompleteTtl) {
      this.autocompleteTtl = autocompleteTtl;
      return this;
    }

    @Override public Builder autocompleteCardinality(int autocompleteCardinality) {
      this.autocompleteCardinality = autocompleteCardinality;
      return this;
    }

    /** Defaults to localhost. */
    public Builder host(String host) {
      if (host == null) throw new NullPointerException("host == null");
      this.host = host;
      return this;
    }

    /** Defaults to 27017. */
    public Builder port(int port) {
      this.port = port;
      return this;
    }

    /** Defaults to "zipkin". */
    public Builder database(String database) {
      if (database == null) throw new NullPointerException("database == null");
      this.database = database;
      return this;
    }

    /** Optional username for authentication. */
    public Builder username(@Nullable String username) {
      this.username = username;
      return this;
    }

    /** Optional password for authentication. */
    public Builder password(@Nullable String password) {
      this.password = password;
      return this;
    }

    @Override public MongoStorage build() {
      return new MongoStorage(this);
    }

    Builder() {
    }
  }

  static final String COLLECTION_SPANS = "spans";
  static final String COLLECTION_DEPENDENCIES = "dependencies";
  static final String COLLECTION_AUTOCOMPLETE_TAGS = "autocomplete_tags";

  final boolean strictTraceId, searchEnabled;
  final List<String> autocompleteKeys;
  final int autocompleteTtl, autocompleteCardinality;
  final String host;
  final int port;
  final String database;
  @Nullable final String username;
  @Nullable final String password;

  volatile MongoClient lazyClient;
  volatile boolean ensuredSchema;
  volatile MongoSpanStore lazySpanStore;
  volatile MongoSpanConsumer lazySpanConsumer;
  volatile MongoAutocompleteTags lazyAutocompleteTags;

  /** close is typically called from a different thread */
  volatile boolean closeCalled;

  MongoStorage(Builder builder) {
    this.strictTraceId = builder.strictTraceId;
    this.searchEnabled = builder.searchEnabled;
    this.autocompleteKeys = builder.autocompleteKeys;
    this.autocompleteTtl = builder.autocompleteTtl;
    this.autocompleteCardinality = builder.autocompleteCardinality;
    this.host = builder.host;
    this.port = builder.port;
    this.database = builder.database;
    this.username = builder.username;
    this.password = builder.password;
  }

  /** Lazy initializes or returns the MongoDB client. */
  MongoClient client() {
    if (lazyClient == null) {
      synchronized (this) {
        if (lazyClient == null) {
          MongoClientSettings.Builder settings = MongoClientSettings.builder()
            .applyToClusterSettings(cluster ->
              cluster.hosts(List.of(new ServerAddress(host, port))));
          if (username != null && password != null) {
            settings.credential(
              com.mongodb.MongoCredential.createCredential(username, database, password.toCharArray()));
          }
          lazyClient = MongoClients.create(settings.build());
        }
      }
    }
    return lazyClient;
  }

  MongoDatabase db() {
    return client().getDatabase(database);
  }

  MongoCollection<Document> spans() {
    return db().getCollection(COLLECTION_SPANS);
  }

  MongoCollection<Document> dependencies() {
    return db().getCollection(COLLECTION_DEPENDENCIES);
  }

  MongoCollection<Document> autocompleteTagsCollection() {
    return db().getCollection(COLLECTION_AUTOCOMPLETE_TAGS);
  }

  void ensureSchema() {
    if (ensuredSchema) return;
    synchronized (this) {
      if (ensuredSchema) return;
      doEnsureSchema();
      ensuredSchema = true;
    }
  }

  void doEnsureSchema() {
    // Spans collection indexes
    MongoCollection<Document> spansCol = spans();
    createIndexQuietly(spansCol, Indexes.ascending("trace_id"), new IndexOptions().name("trace_id"));
    createIndexQuietly(spansCol, Indexes.compoundIndex(
      Indexes.ascending("trace_id"), Indexes.ascending("id")),
      new IndexOptions().name("trace_id_id"));
    createIndexQuietly(spansCol, Indexes.ascending("timestamp_millis"),
      new IndexOptions().name("timestamp_millis"));
    createIndexQuietly(spansCol, Indexes.ascending("local_endpoint_service_name"),
      new IndexOptions().name("local_endpoint_service_name"));
    createIndexQuietly(spansCol, Indexes.ascending("remote_endpoint_service_name"),
      new IndexOptions().name("remote_endpoint_service_name"));
    createIndexQuietly(spansCol, Indexes.ascending("name"),
      new IndexOptions().name("span_name"));
    createIndexQuietly(spansCol, Indexes.ascending("annotations_query"),
      new IndexOptions().name("annotations_query"));
    createIndexQuietly(spansCol, Indexes.ascending("duration"),
      new IndexOptions().name("duration"));

    // Dependencies collection indexes
    MongoCollection<Document> depsCol = dependencies();
    createIndexQuietly(depsCol, Indexes.ascending("day_millis"),
      new IndexOptions().name("day_millis"));
    createIndexQuietly(depsCol, Indexes.compoundIndex(
      Indexes.ascending("parent"), Indexes.ascending("child")),
      new IndexOptions().name("parent_child"));

    // Autocomplete tags collection indexes
    MongoCollection<Document> tagsCol = autocompleteTagsCollection();
    createIndexQuietly(tagsCol, Indexes.compoundIndex(
      Indexes.ascending("key"), Indexes.ascending("value")),
      new IndexOptions().name("key_value").unique(true));
  }

  static void createIndexQuietly(MongoCollection<Document> collection,
    org.bson.conversions.Bson keys, IndexOptions options) {
    try {
      collection.createIndex(keys, options);
    } catch (MongoCommandException e) {
      // Ignore duplicate index errors - index already exists
      if (e.getErrorCode() != 85 && e.getErrorCode() != 86) {
        throw e;
      }
    }
  }

  MongoSpanStore mongoSpanStore() {
    MongoSpanStore result = lazySpanStore;
    if (result == null) {
      synchronized (this) {
        result = lazySpanStore;
        if (result == null) {
          ensureSchema();
          lazySpanStore = result = new MongoSpanStore(this);
        }
      }
    }
    return result;
  }

  @Override public SpanStore spanStore() {
    return mongoSpanStore();
  }

  @Override public Traces traces() {
    return mongoSpanStore();
  }

  @Override public ServiceAndSpanNames serviceAndSpanNames() {
    return mongoSpanStore();
  }

  @Override public AutocompleteTags autocompleteTags() {
    MongoAutocompleteTags result = lazyAutocompleteTags;
    if (result == null) {
      synchronized (this) {
        result = lazyAutocompleteTags;
        if (result == null) {
          ensureSchema();
          lazyAutocompleteTags = result = new MongoAutocompleteTags(this);
        }
      }
    }
    return result;
  }

  @Override public SpanConsumer spanConsumer() {
    MongoSpanConsumer result = lazySpanConsumer;
    if (result == null) {
      synchronized (this) {
        result = lazySpanConsumer;
        if (result == null) {
          ensureSchema();
          lazySpanConsumer = result = new MongoSpanConsumer(this);
        }
      }
    }
    return result;
  }

  @Override public CheckResult check() {
    try {
      client().getDatabase(database).runCommand(new Document("ping", 1));
    } catch (Throwable e) {
      Call.propagateIfFatal(e);
      return CheckResult.failed(e);
    }
    return CheckResult.OK;
  }

  @Override public String toString() {
    return "MongoStorage{host=" + host + ", port=" + port + ", database=" + database + "}";
  }

  @Override public void close() {
    if (closeCalled) return;
    closeCalled = true;
    MongoClient client = lazyClient;
    if (client != null) client.close();
  }

  /** Visible for testing */
  void clear() {
    spans().drop();
    dependencies().drop();
    autocompleteTagsCollection().drop();
    ensuredSchema = false;
    lazySpanStore = null;
    lazySpanConsumer = null;
    lazyAutocompleteTags = null;
    ensureSchema();
  }
}
