/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.internal.ClosedComponentException;
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
 * <p>Spans are stored as documents in a "spans" collection, with indexes on trace ID, service
 * name, span name, and timestamp for efficient querying. Dependencies are computed via aggregation
 * queries. Autocomplete tags are stored in a separate collection.
 *
 * <p>Schema uses a document-based model where each span is stored as a single document. TTL indexes
 * are used for automatic data expiration.
 */
public final class MongoDBStorage extends StorageComponent {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {
    String connectionString = "mongodb://localhost:27017";
    String database = "zipkin";
    boolean strictTraceId = true;
    boolean searchEnabled = true;
    Set<String> autocompleteKeys = Set.of();
    int autocompleteTtl = (int) TimeUnit.HOURS.toMillis(1);
    int autocompleteCardinality = 5 * 4000;
    boolean ensureSchema = true;
    int maxTraceCols = 100_000;
    int spanTtl = 7; // days
    @Nullable String username;
    @Nullable String password;

    Builder() {
    }

    /** Connection string for MongoDB. Defaults to "mongodb://localhost:27017" */
    public Builder connectionString(String connectionString) {
      if (connectionString == null) throw new NullPointerException("connectionString == null");
      this.connectionString = connectionString;
      return this;
    }

    /** Database to store span and index data. Defaults to "zipkin" */
    public Builder database(String database) {
      if (database == null) throw new NullPointerException("database == null");
      this.database = database;
      return this;
    }

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
      this.autocompleteKeys = Set.copyOf(keys);
      return this;
    }

    @Override public Builder autocompleteTtl(int autocompleteTtl) {
      if (autocompleteTtl <= 0) throw new IllegalArgumentException("autocompleteTtl <= 0");
      this.autocompleteTtl = autocompleteTtl;
      return this;
    }

    @Override public Builder autocompleteCardinality(int autocompleteCardinality) {
      if (autocompleteCardinality <= 0) {
        throw new IllegalArgumentException("autocompleteCardinality <= 0");
      }
      this.autocompleteCardinality = autocompleteCardinality;
      return this;
    }

    /** Whether to ensure indexes on startup. Defaults to true. */
    public Builder ensureSchema(boolean ensureSchema) {
      this.ensureSchema = ensureSchema;
      return this;
    }

    /**
     * Spans have multiple values for the same id. This defines a threshold which accommodates this
     * situation, without looking for an unbounded number of results.
     */
    public Builder maxTraceCols(int maxTraceCols) {
      if (maxTraceCols <= 0) throw new IllegalArgumentException("maxTraceCols <= 0");
      this.maxTraceCols = maxTraceCols;
      return this;
    }

    /** Number of days to keep span data. Defaults to 7. */
    public Builder spanTtl(int spanTtl) {
      if (spanTtl <= 0) throw new IllegalArgumentException("spanTtl <= 0");
      this.spanTtl = spanTtl;
      return this;
    }

    /** Username for authentication. No default. */
    public Builder username(@Nullable String username) {
      this.username = username;
      return this;
    }

    /** Password for authentication. No default. */
    public Builder password(@Nullable String password) {
      this.password = password;
      return this;
    }

    @Override public MongoDBStorage build() {
      return new MongoDBStorage(this);
    }
  }

  static final String COLLECTION_SPANS = "spans";
  static final String COLLECTION_DEPENDENCIES = "dependencies";
  static final String COLLECTION_AUTOCOMPLETE_TAGS = "autocomplete_tags";
  static final String COLLECTION_SERVICE_NAMES = "service_names";
  static final String COLLECTION_REMOTE_SERVICE_NAMES = "remote_service_names";
  static final String COLLECTION_SPAN_NAMES = "span_names";

  final boolean strictTraceId, searchEnabled;
  final Set<String> autocompleteKeys;
  final int autocompleteTtl, autocompleteCardinality;
  final String connectionString;
  final String databaseName;
  final boolean ensureSchema;
  final int maxTraceCols;
  final int spanTtl;

  final MongoClient client;
  final MongoDatabase db;

  volatile boolean closeCalled;
  volatile MongoDBSpanConsumer spanConsumer;
  volatile MongoDBSpanStore spanStore;
  volatile MongoDBAutocompleteTags tagStore;

  MongoDBStorage(Builder builder) {
    this.strictTraceId = builder.strictTraceId;
    this.searchEnabled = builder.searchEnabled;
    this.autocompleteKeys = builder.autocompleteKeys;
    this.autocompleteTtl = builder.autocompleteTtl;
    this.autocompleteCardinality = builder.autocompleteCardinality;
    this.connectionString = builder.connectionString;
    this.databaseName = builder.database;
    this.ensureSchema = builder.ensureSchema;
    this.maxTraceCols = builder.maxTraceCols;
    this.spanTtl = builder.spanTtl;
    MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(builder.connectionString));
    if (builder.username != null && builder.password != null) {
      settingsBuilder.credential(MongoCredential.createCredential(
        builder.username, builder.database, builder.password.toCharArray()));
    }
    this.client = MongoClients.create(settingsBuilder.build());
    this.db = client.getDatabase(builder.database);
    if (ensureSchema) ensureIndexes();
  }

  MongoCollection<Document> spansCollection() {
    return db.getCollection(COLLECTION_SPANS);
  }

  MongoCollection<Document> dependenciesCollection() {
    return db.getCollection(COLLECTION_DEPENDENCIES);
  }

  MongoCollection<Document> autocompleteTagsCollection() {
    return db.getCollection(COLLECTION_AUTOCOMPLETE_TAGS);
  }

  MongoCollection<Document> serviceNamesCollection() {
    return db.getCollection(COLLECTION_SERVICE_NAMES);
  }

  MongoCollection<Document> remoteServiceNamesCollection() {
    return db.getCollection(COLLECTION_REMOTE_SERVICE_NAMES);
  }

  MongoCollection<Document> spanNamesCollection() {
    return db.getCollection(COLLECTION_SPAN_NAMES);
  }

  void ensureIndexes() {
    MongoCollection<Document> spans = spansCollection();

    // Primary lookup by trace ID
    createIndexSafe(spans, Indexes.ascending("trace_id"), "trace_id_idx");

    // TTL index for automatic expiration
    createIndexSafe(spans, Indexes.ascending("ts"),
      new IndexOptions().name("ts_ttl_idx").expireAfter((long) spanTtl, TimeUnit.DAYS));

    if (searchEnabled) {
      // Compound index for service name + span name + timestamp queries
      createIndexSafe(spans, Indexes.compoundIndex(
        Indexes.ascending("local_service_name"),
        Indexes.ascending("name"),
        Indexes.descending("ts")
      ), "service_span_ts_idx");

      // Index for remote service name queries
      createIndexSafe(spans, Indexes.compoundIndex(
        Indexes.ascending("local_service_name"),
        Indexes.ascending("remote_service_name"),
        Indexes.descending("ts")
      ), "service_remote_ts_idx");

      // Index for duration queries
      createIndexSafe(spans, Indexes.compoundIndex(
        Indexes.ascending("local_service_name"),
        Indexes.ascending("duration")
      ), "service_duration_idx");

      // Index for annotation/tag queries
      createIndexSafe(spans, Indexes.ascending("annotations_query"), "annotations_query_idx");
    }

    // Service names collection indexes
    MongoCollection<Document> serviceNames = serviceNamesCollection();
    createIndexSafe(serviceNames, Indexes.ascending("service_name"),
      new IndexOptions().name("service_name_unique_idx").unique(true));

    // Remote service names
    MongoCollection<Document> remoteServiceNames = remoteServiceNamesCollection();
    createIndexSafe(remoteServiceNames, Indexes.compoundIndex(
      Indexes.ascending("service_name"),
      Indexes.ascending("remote_service_name")
    ), new IndexOptions().name("service_remote_unique_idx").unique(true));

    // Span names
    MongoCollection<Document> spanNames = spanNamesCollection();
    createIndexSafe(spanNames, Indexes.compoundIndex(
      Indexes.ascending("service_name"),
      Indexes.ascending("span_name")
    ), new IndexOptions().name("service_span_unique_idx").unique(true));

    // Dependencies collection indexes
    MongoCollection<Document> deps = dependenciesCollection();
    createIndexSafe(deps, Indexes.compoundIndex(
      Indexes.ascending("day"),
      Indexes.ascending("parent"),
      Indexes.ascending("child")
    ), new IndexOptions().name("day_parent_child_idx").unique(true));

    // Autocomplete tags
    MongoCollection<Document> autocompleteTags = autocompleteTagsCollection();
    createIndexSafe(autocompleteTags, Indexes.compoundIndex(
      Indexes.ascending("key"),
      Indexes.ascending("value")
    ), new IndexOptions().name("key_value_unique_idx").unique(true));
  }

  static void createIndexSafe(MongoCollection<Document> collection,
    org.bson.conversions.Bson keys, String name) {
    createIndexSafe(collection, keys, new IndexOptions().name(name));
  }

  static void createIndexSafe(MongoCollection<Document> collection,
    org.bson.conversions.Bson keys, IndexOptions options) {
    try {
      collection.createIndex(keys, options);
    } catch (MongoCommandException e) {
      // Ignore if index already exists with different options
      if (e.getErrorCode() != 85 && e.getErrorCode() != 86) throw e;
    }
  }

  @Override public SpanStore spanStore() {
    if (spanStore == null) {
      synchronized (this) {
        if (spanStore == null) {
          spanStore = new MongoDBSpanStore(this);
        }
      }
    }
    return spanStore;
  }

  @Override public Traces traces() {
    return (Traces) spanStore();
  }

  @Override public ServiceAndSpanNames serviceAndSpanNames() {
    return (ServiceAndSpanNames) spanStore();
  }

  @Override public AutocompleteTags autocompleteTags() {
    if (tagStore == null) {
      synchronized (this) {
        if (tagStore == null) {
          tagStore = new MongoDBAutocompleteTags(this);
        }
      }
    }
    return tagStore;
  }

  @Override public SpanConsumer spanConsumer() {
    if (spanConsumer == null) {
      synchronized (this) {
        if (spanConsumer == null) {
          spanConsumer = new MongoDBSpanConsumer(this);
        }
      }
    }
    return spanConsumer;
  }

  @Override public CheckResult check() {
    if (closeCalled) throw new ClosedComponentException();
    try {
      db.runCommand(new Document("ping", 1));
    } catch (Throwable e) {
      Call.propagateIfFatal(e);
      return CheckResult.failed(e);
    }
    return CheckResult.OK;
  }

  @Override public boolean isOverCapacity(Throwable e) {
    return super.isOverCapacity(e);
  }

  @Override public String toString() {
    return "MongoDBStorage{connectionString=" + connectionString
      + ", database=" + databaseName + "}";
  }

  @Override public void close() {
    if (closeCalled) return;
    closeCalled = true;
    client.close();
  }

  /** Drops all collections. Used in tests. */
  void clear() {
    List<String> collections = new ArrayList<>();
    db.listCollectionNames().into(collections);
    for (String name : collections) {
      db.getCollection(name).drop();
    }
    if (ensureSchema) ensureIndexes();
  }
}
