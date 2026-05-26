/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
 * MongoDB implementation of Zipkin storage.
 *
 * <p>Spans are stored in the collection configured by {@link Builder#spanCollection(String)}
 * (defaults to {@code "span"}) inside the database {@link Builder#database(String)}
 * (defaults to {@code "zipkin"}).
 *
 * <p>When {@link StorageComponent.Builder#searchEnabled(boolean)} is false, only retrieval by
 * trace ID via {@link Traces} is supported. All index-backed queries ({@link SpanStore#getTraces},
 * service/span name listing) return empty results.
 *
 * <p>When {@link StorageComponent.Builder#strictTraceId(boolean)} is disabled, only the
 * right-most 16 hex characters of a trace ID are used for grouping and lookup. This is useful
 * during a migration from 64-bit to 128-bit trace IDs.
 *
 * <p>TTL is managed by a MongoDB TTL index on the {@code timestamp_millis} field; the default
 * retention period is 7 days.
 */
public final class MongoDBStorage extends StorageComponent {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {

    String connectionString = "mongodb://localhost:27017";
    String database = "zipkin";
    String spanCollection = "span";
    boolean ensureIndexes = true;
    boolean strictTraceId = true;
    boolean searchEnabled = true;
    Set<String> autocompleteKeys = Set.of();
    int autocompleteTtl = (int) TimeUnit.HOURS.toMillis(1);
    int autocompleteCardinality = 5 * 4000; // e.g. 5 site tags x cardinality 4000 each
    /** Retention period for span documents, in seconds. Defaults to 7 days. */
    int ttlSeconds = (int) TimeUnit.DAYS.toSeconds(7);
    int maxConnections = 10;

    Builder() {
    }

    /**
     * MongoDB connection string. Defaults to {@code "mongodb://localhost:27017"}.
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code mongodb://user:pass@host:27017/zipkin}</li>
     *   <li>{@code mongodb+srv://cluster.example.com/zipkin}</li>
     * </ul>
     */
    public Builder connectionString(String connectionString) {
      if (connectionString == null) throw new NullPointerException("connectionString == null");
      this.connectionString = connectionString;
      return this;
    }

    /** MongoDB database name. Defaults to {@code "zipkin"}. */
    public Builder database(String database) {
      if (database == null) throw new NullPointerException("database == null");
      this.database = database;
      return this;
    }

    /** Collection used to store span documents. Defaults to {@code "span"}. */
    public Builder spanCollection(String spanCollection) {
      if (spanCollection == null) throw new NullPointerException("spanCollection == null");
      this.spanCollection = spanCollection;
      return this;
    }

    /**
     * When true (default), creates the required indexes on startup if they do not already exist.
     * Set to false if you manage indexes externally.
     */
    public Builder ensureIndexes(boolean ensureIndexes) {
      this.ensureIndexes = ensureIndexes;
      return this;
    }

    /**
     * Document TTL in seconds. Spans older than this value are automatically removed by MongoDB.
     * Defaults to 604800 (7 days).
     */
    public Builder ttlSeconds(int ttlSeconds) {
      if (ttlSeconds <= 0) throw new IllegalArgumentException("ttlSeconds <= 0");
      this.ttlSeconds = ttlSeconds;
      return this;
    }

    /** Maximum number of connections in the MongoDB connection pool. Defaults to 10. */
    public Builder maxConnections(int maxConnections) {
      if (maxConnections <= 0) throw new IllegalArgumentException("maxConnections <= 0");
      this.maxConnections = maxConnections;
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

    @Override public MongoDBStorage build() {
      return new MongoDBStorage(this);
    }
  }

  // --- Configuration fields (set once in constructor) ---

  final String connectionString;
  final String database;
  final String spanCollection;
  final boolean ensureIndexes;
  final boolean strictTraceId;
  final boolean searchEnabled;
  final Set<String> autocompleteKeys;
  final int autocompleteTtl;
  final int autocompleteCardinality;
  final int ttlSeconds;
  final int maxConnections;

  // --- Mutable state ---

  /** Signals that {@link #close()} was called. */
  volatile boolean closeCalled;

  /** Lazily initialized on first use; guarded by {@code this}. */
  @Nullable volatile MongoClient mongoClient;

  // Memoized storage sub-components
  volatile MongoDBSpanConsumer spanConsumer;
  volatile MongoDBSpanStore spanStore;
  volatile MongoDBAutocompleteTags tagStore;

  MongoDBStorage(Builder builder) {
    this.connectionString = builder.connectionString;
    this.database = builder.database;
    this.spanCollection = builder.spanCollection;
    this.ensureIndexes = builder.ensureIndexes;
    this.strictTraceId = builder.strictTraceId;
    this.searchEnabled = builder.searchEnabled;
    this.autocompleteKeys = builder.autocompleteKeys;
    this.autocompleteTtl = builder.autocompleteTtl;
    this.autocompleteCardinality = builder.autocompleteCardinality;
    this.ttlSeconds = builder.ttlSeconds;
    this.maxConnections = builder.maxConnections;
  }

  // ---------------------------------------------------------------------------
  // Lazy MongoClient initialisation
  // ---------------------------------------------------------------------------

  /**
   * Returns the shared {@link MongoClient}, creating it on first call.
   *
   * <p>The client is created lazily so that connection errors surface at query time rather than
   * at bean-wiring time, matching the behaviour of the Cassandra and Elasticsearch storages.
   */
  MongoClient mongoClient() {
    if (closeCalled) throw new ClosedComponentException();
    MongoClient result = mongoClient;
    if (result == null) {
      synchronized (this) {
        result = mongoClient;
        if (result == null) {
          result = buildMongoClient();
          if (ensureIndexes) MongoDBSchema.ensureIndexes(result.getDatabase(database), spanCollection, ttlSeconds);
          mongoClient = result;
        }
      }
    }
    return result;
  }

  private MongoClient buildMongoClient() {
    MongoClientSettings settings = MongoClientSettings.builder()
      .applyConnectionString(new com.mongodb.ConnectionString(connectionString))
      .applyToConnectionPoolSettings(b -> b.maxSize(maxConnections))
      .build();
    return MongoClients.create(settings);
  }

  /** Returns the configured database from the shared client. */
  MongoDatabase database() {
    return mongoClient().getDatabase(database);
  }

  // ---------------------------------------------------------------------------
  // StorageComponent interface
  // ---------------------------------------------------------------------------

  /** {@inheritDoc} Memoized to avoid re-creating the span store on every call. */
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

  /** {@inheritDoc} Memoized to avoid re-creating the consumer on every call. */
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

  // ---------------------------------------------------------------------------
  // Health check
  // ---------------------------------------------------------------------------

  @Override public CheckResult check() {
    if (closeCalled) throw new ClosedComponentException();
    try {
      // "ping" command: works even without any collection or data
      mongoClient().getDatabase(database)
        .runCommand(new org.bson.Document("ping", 1));
    } catch (MongoException e) {
      return CheckResult.failed(e);
    }
    return CheckResult.OK;
  }

  // ---------------------------------------------------------------------------
  // Over-capacity detection
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} when the error indicates the MongoDB driver rejected the operation due to
   * a full connection pool ({@link com.mongodb.MongoTimeoutException}) or an explicit resource
   * exhaustion signal.
   */
  @Override public boolean isOverCapacity(Throwable e) {
    return e instanceof com.mongodb.MongoTimeoutException
      || super.isOverCapacity(e);
  }

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------

  @Override public void close() {
    if (closeCalled) return;
    closeCalled = true;
    MongoClient client = mongoClient;
    if (client != null) client.close();
  }

  @Override public String toString() {
    return "MongoDBStorage{connectionString=" + connectionString
      + ", database=" + database
      + ", collection=" + spanCollection + "}";
  }
}
