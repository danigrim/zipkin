/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.LinkedHashSet;
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

public final class MongoDBStorage extends StorageComponent {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder extends StorageComponent.Builder {
    String connectionString = "mongodb://localhost:27017";
    String database = "zipkin";
    boolean strictTraceId = true;
    boolean searchEnabled = true;
    List<String> autocompleteKeys = List.of();
    int autocompleteTtl = (int) TimeUnit.HOURS.toMillis(1);
    int autocompleteCardinality = 5 * 4000;
    boolean ensureSchema = true;
    int maxConnections = 10;

    Builder() {
    }

    /** Connection string for MongoDB. Defaults to "mongodb://localhost:27017". */
    public Builder connectionString(String connectionString) {
      if (connectionString == null) throw new NullPointerException("connectionString == null");
      this.connectionString = connectionString;
      return this;
    }

    /** Database name to use. Defaults to "zipkin". */
    public Builder database(String database) {
      if (database == null) throw new NullPointerException("database == null");
      this.database = database;
      return this;
    }

    /** {@inheritDoc} */
    @Override public Builder strictTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
      return this;
    }

    /** {@inheritDoc} */
    @Override public Builder searchEnabled(boolean searchEnabled) {
      this.searchEnabled = searchEnabled;
      return this;
    }

    /** {@inheritDoc} */
    @Override public Builder autocompleteKeys(List<String> keys) {
      if (keys == null) throw new NullPointerException("keys == null");
      this.autocompleteKeys = keys;
      return this;
    }

    /** {@inheritDoc} */
    @Override public Builder autocompleteTtl(int autocompleteTtl) {
      this.autocompleteTtl = autocompleteTtl;
      return this;
    }

    /** {@inheritDoc} */
    @Override public Builder autocompleteCardinality(int autocompleteCardinality) {
      this.autocompleteCardinality = autocompleteCardinality;
      return this;
    }

    /** Whether to ensure indexes on startup. Defaults to true. */
    public Builder ensureSchema(boolean ensureSchema) {
      this.ensureSchema = ensureSchema;
      return this;
    }

    /** Maximum number of connections in the pool. Defaults to 10. */
    public Builder maxConnections(int maxConnections) {
      this.maxConnections = maxConnections;
      return this;
    }

    @Override public MongoDBStorage build() {
      return new MongoDBStorage(this);
    }
  }

  final boolean strictTraceId, searchEnabled;
  final Set<String> autocompleteKeys;
  final int autocompleteTtl, autocompleteCardinality;
  final String connectionString, database;
  final boolean ensureSchema;
  final int maxConnections;

  volatile boolean closeCalled;
  volatile MongoClient mongoClient;
  volatile MongoDatabase mongoDatabase;
  volatile MongoDBSpanConsumer spanConsumer;
  volatile MongoDBSpanStore spanStore;
  volatile MongoDBAutocompleteTags tagStore;

  MongoDBStorage(Builder builder) {
    this.strictTraceId = builder.strictTraceId;
    this.searchEnabled = builder.searchEnabled;
    this.autocompleteKeys = new LinkedHashSet<>(builder.autocompleteKeys);
    this.autocompleteTtl = builder.autocompleteTtl;
    this.autocompleteCardinality = builder.autocompleteCardinality;
    this.connectionString = builder.connectionString;
    this.database = builder.database;
    this.ensureSchema = builder.ensureSchema;
    this.maxConnections = builder.maxConnections;
  }

  MongoDatabase db() {
    if (mongoDatabase == null) {
      synchronized (this) {
        if (mongoDatabase == null) {
          mongoClient = buildClient();
          mongoDatabase = mongoClient.getDatabase(database);
          if (ensureSchema) MongoDBIndexCreator.ensureIndexes(mongoDatabase);
        }
      }
    }
    return mongoDatabase;
  }

  MongoClient buildClient() {
    MongoClientSettings settings = MongoClientSettings.builder()
      .applyConnectionString(new ConnectionString(connectionString))
      .applyToConnectionPoolSettings(pool ->
        pool.maxSize(maxConnections)
      )
      .build();
    return MongoClients.create(settings);
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
      db().runCommand(new org.bson.Document("ping", 1));
    } catch (Throwable e) {
      Call.propagateIfFatal(e);
      return CheckResult.failed(e);
    }
    return CheckResult.OK;
  }

  @Override public String toString() {
    return "MongoDBStorage{connectionString=" + connectionString + ", database=" + database + "}";
  }

  @Override public void close() {
    if (closeCalled) return;
    closeCalled = true;
    MongoClient client = mongoClient;
    if (client != null) client.close();
  }
}
