/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.server.internal.mongodb;

import java.io.Serializable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.mongodb.MongoDBStorage;

/**
 * Settings for MongoDB storage.
 * <pre>{@code
 * zipkin.storage.mongodb:
 *   connection-string: mongodb://localhost:27017
 *   database: zipkin
 *   max-connections: 10
 *   ensure-schema: true
 * }</pre>
 */
@ConfigurationProperties("zipkin.storage.mongodb")
class ZipkinMongoDBStorageProperties implements Serializable {
  private static final long serialVersionUID = 0L;

  /** MongoDB connection string. Defaults to "mongodb://localhost:27017". */
  private String connectionString = "mongodb://localhost:27017";
  /** Database name. Defaults to "zipkin". */
  private String database = "zipkin";
  /** Maximum number of connections in the pool. Defaults to 10. */
  private int maxConnections = 10;
  /** Whether to ensure indexes on startup. Defaults to true. */
  private boolean ensureSchema = true;

  public String getConnectionString() {
    return connectionString;
  }

  public void setConnectionString(String connectionString) {
    this.connectionString = connectionString;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = "".equals(database) ? "zipkin" : database;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public void setMaxConnections(int maxConnections) {
    this.maxConnections = maxConnections;
  }

  public boolean isEnsureSchema() {
    return ensureSchema;
  }

  public void setEnsureSchema(boolean ensureSchema) {
    this.ensureSchema = ensureSchema;
  }

  public MongoDBStorage.Builder toBuilder() {
    return MongoDBStorage.newBuilder()
      .connectionString(connectionString)
      .database(database)
      .maxConnections(maxConnections)
      .ensureSchema(ensureSchema);
  }
}
