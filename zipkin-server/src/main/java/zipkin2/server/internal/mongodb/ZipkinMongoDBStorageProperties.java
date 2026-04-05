/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.server.internal.mongodb;

import java.io.Serializable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import zipkin2.storage.mongodb.MongoDBStorage;

@ConfigurationProperties("zipkin.storage.mongodb")
class ZipkinMongoDBStorageProperties implements Serializable {
  private static final long serialVersionUID = 0L;

  /** Connection string for MongoDB. Defaults to "mongodb://localhost:27017" */
  private String connectionString = "mongodb://localhost:27017";
  /** Database to store span and index data. Defaults to "zipkin" */
  private String database = "zipkin";
  /** Whether to ensure indexes on startup. Defaults to true. */
  private boolean ensureSchema = true;
  /** Number of days to keep span data. Defaults to 7. */
  private int spanTtl = 7;
  /** Username for authentication. No default. */
  private String username;
  /** Password for authentication. No default. */
  private String password;

  public String getConnectionString() {
    return connectionString;
  }

  public void setConnectionString(String connectionString) {
    this.connectionString = "".equals(connectionString) ? "mongodb://localhost:27017" : connectionString;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = "".equals(database) ? "zipkin" : database;
  }

  public boolean isEnsureSchema() {
    return ensureSchema;
  }

  public void setEnsureSchema(boolean ensureSchema) {
    this.ensureSchema = ensureSchema;
  }

  public int getSpanTtl() {
    return spanTtl;
  }

  public void setSpanTtl(int spanTtl) {
    this.spanTtl = spanTtl;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = "".equals(username) ? null : username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = "".equals(password) ? null : password;
  }

  public MongoDBStorage.Builder toBuilder() {
    return MongoDBStorage.newBuilder()
      .connectionString(connectionString)
      .database(database)
      .ensureSchema(ensureSchema)
      .spanTtl(spanTtl)
      .username(username)
      .password(password);
  }
}
