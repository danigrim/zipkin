/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

class MongoDBContainer extends GenericContainer<MongoDBContainer> {
  static final Logger LOGGER = LoggerFactory.getLogger(MongoDBContainer.class);
  static final int MONGODB_PORT = 27017;

  MongoDBContainer() {
    super("mongo:7");
    addExposedPort(MONGODB_PORT);
    waitingFor(Wait.forListeningPort());
    withStartupTimeout(java.time.Duration.ofMinutes(2));
  }

  MongoDBStorage.Builder newStorageBuilder() {
    return MongoDBStorage.newBuilder()
      .connectionString("mongodb://" + getHost() + ":" + getMappedPort(MONGODB_PORT))
      .database("zipkin_test")
      .ensureSchema(true);
  }

  void clear(MongoDBStorage storage) {
    storage.clear();
  }
}
