/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

class MongoDBContainer extends GenericContainer<MongoDBContainer> {
  static final Logger LOGGER = LoggerFactory.getLogger(MongoDBContainer.class);
  static final int MONGODB_PORT = 27017;

  MongoDBContainer() {
    super("mongo:7");
    addExposedPort(MONGODB_PORT);
    waitStrategy = Wait.forListeningPort();
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
  }

  @Override public void start() {
    super.start();
    LOGGER.info("Using MongoDB connection string {}", connectionString());
  }

  String connectionString() {
    return "mongodb://" + getHost() + ":" + getMappedPort(MONGODB_PORT);
  }

  MongoDBStorage.Builder newStorageBuilder() {
    return MongoDBStorage.newBuilder()
      .connectionString(connectionString())
      .database("zipkin");
  }

  void clear(MongoDBStorage storage) {
    storage.clear();
  }
}
