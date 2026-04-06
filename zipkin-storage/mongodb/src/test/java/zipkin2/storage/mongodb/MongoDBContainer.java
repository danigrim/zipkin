/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

class MongoDBContainer extends org.testcontainers.containers.MongoDBContainer {
  static final Logger LOGGER = LoggerFactory.getLogger(MongoDBContainer.class);

  MongoDBContainer() {
    super("mongo:7");
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
  }

  @Override public void start() {
    super.start();
    LOGGER.info("Using MongoDB connection string {}", getConnectionString());
  }

  MongoStorage.Builder newStorageBuilder() {
    return MongoStorage.newBuilder()
      .host(getHost())
      .port(getMappedPort(27017))
      .database("zipkin");
  }

  void clear(MongoStorage storage) {
    storage.clear();
  }
}
