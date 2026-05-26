/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.testcontainers.utility.DockerImageName.parse;
import static zipkin2.Call.propagateIfFatal;

class MongoDBContainer extends GenericContainer<MongoDBContainer> {
  static final Logger LOGGER = LoggerFactory.getLogger(MongoDBContainer.class);

  MongoDBContainer() {
    super(parse("mongo:7"));
    addExposedPort(27017);
    waitStrategy = Wait.forListeningPort();
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
  }

  @Override public void start() {
    super.start();
    LOGGER.info("Using MongoDB at {}", connectionString());
  }

  String connectionString() {
    return "mongodb://" + getHost() + ":" + getMappedPort(27017);
  }

  MongoDBStorage.Builder newStorageBuilder() {
    return MongoDBStorage.newBuilder()
      .connectionString(connectionString())
      .maxConnections(1);
  }

  void clear(MongoDBStorage storage) {
    MongoDatabase db = storage.database();
    db.getCollection(storage.spanCollection).drop();
  }
}
