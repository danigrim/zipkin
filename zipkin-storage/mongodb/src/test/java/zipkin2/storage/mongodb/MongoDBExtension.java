/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import zipkin2.CheckResult;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.testcontainers.utility.DockerImageName.parse;

class MongoDBExtension implements BeforeAllCallback, AfterAllCallback {
  static final Logger LOGGER = LoggerFactory.getLogger(MongoDBExtension.class);

  final MongoDBContainer container = new MongoDBContainer();

  @Override public void beforeAll(ExtensionContext context) throws Exception {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      // Only run once in outermost scope.
      return;
    }

    container.start();
    LOGGER.info("Using hostPort {}:{}", host(), port());

    try (MongoStorage result = computeStorageBuilder().build()) {
      CheckResult check = result.check();
      assumeTrue(check.ok(), () -> "Could not connect to storage, skipping test: "
        + check.error().getMessage());
    }
  }

  @Override public void afterAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      // Only run once in outermost scope.
      return;
    }

    container.stop();
  }

  MongoStorage.Builder computeStorageBuilder() {
    return MongoStorage.newBuilder()
      .host(host())
      .port(port());
  }

  String host() {
    return container.getHost();
  }

  int port() {
    return container.getMappedPort(27017);
  }

  // mostly waiting for https://github.com/testcontainers/testcontainers-java/issues/3537
  static final class MongoDBContainer extends GenericContainer<MongoDBContainer> {
    MongoDBContainer() {
      super(parse("mongo:7.0"));
      addExposedPort(27017);
      waitStrategy = Wait.forListeningPort();
      withLogConsumer(new Slf4jLogConsumer(LOGGER));
    }
  }
}
