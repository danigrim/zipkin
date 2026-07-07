/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.redis;

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

class RedisExtension implements BeforeAllCallback, AfterAllCallback {
  static final Logger LOGGER = LoggerFactory.getLogger(RedisExtension.class);
  static final int REDIS_PORT = 6379;

  final RedisContainer container = new RedisContainer();

  @Override public void beforeAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      // Only run once in outermost scope.
      return;
    }

    container.start();
    LOGGER.info("Using hostPort {}:{}", host(), port());

    try (RedisStorage result = computeStorageBuilder().build()) {
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

  RedisStorage.Builder computeStorageBuilder() {
    return RedisStorage.newBuilder().host(host()).port(port());
  }

  String host() {
    return container.getHost();
  }

  int port() {
    return container.getMappedPort(REDIS_PORT);
  }

  static final class RedisContainer extends GenericContainer<RedisContainer> {
    RedisContainer() {
      super(parse("redis:7-alpine"));
      addExposedPort(REDIS_PORT);
      waitStrategy = Wait.forListeningPort();
      withLogConsumer(new Slf4jLogConsumer(LOGGER));
    }
  }
}
