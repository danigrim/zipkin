/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.server.internal.mongodb;

import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import zipkin2.storage.StorageComponent;
import zipkin2.storage.mongodb.MongoDBStorage;

/**
 * Auto-configuration for MongoDB storage backend.
 *
 * <p>This is activated when {@code zipkin.storage.type=mongodb} is set and the MongoDB storage
 * class is on the classpath.
 */
@ConditionalOnClass(MongoDBStorage.class)
@EnableConfigurationProperties(ZipkinMongoDBStorageProperties.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "mongodb")
@ConditionalOnMissingBean(StorageComponent.class)
public class ZipkinMongoDBStorageConfiguration {

  @Bean @ConditionalOnMissingBean StorageComponent storage(
      ZipkinMongoDBStorageProperties properties,
      @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId,
      @Value("${zipkin.storage.search-enabled:true}") boolean searchEnabled,
      @Value("${zipkin.storage.autocomplete-keys:}") List<String> autocompleteKeys,
      @Value("${zipkin.storage.autocomplete-ttl:3600000}") int autocompleteTtl,
      @Value("${zipkin.storage.autocomplete-cardinality:20000}") int autocompleteCardinality) {
    return properties.toBuilder()
      .strictTraceId(strictTraceId)
      .searchEnabled(searchEnabled)
      .autocompleteKeys(autocompleteKeys)
      .autocompleteTtl(autocompleteTtl)
      .autocompleteCardinality(autocompleteCardinality)
      .build();
  }
}
