# storage-redis
This Redis storage component includes a blocking `SpanStore` and span consumer function.
`SpanStore.getDependencies()` aggregates dependency links on-demand.

The implementation uses [Jedis](https://github.com/redis/jedis) and stores spans as
v2 JSON. Query semantics mirror `zipkin2.storage.InMemoryStorage`.

## Data model
All keys are prefixed with `zipkin:`.

* `zipkin:trace:{lowTraceId}` — a set of v2 JSON spans sharing the lower 64-bits of a trace ID.
* `zipkin:traces` — a sorted set of `{lowTraceId}:{timestampMillis}` members scored by epoch
  millis, used for time-ranged trace lookup and dependency aggregation.
* `zipkin:service:{name}:traces` — the per-service equivalent of `zipkin:traces`.
* `zipkin:services` — a set of local service names.
* `zipkin:service:{name}:spans` / `:remote` — span and remote-service names per local service.
* `zipkin:autocomplete:{key}` — the values seen for a configured autocomplete tag key.

Keys are given a sliding TTL (`RedisStorage.Builder.dataTtl`, default 7 days) to bound memory use.
When `searchEnabled` is false, only the trace and timestamp keys are written.

## Usage
`zipkin2.storage.redis.RedisStorage.Builder` connects to `localhost:6379` by default:

```java
StorageComponent storage = RedisStorage.newBuilder()
  .host("redis.example.com")
  .port(6379)
  .build();
```

A pre-configured `redis.clients.jedis.JedisPool` can be supplied via `jedisPool(...)`.

## Testing this component
This module conditionally runs integration tests against a Docker managed Redis container.

Ex.
```
$ ./mvnw clean verify -pl :zipkin-storage-redis
```

If you run tests via Maven or otherwise without Docker, integration tests are silently skipped.
