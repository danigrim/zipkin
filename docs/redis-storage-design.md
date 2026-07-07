# Design: Redis-backed storage backend for Zipkin

Status: Proposal (design only — no functional Redis code is included in this document)

This document describes how a Redis-backed storage backend could be added to Zipkin.
It is written against the existing storage SPI in the `zipkin` core module and the
existing storage modules `zipkin-storage/cassandra`, `zipkin-storage/elasticsearch`,
and `zipkin-storage/mysql-v1`. The MySQL (v1) module is used as the primary template
because it is the smallest, self-contained, single-node relational backend and maps
most cleanly onto the Redis data-structure model.

## 1. Background: the storage SPI

All Zipkin storage backends implement the abstract class
`zipkin2.storage.StorageComponent` (`zipkin/src/main/java/zipkin2/storage/StorageComponent.java`).
The contract a backend must satisfy is:

- `SpanStore spanStore()` — query side (required).
- `SpanConsumer spanConsumer()` — write side (required).
- `Traces traces()` — batch trace lookup by ID (defaults to a `TracesAdapter` over `spanStore()`).
- `ServiceAndSpanNames serviceAndSpanNames()` — service/span/remote-service name lookup
  (defaults delegate to the deprecated `SpanStore` methods).
- `AutocompleteTags autocompleteTags()` — site-specific tag key/value suggestions
  (defaults to an empty implementation).
- `CheckResult check()` — health check, inherited from `zipkin2.Component`.
- `boolean isOverCapacity(Throwable)` — load-shedding hint (defaults to detecting
  `RejectedExecutionException`).

The nested `StorageComponent.Builder` defines the cross-backend options every backend
should honor:

- `strictTraceId(boolean)` — when `false`, group/lookup traces by the right-most 16
  hex characters (low 64 bits) of the trace ID.
- `searchEnabled(boolean)` — when `false`, disable indexed queries (only `traces()` by
  ID need work); indexed operations should return empty rather than throw.
- `autocompleteKeys(List<String>)`, `autocompleteTtl(int)`, `autocompleteCardinality(int)`
  — control which tag keys are indexed for autocomplete, and how aggressively repeated
  writes are suppressed.

### 1.1 `SpanStore` (query side)

From `zipkin/src/main/java/zipkin2/storage/SpanStore.java` and the related
`ServiceAndSpanNames` interface:

| Method | Semantics |
| --- | --- |
| `Call<List<List<Span>>> getTraces(QueryRequest)` | Traces matching a filter, grouped by trace ID, no ordering expectation. |
| `Call<List<Span>> getTrace(String traceId)` | All spans sharing a 128-bit trace ID (deprecated in favor of `Traces.getTrace`). |
| `Call<List<String>> getServiceNames()` | All local (and, historically, remote) service names, lexicographic. |
| `Call<List<String>> getSpanNames(String serviceName)` | Span names recorded by a service, lexicographic. |
| `Call<List<String>> getRemoteServiceNames(String serviceName)` | Remote service names recorded by a service (on `ServiceAndSpanNames`). |
| `Call<List<DependencyLink>> getDependencies(long endTs, long lookback)` | Aggregated service dependency links within `(endTs - lookback, endTs]`. |

`QueryRequest` (`zipkin/src/main/java/zipkin2/storage/QueryRequest.java`) carries the
filter used by `getTraces`: `serviceName`, `remoteServiceName`, `spanName`,
`annotationQuery` (a map of tag/annotation constraints), `minDuration`/`maxDuration`
(micros), `endTs`/`lookback` (millis), and `limit`. Note `QueryRequest.test(List<Span>)`
can re-check a candidate trace in memory — a backend only needs its index to *narrow*
candidates, not perfectly answer the query.

### 1.2 `SpanConsumer` (write side)

`zipkin/src/main/java/zipkin2/storage/SpanConsumer.java` is a single method:

```java
Call<Void> accept(List<Span> spans);
```

Everything Zipkin can query is derived from spans written here. In MySQL, `accept`
delegates to a `BatchInsertSpans` function that writes rows into `zipkin_spans` and
`zipkin_annotations` (`MySQLSpanConsumer.java`); dependency links are computed on demand
at query time from those rows (there is no pre-aggregation job in the v1 module by
default).

### 1.3 The `Call` abstraction

Storage methods return `zipkin2.Call<T>`, a lazy, executable/enqueueable, one-shot
request object (sync `execute()` or async `enqueue(Callback)`). The Redis backend would
wrap Redis client operations in `Call` implementations exactly as MySQL wraps JOOQ
`DataSourceCall`s and Cassandra wraps `ResultSetFutureCall`s.

## 2. Proposed module layout: `zipkin-storage/redis`

Mirror `zipkin-storage/mysql-v1`. Add the module to the aggregator
`zipkin-storage/pom.xml` (which currently lists `cassandra`, `mysql-v1`, `elasticsearch`):

```
zipkin-storage/redis/
  pom.xml                        # artifactId: zipkin-storage-redis; parent: zipkin-storage-parent
  README.md
  src/main/java/zipkin2/storage/redis/
    RedisStorage.java            # StorageComponent + Builder      (cf. MySQLStorage)
    RedisSpanStore.java          # SpanStore, Traces, ServiceAndSpanNames (cf. MySQLSpanStore)
    RedisSpanConsumer.java       # SpanConsumer                    (cf. MySQLSpanConsumer)
    RedisAutocompleteTags.java   # AutocompleteTags                (cf. MySQLAutocompleteTags)
    RedisCall.java               # Call<T> wrapper around the async Redis client
    Schema.java                  # key builders / naming conventions (cf. mysql Schema)
    internal/                    # codecs, pipeline helpers, cursor/scan utilities
  src/test/java/zipkin2/storage/redis/
    RedisStorageTest.java        # unit tests (no server)
    ITRedisStorage.java          # Testcontainers-based integration test (cf. ITMySQLStorage)
```

Client choice (an open question, see §7): a Netty-based async client such as **Lettuce**
fits Zipkin's non-blocking `Call`/`Callback` model best; **Jedis** is simpler but
blocking (the MySQL module is itself blocking and offloads to an `Executor`, so a
blocking client with an executor is also acceptable). This document is client-agnostic
and only assumes standard Redis data types (plus optionally RediSearch, see §7).

The `pom.xml` should follow `zipkin-storage/mysql-v1/pom.xml`: inherit from
`zipkin-storage-parent`, set `<artifactId>zipkin-storage-redis</artifactId>` and a
`<name>Storage: Redis</name>`, depend on `zipkin` (provided/compile as siblings do),
the Redis client, and `testcontainers` for integration tests.

## 3. Redis data model

All keys are namespaced under a configurable prefix (default `zipkin:`) so multiple
logical Zipkin instances can share a Redis deployment. Trace and span IDs are stored as
the canonical lower-hex strings Zipkin already uses (`Span.traceId()`, `Span.id()`),
avoiding the 64/128-bit split MySQL needs. Timestamps are `Span.timestamp()` epoch
micros; index scores use epoch millis to match `QueryRequest` granularity.

### 3.1 Spans (raw storage, keyed by trace)

Store the raw spans of a trace together so `getTrace`/`Traces.getTrace` is a single
key read.

| Key | Type | Value | Purpose |
| --- | --- | --- | --- |
| `zipkin:trace:{traceId}` | LIST (or SET) | JSON/PROTO3-encoded `Span` per element | All spans in a trace; append on `accept`, read whole for `getTrace`. |

Encoding reuses the existing codecs (`SpanBytesEncoder`/`SpanBytesDecoder`) already used
throughout Zipkin, so no new serialization format is introduced. A per-trace `LIST`
tolerates the duplicate/partial-span updates Zipkin expects (shared spans reported by
both client and server); deduplication, if desired, happens on read via the same
grouping logic MySQL uses. A per-trace TTL (`EXPIRE zipkin:trace:{traceId}`) implements
data retention (see §7).

### 3.2 Search indexes (to support `getTraces`)

Because Redis has no ad-hoc query planner, `getTraces` is served by intersecting several
secondary indexes, then re-checking candidates in memory with `QueryRequest.test(...)`.
This mirrors how Cassandra builds `trace_by_service_span` index tables and how MySQL
uses column indexes on `zipkin_spans`/`zipkin_annotations`.

Two complementary index shapes:

1. **Time-ordered sets per search dimension** (Sorted Set, score = span timestamp millis):

   | Key | Members |
   | --- | --- |
   | `zipkin:idx:service:{serviceName}` | traceIds seen for a local service |
   | `zipkin:idx:span:{serviceName}:{spanName}` | traceIds for a (service, spanName) |
   | `zipkin:idx:remote:{serviceName}:{remoteServiceName}` | traceIds for a (service, remoteServiceName) |
   | `zipkin:idx:annotation:{serviceName}:{key}` | traceIds where an annotation/tag key is present |
   | `zipkin:idx:tag:{serviceName}:{key}={value}` | traceIds where a tag equals a value |

   `getTraces` computes the relevant member key(s) from the `QueryRequest`, uses
   `ZRANGEBYSCORE`/`ZREVRANGEBYSCORE` with `endTs-lookback .. endTs` and `LIMIT` to get
   the newest candidate trace IDs, and — when more than one condition is present —
   intersects them (`ZINTERSTORE` into a short-lived key, or client-side set
   intersection) before loading candidate traces and applying `QueryRequest.test`.
   `minDuration`/`maxDuration` and multi-tag `annotationQuery` are enforced by the
   in-memory `test` pass (the same fallback MySQL/Cassandra rely on when the index
   cannot fully refine).

2. **Name catalogs** (Sorted Sets, lexicographic) for the autocomplete-style lookups:

   | Key | Members | Serves |
   | --- | --- | --- |
   | `zipkin:services` | local service names | `getServiceNames` |
   | `zipkin:spans:{serviceName}` | span names for a service | `getSpanNames` |
   | `zipkin:remotes:{serviceName}` | remote service names for a service | `getRemoteServiceNames` |

   Sorted sets with equal scores allow `ZRANGEBYLEX` to return names already sorted
   lexicographically, matching the SPI's "sorted lexicographically" contract without a
   post-sort.

### 3.3 Autocomplete tags

| Key | Type | Members | Serves |
| --- | --- | --- | --- |
| `zipkin:autocomplete:keys` | SET / ZSET | configured autocomplete keys actually seen | `AutocompleteTags.getKeys()` |
| `zipkin:autocomplete:{key}` | ZSET | observed values for that key | `AutocompleteTags.getValues(key)` |

Only keys configured via `autocompleteKeys` are indexed (same rule as MySQL's
`MySQLAutocompleteTags`). `autocompleteTtl`/`autocompleteCardinality` map naturally to
Redis: suppress re-writing a recently written key/value pair (a short-lived marker key
with `PX autocompleteTtl` and `NX`), and cap the cardinality of a values ZSET.

### 3.4 Dependency links

Two options, mirroring the two modes already present in the codebase:

- **On-demand (MySQL v1 style, default):** compute links at query time. `getDependencies`
  scans the trace index sorted set(s) over `(endTs-lookback, endTs]`, loads those traces,
  and feeds their spans to the existing `zipkin2.internal.DependencyLinker` (exactly what
  `MySQLSpanStore` does via `AggregateDependencies`). No extra write-time state.
- **Pre-aggregated (Cassandra/Elasticsearch style):** maintain daily buckets updated on
  write or by a periodic job:

  | Key | Type | Field/Member | Value |
  | --- | --- | --- | --- |
  | `zipkin:dependencies:{yyyy-mm-dd}` | HASH | `{parent}|{child}` | packed callCount/errorCount |

  `getDependencies` then reads the day buckets spanning the window and merges them, as
  `SelectDependencies` does for MySQL when `hasPreAggregatedDependencies` is true.

Recommendation: start with on-demand (simplest, no write amplification), leave
pre-aggregation as a follow-up toggle. The SPI already documents that implementations may
bucket daily and floor `endTs` to the bucket.

## 4. Mapping SPI operations onto Redis

### 4.1 `SpanConsumer.accept(List<Span> spans)`

Executed as one pipelined/transactional batch (`MULTI`/pipeline) so a batch of spans is
one round trip, analogous to MySQL's `create.batch(inserts).execute()`:

For each `Span`:
1. `RPUSH zipkin:trace:{span.traceId()}` the encoded span; `EXPIRE` the trace key to the
   configured retention TTL.
2. Derive `serviceName = span.localServiceName()`. When present:
   - `ZADD zipkin:services 0 {serviceName}`.
   - `ZADD zipkin:spans:{serviceName} 0 {span.name()}` (if a name is present).
   - `ZADD zipkin:idx:service:{serviceName} {tsMillis} {traceId}`.
   - `ZADD zipkin:idx:span:{serviceName}:{span.name()} {tsMillis} {traceId}`.
3. When `span.remoteServiceName()` is present:
   - `ZADD zipkin:remotes:{serviceName} 0 {remoteServiceName}`.
   - `ZADD zipkin:idx:remote:{serviceName}:{remoteServiceName} {tsMillis} {traceId}`.
4. For each annotation value and each tag key: `ZADD zipkin:idx:annotation:{serviceName}:{key} {tsMillis} {traceId}`
   and, for tags, `ZADD zipkin:idx:tag:{serviceName}:{key}={value} {tsMillis} {traceId}`.
5. For configured autocomplete keys present in `span.tags()`: `ZADD zipkin:autocomplete:{key} 0 {value}`
   and record the key in `zipkin:autocomplete:keys` (subject to TTL/cardinality suppression).

When `searchEnabled == false`, steps 2–5 are skipped (only step 1 runs), matching the SPI
intent that indexing can be disabled. When `strictTraceId == false`, index members use the
low-64-bit (right-most 16 hex chars) form of the trace ID so lookups can group truncated IDs.

Returns a `Call<Void>` (`RedisCall`) that executes the pipeline; empty input returns
`Call.create(null)` as MySQL does.

### 4.2 `SpanStore.getTrace(traceId)` / `Traces.getTrace`

`LRANGE zipkin:trace:{normalizeTraceId(traceId)} 0 -1`, decode each element to a `Span`.
With `strictTraceId == false`, additionally consult a low-64-bit alias so truncated IDs
resolve; then filter with `StrictTraceId.filterSpans` exactly as `MySQLSpanStore` does.

### 4.3 `SpanStore.getTraces(QueryRequest)`

1. If `!searchEnabled`, return `Call.emptyList()` (MySQL behavior).
2. Choose the most selective index key(s) from the request:
   - `spanName` present → `zipkin:idx:span:{service}:{spanName}`
   - else `remoteServiceName` present → `zipkin:idx:remote:{service}:{remote}`
   - else each `annotationQuery` entry → the matching `annotation`/`tag` index
   - else `serviceName` present → `zipkin:idx:service:{service}`
   - else (no service) → union across services, or a global time index (see §7).
3. `ZREVRANGEBYSCORE key {endTs} {endTs-lookback} LIMIT 0 {fetchLimit}` for each chosen
   key; when multiple conditions exist, intersect the candidate trace-ID sets
   (`ZINTERSTORE` to a temp key with a short TTL, or intersect client-side).
   Over-fetch by a multiplier (Cassandra exposes this as `index-fetch-multiplier`) so the
   in-memory refine step can drop false positives and still satisfy `limit`.
4. Load candidate traces (§4.2) and keep those where `QueryRequest.test(spans)` is true.
5. Group by trace ID (reuse `GroupByTraceId`), apply `StrictTraceId.filterTraces` when
   strict, truncate to `limit`.

This is the same "index narrows, memory refines" strategy the existing backends use.

### 4.4 `getServiceNames` / `getSpanNames` / `getRemoteServiceNames`

- `getServiceNames()` → `ZRANGEBYLEX zipkin:services - +` (empty if `!searchEnabled`).
- `getSpanNames(service)` → `ZRANGEBYLEX zipkin:spans:{service} - +` (empty if service is
  empty or `!searchEnabled`).
- `getRemoteServiceNames(service)` → `ZRANGEBYLEX zipkin:remotes:{service} - +` (empty if
  service is empty or `!searchEnabled`).

Results are already lexicographically ordered by `ZRANGEBYLEX`.

### 4.5 `getDependencies(endTs, lookback)`

Default (on-demand): validate `endTs > 0` and `lookback > 0` (as MySQL does), collect
candidate trace IDs from the time indexes over the window, load their spans, and run
`DependencyLinker` to produce `List<DependencyLink>`. If pre-aggregation is enabled, read
and merge the `zipkin:dependencies:{day}` hashes covering the window instead.

### 4.6 `check()` and `isOverCapacity`

- `check()` issues a cheap command (`PING`, or a bounded `EXISTS`/`ZCARD` on a known key)
  and returns `CheckResult.OK` or `CheckResult.failed(e)`, mirroring MySQL's trivial
  `SELECT ... LIMIT 1`.
- `isOverCapacity(Throwable)` returns true for the client's pool-exhaustion/timeout
  exceptions in addition to the default `RejectedExecutionException`.

## 5. `zipkin-server` auto-configuration wiring

The server does not use Spring `spring.factories` autoconfiguration for storage. Instead,
each backend config class is listed explicitly in
`zipkin-server/src/main/java/zipkin2/server/internal/InternalZipkinConfiguration.java`
via `@Import`, and each is gated on the `zipkin.storage.type` property. That property is
bound from the `STORAGE_TYPE` environment variable in
`zipkin-server/src/main/resources/zipkin-server-shared.yml`:

```yaml
zipkin:
  storage:
    type: ${STORAGE_TYPE:mem}
```

So `STORAGE_TYPE=redis` sets `zipkin.storage.type=redis`.

### 5.1 New config package `zipkin2.server.internal.redis`

Mirror `zipkin-server/src/main/java/zipkin2/server/internal/mysql/`
(`ZipkinMySQLStorageConfiguration`, `ZipkinMySQLStorageProperties`):

`ZipkinRedisStorageConfiguration.java`:

```java
@EnableConfigurationProperties(ZipkinRedisStorageProperties.class)
@ConditionalOnClass(RedisStorage.class)
@ConditionalOnProperty(name = "zipkin.storage.type", havingValue = "redis")
@ConditionalOnMissingBean(StorageComponent.class)
public class ZipkinRedisStorageConfiguration {
  @Bean StorageComponent storage(
      ZipkinRedisStorageProperties redis,
      @Value("${zipkin.storage.strict-trace-id:true}") boolean strictTraceId,
      @Value("${zipkin.storage.search-enabled:true}") boolean searchEnabled,
      @Value("${zipkin.storage.autocomplete-keys:}") List<String> autocompleteKeys) {
    return RedisStorage.newBuilder()
        .strictTraceId(strictTraceId)
        .searchEnabled(searchEnabled)
        .autocompleteKeys(autocompleteKeys)
        // .redisUri(...), .prefix(...), .ttl(...) from properties
        .build();
  }
}
```

The four conditions are exactly those on `ZipkinMySQLStorageConfiguration`:
`@ConditionalOnClass` so the module is optional on the classpath,
`@ConditionalOnProperty(havingValue = "redis")` so it only activates for
`STORAGE_TYPE=redis`, and `@ConditionalOnMissingBean(StorageComponent.class)` so an
explicitly provided component wins.

`ZipkinRedisStorageProperties.java` — `@ConfigurationProperties("zipkin.storage.redis")`,
mapping env vars to a datasource-like config (see §5.3), analogous to
`ZipkinMySQLStorageProperties`.

### 5.2 Register the config class

Add the import and the entry to the `@Import({...})` list in
`InternalZipkinConfiguration.java`, next to the existing storage classes:

```java
import zipkin2.server.internal.redis.ZipkinRedisStorageConfiguration;
// ...
@Import({
  // ...
  ZipkinCassandra3StorageConfiguration.class,
  ZipkinElasticsearchStorageConfiguration.class,
  ZipkinMySQLStorageConfiguration.class,
  ZipkinRedisStorageConfiguration.class, // new
  // ...
})
```

### 5.3 Default properties

Add a `redis:` block under `zipkin.storage` in `zipkin-server-shared.yml` following the
`mysql:`/`cassandra3:` blocks, e.g.:

```yaml
    redis:
      uri: ${REDIS_URI:redis://localhost:6379}
      prefix: ${REDIS_PREFIX:zipkin:}
      ttl: ${REDIS_TTL:604800000}   # span retention in millis (7 days)
```

`strict-trace-id`, `search-enabled`, `autocomplete-*`, and the `throttle:` block already
exist at the `zipkin.storage` level and apply to every backend, including Redis, with no
extra wiring. Throttled storage (`ConditionalOnThrottledStorage`) wraps whatever
`StorageComponent` bean is produced, so Redis benefits from it automatically.

The `zipkin-server` `pom.xml` gains an optional dependency on `zipkin-storage-redis`
(alongside the existing storage modules) so `@ConditionalOnClass(RedisStorage.class)` can
activate when present.

## 6. End-to-end request flow (Redis)

```
Reporter --HTTP/gRPC/Kafka--> Collector --> SpanConsumer.accept(spans)
    RedisSpanConsumer: pipeline RPUSH trace + ZADD indexes/catalogs/autocomplete

Zipkin UI / API --> ZipkinQueryApiV2 --> SpanStore / ServiceAndSpanNames / AutocompleteTags
    getServiceNames  -> ZRANGEBYLEX zipkin:services
    getSpanNames     -> ZRANGEBYLEX zipkin:spans:{service}
    getTraces        -> ZREVRANGEBYSCORE idx:* (+ ZINTERSTORE) -> load traces -> QueryRequest.test
    getTrace         -> LRANGE zipkin:trace:{id}
    getDependencies  -> scan window -> DependencyLinker  (or read dependencies:{day})
```

## 7. Open questions and trade-offs

- **TTL / expiry.** Per-key `EXPIRE` on `zipkin:trace:{id}` gives O(1) native retention for
  raw spans — a genuine advantage over MySQL, which needs an external delete job. But
  Redis does **not** cascade: entries in the index sorted sets (§3.2) and autocomplete
  ZSETs do not disappear when a trace key expires, so they accumulate stale trace IDs.
  Mitigations to evaluate: (a) score index members by timestamp and periodically
  `ZREMRANGEBYSCORE` older than the retention window (a lightweight maintenance task);
  (b) tolerate dangling IDs and drop them lazily when a `getTraces` candidate fails to
  load; (c) use `keyspace notifications` to react to expiries. This is the single biggest
  correctness/ops question.

- **Memory footprint.** Redis keeps everything in RAM. High-throughput tracing produces
  large data volumes, and the secondary indexes multiply storage (each span can add to
  several sorted sets). Options: aggressive TTLs, sampling before storage, `searchEnabled=false`
  to drop all indexes when traces are found via logs, capping index ZSET sizes, and using
  Redis eviction policies carefully (eviction of index keys silently degrades search).
  Redis is likely best positioned as a **short-retention / high-speed** backend rather
  than a long-term archive; that positioning should be documented in the module README.

- **Secondary indexes for search.** The hand-rolled sorted-set intersection in §4.3 is
  workable but has sharp edges: high-cardinality tag values create many small keys;
  `ZINTERSTORE` cost grows with the smallest set size; and service-less queries need a
  global time index or a fan-out union. A strong alternative is **RediSearch** (the
  Redis Stack search module), which offers real secondary indexing and query expressions
  and could replace most of the bespoke index bookkeeping — at the cost of requiring a
  Redis build that ships RediSearch. Decide: portable plain-Redis indexes vs. RediSearch
  dependency.

- **Atomicity & duplicates.** `accept` should write the trace key and all its index
  entries atomically (`MULTI`/`EXEC` or a Lua script) so a crash cannot leave a trace
  indexed but unstored (or vice versa). Zipkin also expects duplicate/partial spans
  (client+server "shared" spans); the per-trace `LIST` accepts duplicates and grouping
  handles them on read — a `SET` of encoded spans would instead dedupe byte-identical
  copies. Choose based on how re-reported spans should merge.

- **Client & concurrency model.** Lettuce (async, Netty) matches Zipkin's
  `Call`/`Callback` non-blocking model directly; Jedis (blocking) would require an
  `Executor` like the MySQL module. Cluster mode complicates multi-key ops
  (`ZINTERSTORE`, `MULTI`) because keys must hash to the same slot — hash tags (e.g.
  `zipkin:{service}:...`) or restricting cross-key ops would be needed.

- **`strictTraceId=false` aliasing.** Supporting low-64-bit grouping requires either
  storing an alias from the 16-hex key to the 32-hex trace key, or indexing members by
  their low-64-bit form. This adds write-time work and needs a clear rule for which raw
  key holds the spans.

- **Dependencies mode.** On-demand aggregation (default) avoids write amplification but
  makes `getDependencies` proportional to the number of traces in the window; daily
  pre-aggregated hashes make reads cheap but add write cost and a merge/expiry story.
  Ship on-demand first, add pre-aggregation as an opt-in.

## 8. Summary

A `zipkin-storage/redis` module modeled on `mysql-v1` can satisfy the
`StorageComponent` SPI by storing raw spans in per-trace lists and deriving all query
paths from sorted-set indexes and catalogs, reusing existing pieces (`SpanBytesEncoder`,
`GroupByTraceId`, `StrictTraceId`, `QueryRequest.test`, `DependencyLinker`). Server
selection needs only a new `zipkin2.server.internal.redis` config class gated on
`zipkin.storage.type=redis` (from `STORAGE_TYPE`), registered in
`InternalZipkinConfiguration`, plus a `redis:` defaults block. The main risks are index
expiry consistency and RAM footprint, which point to Redis being a fast, short-retention
backend and motivate evaluating RediSearch for the search path.
