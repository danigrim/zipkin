# zipkin-storage-mongodb

This module provides a MongoDB storage backend for Zipkin.

## Configuration

| Property | Environment Variable | Default | Description |
| --- | --- | --- | --- |
| `zipkin.storage.type` | `STORAGE_TYPE` | | Set to `mongodb` to enable |
| `zipkin.storage.mongodb.connection-string` | `MONGODB_CONNECTION_STRING` | `mongodb://localhost:27017` | MongoDB connection string |
| `zipkin.storage.mongodb.database` | `MONGODB_DATABASE` | `zipkin` | Database name |
| `zipkin.storage.mongodb.ensure-schema` | `MONGODB_ENSURE_SCHEMA` | `true` | Create indexes on startup |
| `zipkin.storage.mongodb.span-ttl` | `MONGODB_SPAN_TTL` | `7` | Days to keep span data |
| `zipkin.storage.mongodb.username` | `MONGODB_USERNAME` | | Authentication username |
| `zipkin.storage.mongodb.password` | `MONGODB_PASSWORD` | | Authentication password |

### Common Zipkin Storage Properties

These apply to all storage backends, including MongoDB:

| Property | Default | Description |
| --- | --- | --- |
| `zipkin.storage.strict-trace-id` | `true` | Require 128-bit trace ID match |
| `zipkin.storage.search-enabled` | `true` | Enable search indexes |
| `zipkin.storage.autocomplete-keys` | | Tag keys eligible for autocomplete |
| `zipkin.storage.autocomplete-ttl` | `3600000` | Autocomplete TTL in ms |
| `zipkin.storage.autocomplete-cardinality` | `20000` | Max unique autocomplete values |

## Example Usage

To use MongoDB as the Zipkin storage backend, start the server with:

```bash
STORAGE_TYPE=mongodb MONGODB_CONNECTION_STRING=mongodb://localhost:27017 java -jar zipkin-server.jar
```

Or with Docker:

```bash
docker run -e STORAGE_TYPE=mongodb -e MONGODB_CONNECTION_STRING=mongodb://host.docker.internal:27017 openzipkin/zipkin
```

## Schema

The module uses the following collections:

- **spans**: Stores span documents with indexes on trace_id, service name, timestamp, and duration.
  A TTL index automatically expires old data.
- **service_names**: Stores unique service names for autocomplete.
- **span_names**: Stores service-to-span-name mappings.
- **remote_service_names**: Stores service-to-remote-service-name mappings.
- **dependencies**: Stores pre-aggregated dependency links.
- **autocomplete_tags**: Stores tag key-value pairs for autocomplete.

### Indexes

When `ensure-schema` is true (default), the following indexes are created on startup:

- `spans.trace_id`: For trace lookups
- `spans.ts`: TTL index for automatic expiration
- `spans.(local_service_name, name, ts)`: For service+span+time queries
- `spans.(local_service_name, remote_service_name, ts)`: For remote service queries
- `spans.(local_service_name, duration)`: For duration-based queries
- `spans.annotations_query`: For annotation/tag search

## Dependencies

Dependencies are computed on-the-fly using MongoDB aggregation queries and the existing
`DependencyLinker` utility. This means no separate dependency aggregation job is needed.

## Performance

- Connection pooling is handled by the MongoDB Java driver's built-in connection pool.
- Upsert operations prevent duplicate spans.
- Compound indexes support efficient query patterns used by the Zipkin UI.
- TTL indexes automatically clean up old data without manual intervention.
