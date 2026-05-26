/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.storage.mongodb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.Nullable;

final class MongoDBSpanCodec {

  static Document toDocument(Span span) {
    Document doc = new Document();
    doc.append("traceId", span.traceId());
    if (span.parentId() != null) doc.append("parentId", span.parentId());
    doc.append("id", span.id());
    if (span.kind() != null) doc.append("kind", span.kind().name());
    if (span.name() != null) doc.append("name", span.name());
    if (span.timestamp() != null) {
      doc.append("timestamp", span.timestamp());
      doc.append("timestamp_millis", span.timestamp() / 1000);
    }
    if (span.duration() != null) doc.append("duration", span.duration());
    if (span.localEndpoint() != null) {
      doc.append("localEndpoint", endpointToDocument(span.localEndpoint()));
    }
    if (span.remoteEndpoint() != null) {
      doc.append("remoteEndpoint", endpointToDocument(span.remoteEndpoint()));
    }
    if (!span.annotations().isEmpty()) {
      List<Document> annotations = new ArrayList<>(span.annotations().size());
      for (Annotation a : span.annotations()) {
        annotations.add(new Document("timestamp", a.timestamp()).append("value", a.value()));
      }
      doc.append("annotations", annotations);
    }
    if (!span.tags().isEmpty()) {
      List<Document> tags = new ArrayList<>(span.tags().size());
      for (Map.Entry<String, String> tag : span.tags().entrySet()) {
        tags.add(new Document("key", tag.getKey()).append("value", tag.getValue()));
      }
      doc.append("tags", tags);
    }
    if (Boolean.TRUE.equals(span.debug())) doc.append("debug", true);
    if (Boolean.TRUE.equals(span.shared())) doc.append("shared", true);
    return doc;
  }

  static Document endpointToDocument(Endpoint endpoint) {
    Document doc = new Document();
    if (endpoint.serviceName() != null) doc.append("serviceName", endpoint.serviceName());
    if (endpoint.ipv4() != null) doc.append("ipv4", endpoint.ipv4());
    if (endpoint.ipv6() != null) doc.append("ipv6", endpoint.ipv6());
    if (endpoint.portAsInt() != 0) doc.append("port", endpoint.portAsInt());
    return doc;
  }

  static Span fromDocument(Document doc) {
    Span.Builder builder = Span.newBuilder()
      .traceId(doc.getString("traceId"))
      .id(doc.getString("id"));

    if (doc.containsKey("parentId")) builder.parentId(doc.getString("parentId"));
    if (doc.containsKey("kind")) builder.kind(Span.Kind.valueOf(doc.getString("kind")));
    if (doc.containsKey("name")) builder.name(doc.getString("name"));
    if (doc.containsKey("timestamp")) builder.timestamp(doc.getLong("timestamp"));
    if (doc.containsKey("duration")) builder.duration(doc.getLong("duration"));

    if (doc.containsKey("localEndpoint")) {
      builder.localEndpoint(endpointFromDocument(doc.get("localEndpoint", Document.class)));
    }
    if (doc.containsKey("remoteEndpoint")) {
      builder.remoteEndpoint(endpointFromDocument(doc.get("remoteEndpoint", Document.class)));
    }

    if (doc.containsKey("annotations")) {
      List<Document> annotations = doc.getList("annotations", Document.class);
      for (Document a : annotations) {
        builder.addAnnotation(a.getLong("timestamp"), a.getString("value"));
      }
    }

    if (doc.containsKey("tags")) {
      List<Document> tags = doc.getList("tags", Document.class);
      for (Document t : tags) {
        builder.putTag(t.getString("key"), t.getString("value"));
      }
    }

    if (doc.containsKey("debug")) builder.debug(doc.getBoolean("debug"));
    if (doc.containsKey("shared")) builder.shared(doc.getBoolean("shared"));

    return builder.build();
  }

  @Nullable static Endpoint endpointFromDocument(Document doc) {
    if (doc == null) return null;
    Endpoint.Builder builder = Endpoint.newBuilder();
    if (doc.containsKey("serviceName")) builder.serviceName(doc.getString("serviceName"));
    if (doc.containsKey("ipv4")) builder.ip(doc.getString("ipv4"));
    if (doc.containsKey("ipv6")) builder.ip(doc.getString("ipv6"));
    if (doc.containsKey("port")) builder.port(doc.getInteger("port"));
    return builder.build();
  }

  private MongoDBSpanCodec() {
  }
}
