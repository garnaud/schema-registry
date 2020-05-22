/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.metrics;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.utils.SystemTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.Manifest;

public class MetricsContainer {

  private static final Logger log = LoggerFactory.getLogger(MetricsContainer.class);

  private static final String JMX_PREFIX = "kafka.schema.registry";

  private final Metrics metrics;
  private final Map<String, String> configuredTags;
  private final String commitId;

  private final SchemaRegistryMetric isMasterNode;
  private final SchemaRegistryMetric schemasCreated;
  private final SchemaRegistryMetric schemasDeleted;
  private final SchemaRegistryMetric customSchemaProviders;
  private final SchemaRegistryMetric apiCallsSuccess;
  private final SchemaRegistryMetric apiCallsFailure;

  private final SchemaRegistryMetric avroSchemasCreated;
  private final SchemaRegistryMetric jsonSchemasCreated;
  private final SchemaRegistryMetric protobufSchemasCreated;

  private final SchemaRegistryMetric avroSchemasDeleted;
  private final SchemaRegistryMetric jsonSchemasDeleted;
  private final SchemaRegistryMetric protobufSchemasDeleted;

  public MetricsContainer(SchemaRegistryConfig config) {
    this.configuredTags =
            Application.parseListToMap(config.getList(RestConfig.METRICS_TAGS_CONFIG));
    this.commitId = getCommitId();

    MetricConfig metricConfig =
            new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                            TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters =
            config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class);
    reporters.add(new JmxReporter(JMX_PREFIX));

    this.metrics = new Metrics(metricConfig, reporters, new SystemTime());

    this.isMasterNode = createMetric("master-slave-role",
            "1.0 indicates the node is the active master in the cluster and is the"
            + " node where all register schema and config update requests are "
            + "served.");

    this.apiCallsSuccess = createMetric("api-success-count", "Number of successful API calls");
    this.apiCallsFailure = createMetric("api-failure-count", "Number of failed API calls");

    this.customSchemaProviders = createMetric("custom-schema-provider-count",
            "Number of custom schema providers");

    this.schemasCreated = createMetric("registered-count", "Number of registered schemas");
    this.schemasDeleted = createMetric("deleted-count", "Number of deleted schemas");

    this.avroSchemasCreated = createMetric("avro-schemas-created",
            "Number of registered Avro schemas");

    this.avroSchemasDeleted = createMetric("avro-schemas-deleted",
            "Number of deleted Avro schemas");

    this.jsonSchemasCreated = createMetric("json-schemas-created",
            "Number of registered JSON schemas");

    this.jsonSchemasDeleted = createMetric("json-schemas-deleted",
            "Number of deleted JSON schemas");

    this.protobufSchemasCreated = createMetric("protobuf-schemas-created",
            "Number of registered Protobuf schemas");

    this.protobufSchemasDeleted = createMetric("protobuf-schemas-deleted",
            "Number of deleted Protobuf schemas");
  }

  private SchemaRegistryMetric createMetric(String name, String metricDescription) {
    return createMetric(name, name, name, metricDescription);
  }

  private SchemaRegistryMetric createMetric(String sensorName, String metricName,
                                            String metricGroup, String metricDescription) {
    MetricName mn = new MetricName(metricName, metricGroup, metricDescription, configuredTags);
    return new SchemaRegistryMetric(metrics, sensorName, mn);
  }

  public void setMaster(boolean isMaster) {
    isMasterNode.set(isMaster ? 1 : 0);
  }

  public void setCustomSchemaProvidersCount(long count) {
    customSchemaProviders.set(count);
  }

  public void apiCallSucceeded() {
    apiCallsSuccess.increment();
  }

  public void apiCallFailed() {
    apiCallsFailure.increment();
  }

  public void schemaRegistered(SchemaValue schemaValue) {
    schemasCreated.increment();
    SchemaRegistryMetric typeMetric = getSchemaTypeMetric(schemaValue, true);
    if (typeMetric != null) {
      typeMetric.increment();
    }
  }

  public void schemaDeleted(SchemaValue schemaValue) {
    schemasDeleted.increment();
    SchemaRegistryMetric typeMetric = getSchemaTypeMetric(schemaValue, false);
    if (typeMetric != null) {
      typeMetric.increment();
    }
  }

  private SchemaRegistryMetric getSchemaTypeMetric(SchemaValue sv, boolean isRegister) {
    String type = sv.getSchemaType() == null ? AvroSchema.TYPE : sv.getSchemaType();
    switch (type) {
      case AvroSchema.TYPE:
        return isRegister ? avroSchemasCreated : avroSchemasDeleted;
      case JsonSchema.TYPE:
        return isRegister ? jsonSchemasCreated : jsonSchemasDeleted;
      case ProtobufSchema.TYPE:
        return isRegister ? protobufSchemasCreated : protobufSchemasDeleted;
      default:
        return null;
    }
  }

  private static String getCommitId() {
    try {
      final String fileName = "/META-INF/MANIFEST.MF";
      final InputStream manifestFile = KafkaSchemaRegistry.class.getResourceAsStream(fileName);
      if (manifestFile != null) {
        String commitId = new Manifest(manifestFile).getMainAttributes().getValue("Commit-ID");
        if (commitId != null) {
          return commitId;
        }
        log.error("Missing Commit-ID entry");
      } else {
        log.error("Missing manifest file");
      }
    } catch (IOException e) {
      log.error("Cannot open manifest file", e);
    }
    return "Unknown";
  }
}
