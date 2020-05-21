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

package io.confluent.kafka.schemaregistry.utils;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.utils.SystemTime;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SchemaRegistryMetrics {

  private final Metrics metrics;
  private final Map<String, String> configuredTags;

  private final Map<String, SchemaCountSensor> schemaCreatedByType = new ConcurrentHashMap<>();
  private final Map<String, SchemaCountSensor> schemaDeletedByType = new ConcurrentHashMap<>();

  private final SchemaRegistryMetric masterNodeMetric;
  private final SchemaRegistryMetric schemasCreated;
  private final SchemaRegistryMetric schemasDeleted;
  private final SchemaRegistryMetric customSchemaProviders;
  private final SchemaRegistryMetric apiCallsSuccess;
  private final SchemaRegistryMetric apiCallsFailure;

  public SchemaRegistryMetrics(SchemaRegistryConfig config) {
    this.configuredTags =
            Application.parseListToMap(config.getList(RestConfig.METRICS_TAGS_CONFIG));

    MetricConfig metricConfig =
            new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG),
                            TimeUnit.MILLISECONDS);
    List<MetricsReporter> reporters =
            config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class);

    String jmxPrefix = "kafka.schema.registry";
    reporters.add(new JmxReporter(jmxPrefix));

    this.metrics = new Metrics(metricConfig, reporters, new SystemTime());
    this.masterNodeMetric = new SchemaRegistryMetric(metrics, "master-slave-role",
            new MetricName("master-slave-role", "master-slave-role",
            "1.0 indicates the node is the active master in the cluster and is the"
                    + " node where all register schema and config update requests are "
                    + "served.", configuredTags));

    this.schemasCreated = new SchemaRegistryMetric(metrics, "registered-count",
            new MetricName("num-schemas", "count", "Number of registered schemas",
                           configuredTags));
    this.schemasDeleted = new SchemaRegistryMetric(metrics, "deleted-count",
            new MetricName("num-schemas", "count", "Number of deleted schemas", configuredTags));

    this.customSchemaProviders = new SchemaRegistryMetric(metrics, "custom-schema-provider-count",
            new MetricName("custom-count", "count", "Number of custom schema providers",
                    configuredTags));

    this.apiCallsSuccess = new SchemaRegistryMetric(metrics, "api-success",
            new MetricName("api-success-count", "count", "Number of successfull API calls",
                    configuredTags));

    this.apiCallsFailure = new SchemaRegistryMetric(metrics, "api-failure",
            new MetricName("api-failure-count", "failure-count", "Number of failed API calls",
                    configuredTags));
  }

  public void setMaster(boolean isMaster) {
    masterNodeMetric.set(isMaster ? 1 : 0);
  }

  public void setCustomSchemaProviders(long count) {
    customSchemaProviders.set(count);
  }

  public void schemaRegistered(SchemaValue schemaValue) {
    schemasCreated.increment();
    String type = getSchemaType(schemaValue);
    SchemaCountSensor sensor = schemaCreatedByType.computeIfAbsent(type,
        t -> new SchemaCountSensor(getMetricDescriptor(t)));
    sensor.increment();
  }

  public void schemaDeleted(SchemaValue schemaValue) {
    schemasDeleted.increment();
    String type = getSchemaType(schemaValue);
    SchemaCountSensor sensor = schemaDeletedByType.computeIfAbsent(type,
        t -> new SchemaCountSensor(getMetricDescriptor(t)));
    sensor.increment();
  }

  private static String getSchemaType(SchemaValue schemaValue) {
    return schemaValue.getSchemaType() == null ? AvroSchema.TYPE : schemaValue.getSchemaType();
  }

  private class SchemaCountSensor {
    private final AtomicLong count = new AtomicLong();
    private final Sensor sensor;

    public SchemaCountSensor(MetricDescriptor md) {
      sensor = metrics.sensor(md.sensorName);
      sensor.add(new MetricName("num-schemas", md.group, md.description, configuredTags),
                 new Value());
    }

    public void increment() {
      sensor.record(count.addAndGet(1));
    }
  }

  private static MetricDescriptor getMetricDescriptor(String type) {
    MetricDescriptor md = metricDescriptorMap.get(type);
    if (md == null) {
      throw new IllegalArgumentException("Invalid schema type: " + type);
    } else {
      return md;
    }
  }

  private static final Map<String, MetricDescriptor> metricDescriptorMap =
          ImmutableMap.of(AvroSchema.TYPE, MetricDescriptor.AVRO,
                  JsonSchema.TYPE, MetricDescriptor.JSON,
                  ProtobufSchema.TYPE, MetricDescriptor.PROTOBUF);

  private enum MetricDescriptor {
    TOTAL("count", "count", "Number of registered schemas"),
    AVRO("count-avro", "count_avro", "Number of registered Avro schemas"),
    JSON("count-json", "count_json", "Number of registered JSON schemas"),
    PROTOBUF("count-protobuf", "count_protobuf", "Number of registered Protobuf schemas");

    public final String group;
    public final String description;
    public final String sensorName;

    MetricDescriptor(String sensorName, String group, String description) {
      this.sensorName = sensorName;
      this.group = group;
      this.description = description;
    }
  }
}
