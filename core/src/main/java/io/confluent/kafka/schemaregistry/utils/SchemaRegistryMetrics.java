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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SchemaRegistryMetrics {

  private final Metrics metrics;
  private final Sensor masterNodeSensor;
  private final Map<String, SchemaCountSensor> schemaCreatedByType = new ConcurrentHashMap<>();
  private final Map<String, SchemaCountSensor> schemaDeletedByType = new ConcurrentHashMap<>();

  public SchemaRegistryMetrics(SchemaRegistryConfig config) {
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
    this.masterNodeSensor = metrics.sensor("master-slave-role");

    Map<String, String> configuredTags = Application.parseListToMap(
            config.getList(RestConfig.METRICS_TAGS_CONFIG)
    );
    MetricName
            m = new MetricName("master-slave-role", "master-slave-role",
            "1.0 indicates the node is the active master in the cluster and is the"
                    + " node where all register schema and config update requests are "
                    + "served.", configuredTags);
    this.masterNodeSensor.add(m, new Value());
  }

  public void setMaster(boolean isMaster) {
    masterNodeSensor.record(isMaster ? 1.0 : 0.0);
  }

  public void schemaRegistered(SchemaValue schemaValue) {
    String type = getSchemaType(schemaValue);
    SchemaCountSensor sensor = schemaCreatedByType.computeIfAbsent(type, t -> new SchemaCountSensor(getMetricDescriptor(t)));
    sensor.increment();
  }

  public void schemaDeleted(SchemaValue schemaValue) {
    String type = getSchemaType(schemaValue);
    SchemaCountSensor sensor = schemaDeletedByType.computeIfAbsent(type, t -> new SchemaCountSensor(getMetricDescriptor(t)));
    sensor.increment();
  }

  private static String getSchemaType(SchemaValue schemaValue) {
    return schemaValue.getSchemaType() == null ? AvroSchema.TYPE : schemaValue.getSchemaType();
  }

  private class SchemaCountSensor {
    private AtomicLong count = new AtomicLong(0);
    private Sensor sensor;

    public SchemaCountSensor(MetricDescriptor md) {
      sensor = metrics.sensor(md.sensorName);
      Map<String, String> tags = new LinkedHashMap<>();
      sensor.add(new MetricName("num-schemas", md.group, md.description, tags), new Value());
    }

    public long get() {
      return count.get();
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
    NULL("qm-status", "status", "Number of registered schemas"),
    AVRO("qm-status-avro", "status_avro", "Number of registered Avro schemas"),
    JSON("qm-status-json", "status_json", "Number of registered JSON schemas"),
    PROTOBUF("qm-status-protobuf", "status_protobuf", "Number of registered Protobuf schemas");

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
