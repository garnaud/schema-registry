package io.confluent.kafka.schemaregistry.metrics;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import org.junit.Test;

public class MetricsTest extends ClusterTestHarness {

  public MetricsTest() { super(1, true); }

  @Test
  public void testBasic() throws Exception {
  }
}
