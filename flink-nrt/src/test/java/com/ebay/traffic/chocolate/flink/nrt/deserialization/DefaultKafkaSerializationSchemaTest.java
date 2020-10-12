package com.ebay.traffic.chocolate.flink.nrt.deserialization;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class DefaultKafkaSerializationSchemaTest {
  private DefaultKafkaSerializationSchema defaultKafkaSerializationSchema;

  @Before
  public void setUp() throws Exception {
    defaultKafkaSerializationSchema = new DefaultKafkaSerializationSchema();
  }

  @Test
  public void serialize() {
    ProducerRecord<byte[], byte[]> test = defaultKafkaSerializationSchema.serialize(new Tuple3<>("test", 1L, "hello, world!".getBytes()), System.currentTimeMillis());
    assertEquals("test", test.topic());
    assertArrayEquals("hello, world!".getBytes(), test.value());
  }
}