package com.ebay.traffic.chocolate.flink.nrt.deserialization;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DefaultKafkaDeserializationSchemaTest {
  private DefaultKafkaDeserializationSchema defaultKafkaDeserializationSchema;
  private ConsumerRecord<byte[], byte[]> consumerRecord;

  @Before
  public void setUp() throws Exception {
    defaultKafkaDeserializationSchema = new DefaultKafkaDeserializationSchema();
    byte[] key = "key".getBytes();
    byte[] value = "value".getBytes();
    consumerRecord = new ConsumerRecord<>("test", 0, 0L, key, value);
  }

  @Test
  public void deserialize() throws Exception {
    assertEquals(defaultKafkaDeserializationSchema.deserialize(consumerRecord), consumerRecord);
  }

  @Test
  public void getProducedType() {
    TypeInformation<ConsumerRecord<byte[], byte[]>> producedType = defaultKafkaDeserializationSchema.getProducedType();
    assertEquals("org.apache.kafka.clients.consumer.ConsumerRecord", producedType.getTypeClass().getName());
  }

  @Test
  public void isEndOfStream() {
    assertFalse(defaultKafkaDeserializationSchema.isEndOfStream(consumerRecord));
  }
}