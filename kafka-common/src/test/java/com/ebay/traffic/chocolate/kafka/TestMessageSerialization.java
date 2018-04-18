package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.*;
import io.ebay.rheos.schema.avro.RheosEventSerializer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static com.ebay.traffic.chocolate.common.TestHelper.*;

/**
 * Created by yliu29 on 2/13/18.
 */
@Ignore
public class TestMessageSerialization {

  @Test
  public void testFilterMessageSerialization() throws Exception {
    final String topic = "marketingtech.ap.tracking-events.filtered-epn";

    RheosKafkaProducer<Long, FilterMessage> producer =
            new RheosKafkaProducer<>(loadProperties("rheos-kafka-filter-producer.properties"));

    FilterMessage message = newFilterMessage(1L, 11L, 111L);

    RheosEventSerializer serializer = new RheosEventSerializer();
    byte[] bytes = serializer.serialize(topic, producer.getRheosEvent(message));

    RheosFilterMessageDeserializer deserializer = new RheosFilterMessageDeserializer();
    FilterMessage result = deserializer.deserialize(topic, bytes);
    Assert.assertTrue(message.equals(result));

    FilterMessageSerializer serializer1 = new FilterMessageSerializer();
    byte[] bytes1 = serializer1.serialize(topic, message);
    FilterMessageDeserializer deserializer1 = new FilterMessageDeserializer();
    FilterMessage result1 = deserializer1.deserialize(topic, bytes1);
    Assert.assertTrue(message.equals(result1));

    producer.close();
  }

  @Test
  public void testListenerMessageSerialization() throws Exception {
    final String topic = "marketingtech.ap.tracking-events.listened-epn";

    RheosKafkaProducer<Long, ListenerMessage> producer =
            new RheosKafkaProducer<>(loadProperties("rheos-kafka-listener-producer.properties"));

    ListenerMessage message = newListenerMessage(1L, 11L, 111L);

    RheosEventSerializer serializer = new RheosEventSerializer();
    byte[] bytes = serializer.serialize(topic, producer.getRheosEvent(message));

    RheosListenerMessageDeserializer deserializer = new RheosListenerMessageDeserializer();
    ListenerMessage result = deserializer.deserialize(topic, bytes);
    Assert.assertTrue(message.equals(result));

    ListenerMessageSerializer serializer1 = new ListenerMessageSerializer();
    byte[] bytes1 = serializer1.serialize(topic, message);
    ListenerMessageDeserializer deserializer1 = new ListenerMessageDeserializer();
    ListenerMessage result1 = deserializer1.deserialize(topic, bytes1);
    Assert.assertTrue(message.equals(result1));
  }
}
