package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.*;
import io.ebay.rheos.schema.avro.RheosEventSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by yliu29 on 2/13/18.
 */
public class TestMessageSerialization {

  @Test
  public void testFilterMessageSerialization() throws Exception {
    final String topic = "misc.crossdcs.marketing-tracking.tracking-events-epn-filtered";

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
    final String topic = "misc.crossdcs.marketing-tracking.tracking-events-epn-listened";

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

  static FilterMessage newFilterMessage(long snapshotId, long publisherId, long campaignId) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setTimestamp(System.currentTimeMillis());
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setRequestHeaders("");
    message.setUri("http://");
    message.setResponseHeaders("");
    message.setChannelAction(ChannelAction.CLICK);
    message.setChannelType(ChannelType.EPN);
    message.setHttpMethod(HttpMethod.POST);
    message.setValid(true);
    message.setFilterFailed("");
    message.setSnid("");
    return message;
  }

  static ListenerMessage newListenerMessage(long snapshotId, long publisherId, long campaignId) {
    ListenerMessage message = new ListenerMessage();
    message.setSnapshotId(snapshotId);
    message.setTimestamp(System.currentTimeMillis());
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setRequestHeaders("");
    message.setUri("http://");
    message.setResponseHeaders("");
    message.setChannelAction(ChannelAction.CLICK);
    message.setChannelType(ChannelType.EPN);
    message.setHttpMethod(HttpMethod.POST);
    message.setSnid("");
    return message;
  }

  static Properties loadProperties(String fileName) throws IOException {
    Properties prop = new Properties();
    InputStream in = TestMessageSerialization.class.getClassLoader().getResourceAsStream(fileName);
    prop.load(in);
    in.close();
    return prop;
  }
}
