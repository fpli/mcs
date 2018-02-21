package com.ebay.traffic.kafka;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import io.ebay.rheos.schema.avro.RheosEventSerializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * Created by yliu29 on 2/14/18.
 */
public class TestRheosKafkaProducer {

  @Test
  public void testSerialization() throws Exception {
    final String topic = "misc.crossdcs.marketing-tracking.tracking-events-epn-filtered";

    RheosKafkaProducer<Long, FilterMessage> producer =
            new RheosKafkaProducer<>(loadProperties("rheos-kafka-producer.properties"));

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
  public void testRheosKafkaProducer() throws Exception {

    final String topic = "misc.crossdcs.marketing-tracking.tracking-events-epn-filtered";

    Producer<Long, FilterMessage> producer =
            new RheosKafkaProducer<>(loadProperties("rheos-kafka-producer.properties"));

    FilterMessage message1 = newFilterMessage(1L, 11L, 111L);
    FilterMessage message2 = newFilterMessage(2L, 22L, 222L);
    FilterMessage message3 = newFilterMessage(3L, 33L, 333L);

    producer.send(new ProducerRecord<>(topic, 1L, message1));
    producer.send(new ProducerRecord<>(topic, 3L, message3));
    producer.send(new ProducerRecord<>(topic, 2L, message2));
    producer.flush();
    producer.close();

    Consumer<Long, FilterMessage> consumer =
            new KafkaConsumer<>(loadProperties("rheos-kafka-consumer.properties"));
    consumer.subscribe(Arrays.asList(topic));

    int count = 0;
    long start = System.currentTimeMillis();
    long end = start;
    while (count < 1 && (end - start < 3 * 60 * 1000)) {
      ConsumerRecords<Long, FilterMessage> consumerRecords = consumer.poll(3000);
      Iterator<ConsumerRecord<Long, FilterMessage>> iterator = consumerRecords.iterator();

      while (iterator.hasNext()) {
        ConsumerRecord<Long, FilterMessage> record = iterator.next();
        FilterMessage message = record.value();
        System.out.println(message);
        count++;
      }
      end = System.currentTimeMillis();
    }

    Assert.assertTrue(count >= 1);
  }

  private FilterMessage newFilterMessage(long snapshotId, long publisherId, long campaignId) {
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

  private Properties loadProperties(String fileName) throws IOException {
    Properties prop = new Properties();
    InputStream in = getClass().getClassLoader().getResourceAsStream(fileName);
    prop.load(in);
    in.close();
    return prop;
  }
}
