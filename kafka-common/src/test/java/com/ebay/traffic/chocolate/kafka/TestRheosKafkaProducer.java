package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import static com.ebay.traffic.chocolate.kafka.TestMessageSerialization.*;

/**
 * Created by yliu29 on 2/14/18.
 */
public class TestRheosKafkaProducer {

  @Test
  public void testRheosKafkaProducer() throws Exception {

    final String topic = "misc.crossdcs.marketing-tracking.tracking-events-epn-filtered";

    KafkaSink.KafkaConfigurable conf = new KafkaSink.KafkaConfigurable() {

      @Override
      public String getKafkaCluster() {
        return "rheos";
      }

      @Override
      public Properties getKafkaProperties() throws IOException {
        return null;
      }

      @Override
      public Properties getRheosKafkaProperties() throws IOException {
        return TestMessageSerialization.loadProperties("rheos-kafka-filter-producer.properties");
      }
    };

    KafkaSink.initialize(conf);

    Producer<Long, FilterMessage> producer = KafkaSink.get();

    FilterMessage message1 = newFilterMessage(1L, 11L, 111L);
    FilterMessage message2 = newFilterMessage(2L, 22L, 222L);
    FilterMessage message3 = newFilterMessage(3L, 33L, 333L);

    producer.send(new ProducerRecord<>(topic, 1L, message1));
    producer.send(new ProducerRecord<>(topic, 3L, message3));
    producer.send(new ProducerRecord<>(topic, 2L, message2));
    producer.flush();
    producer.close();

    Consumer<Long, FilterMessage> consumer =
            new KafkaConsumer<>(loadProperties("rheos-kafka-filter-consumer.properties"));
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
}
