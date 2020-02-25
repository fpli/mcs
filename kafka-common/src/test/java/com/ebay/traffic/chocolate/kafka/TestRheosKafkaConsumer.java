package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static com.ebay.traffic.chocolate.common.TestHelper.loadProperties;

/**
 * Created by jialili1 on 1/7/19.
 */
public class TestRheosKafkaConsumer {

  @Test
  @Ignore
  public void testRheosKafkaConsumer() throws Exception {

    final String topic = "marketing.tracking.ssl.filtered-epn";

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
