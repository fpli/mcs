package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

import static com.ebay.traffic.chocolate.common.TestHelper.loadProperties;
import static com.ebay.traffic.chocolate.common.TestHelper.newUnifiedTrackingMessage;

/**
 * Created by jialili1 on 11/5/20
 */
public class TestRheosUnifiedTrackingProducer {

  @Test
  public void testRheosKafkaProducer() throws Exception {
    final String topic = "marketing.tracking.staging.events.total";

    Producer<byte[], UnifiedTrackingMessage> producer =
        new RheosKafkaProducer<>(loadProperties("rheos-kafka-unified-tracking-producer.properties"));

    UnifiedTrackingMessage message1 = newUnifiedTrackingMessage("1");
    UnifiedTrackingMessage message2 = newUnifiedTrackingMessage("2");
    UnifiedTrackingMessage message3 = newUnifiedTrackingMessage("3");

    Callback callback = new Callback() {
      @Override
      public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
          e.printStackTrace();
        }
      }
    };

    producer.send(new ProducerRecord<>(topic, "1".getBytes(), message1), callback);
    producer.send(new ProducerRecord<>(topic, "3".getBytes(), message3), callback);
    producer.send(new ProducerRecord<>(topic, "2".getBytes(), message2), callback);
    producer.flush();
    producer.close();

    System.out.println("Producer sent 3 message.");

    Consumer<byte[], UnifiedTrackingMessage> consumer =
        new KafkaConsumer<>(loadProperties("rheos-kafka-unified-tracking-consumer.properties"));
    consumer.subscribe(Arrays.asList(topic));

    int count = 0;
    long start = System.currentTimeMillis();
    long end = start;
    while (count < 3 && (end - start < 3 * 60 * 1000)) {
      ConsumerRecords<byte[], UnifiedTrackingMessage> consumerRecords = consumer.poll(3000);
      Iterator<ConsumerRecord<byte[], UnifiedTrackingMessage>> iterator = consumerRecords.iterator();

      while (iterator.hasNext()) {
        ConsumerRecord<byte[], UnifiedTrackingMessage> record = iterator.next();
        UnifiedTrackingMessage message = record.value();
        System.out.println(message);
        count++;
      }
      end = System.currentTimeMillis();
    }

    Assert.assertTrue(count >= 1);
  }
}
