package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
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
import static com.ebay.traffic.chocolate.common.TestHelper.newBehaviorMessage;

/**
 * Created by jialili1 on 8/3/20
 */
public class TestRheosBehaviorProducer {

  @Test
  public void testRheosKafkaProducer() throws Exception {
    final String topic = "marketing.tracking.staging.behavior";

    Producer<byte[], BehaviorMessage> producer =
        new RheosKafkaProducer<>(loadProperties("rheos-kafka-behavior-producer.properties"));

    BehaviorMessage message1 = newBehaviorMessage("1");
    BehaviorMessage message2 = newBehaviorMessage("2");
    BehaviorMessage message3 = newBehaviorMessage("3");

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

    Consumer<byte[], BehaviorMessage> consumer =
        new KafkaConsumer<>(loadProperties("rheos-kafka-behavior-consumer.properties"));
    consumer.subscribe(Arrays.asList(topic));

    int count = 0;
    long start = System.currentTimeMillis();
    long end = start;
    while (count < 3 && (end - start < 3 * 60 * 1000)) {
      ConsumerRecords<byte[], BehaviorMessage> consumerRecords = consumer.poll(3000);
      Iterator<ConsumerRecord<byte[], BehaviorMessage>> iterator = consumerRecords.iterator();

      while (iterator.hasNext()) {
        ConsumerRecord<byte[], BehaviorMessage> record = iterator.next();
        BehaviorMessage message = record.value();
        System.out.println(message);
        count++;
      }
      end = System.currentTimeMillis();
    }

    Assert.assertTrue(count >= 1);
  }
}
