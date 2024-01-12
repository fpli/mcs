package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.avro.AkamaiMessage;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericRecord;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static com.ebay.traffic.chocolate.common.TestHelper.loadProperties;
import static com.ebay.traffic.chocolate.common.TestHelper.newAkamaiMessage;

/**
 * Created by jialili1 on 11/5/20
 */
public class TestRheosAkamaiProducer {

  @Test
  public void testRheosKafkaProducer() throws Exception {
    final String topic = "marketing.tracking.staging.akamailog.event";

    Producer<byte[], AkamaiMessage> producer =
        new RheosKafkaProducer<>(loadProperties("rheos-kafka-akamai-producer.properties"));

    AkamaiMessage message1 = newAkamaiMessage("1");
    AkamaiMessage message2 = newAkamaiMessage("2");
    AkamaiMessage message3 = newAkamaiMessage("3");

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

    Consumer<byte[], RheosEvent> consumer =
        new KafkaConsumer<>(loadProperties("rheos-kafka-akamai-consumer.properties"));
    consumer.subscribe(Arrays.asList(topic));

    int count = 0;
    long start = System.currentTimeMillis();
    long end = start;
    Map<String, Object> config = new HashMap<>();
    // for staging
    config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");
    // for production
    //config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.stratus.ebay.com");
    GenericRecordDomainDataDecoder decoder = new GenericRecordDomainDataDecoder(config);
    while (count < 3 && (end - start < 5 * 60 * 1000)) {
      ConsumerRecords<byte[], RheosEvent> consumerRecords = consumer.poll(3000);
      Iterator<ConsumerRecord<byte[], RheosEvent>> iterator = consumerRecords.iterator();
      while (iterator.hasNext()) {
        System.out.println(decoder.decode(iterator.next().value()));
        count++;
      }

      end = System.currentTimeMillis();
    }

    Assert.assertTrue(count >= 1);
  }
}
