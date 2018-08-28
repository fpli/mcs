package com.ebay.traffic.chocolate.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Created by yliu29 on 2/23/18.
 *
 * Kafka Consumer Factory
 */
public class KafkaConsumerFactory {

  public static <K, V extends GenericRecord> Consumer<K, V> create(Properties properties) {
    return new KafkaConsumer<K, V>(properties);
  }
}
