package com.ebay.traffic.chocolate.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

/**
 * Created by yliu29 on 2/23/18.
 *
 * Kafka Consumer Factory
 */
public class KafkaConsumerFactory2 {
  public static <K, V> Consumer<K, V> create(Properties properties) {
    return new KafkaConsumer<K, V>(properties);
  }
}
