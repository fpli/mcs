package com.ebay.traffic.chocolate.flink.nrt.kafka;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * This is the default implementation, and does not do any processing on the Kafka ConsumerRecords.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class DefaultKafkaDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<byte[], byte[]>> {
  @Override
  public ConsumerRecord<byte[], byte[]> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
    return record;
  }

  @Override
  public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
    return TypeInformation.of(new TypeHint<ConsumerRecord<byte[], byte[]>>() { });
  }

  @Override
  public boolean isEndOfStream(ConsumerRecord<byte[], byte[]> nextElement) {
    return false;
  }
}
