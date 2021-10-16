package com.ebay.traffic.chocolate.kafka;

import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.traffic.monitoring.Field;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by yliu29 on 3/15/19.
 */
public class ConsumerListener<K, V> implements ConsumerRebalanceListener {
  private static final Logger LOG = Logger.getLogger(ConsumerListener.class);

  private final Consumer<K, V> consumer;

  public ConsumerListener(Consumer<K, V> consumer) {
    this.consumer = consumer;
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

    for (TopicPartition partition : partitions) {
      long offset = consumer.position(partition);
      MonitorUtil.info("KafkaConsumerOffset-Assigned", offset,
              Field.of("topic", partition.topic()),
              Field.of("consumer", partition.partition()));
    }
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    Map<TopicPartition, OffsetAndMetadata> revoked = new HashMap<>();

    for (TopicPartition partition : partitions) {
      long offset = consumer.position(partition);
      revoked.put(partition, new OffsetAndMetadata(offset));
      MonitorUtil.info("KafkaConsumerOffset-Revoked", offset,
              Field.of("topic", partition.topic()),
              Field.of("consumer", partition.partition()));
    }

    try {
      consumer.commitSync(revoked);
    } catch (Exception e) {
      LOG.warn("Commit offset failed!", e);
      MonitorUtil.info("CommitOffsetFailed");
    }
  }

  /**
   * CommitSync
   */
  public void commitSync() {
    Set<TopicPartition> assignment = consumer.assignment();
    for (TopicPartition tp : assignment) {
      MonitorUtil.info("KafkaConsumerOffset", consumer.position(tp),
              Field.of("topic", tp.topic()),
              Field.of("consumer", tp.partition()));
    }

    try {
      consumer.commitSync();
    } catch (Exception e) {
      LOG.warn("Commit offset failed!", e);
      MonitorUtil.info("CommitOffsetFailed");
    }
  }
}
