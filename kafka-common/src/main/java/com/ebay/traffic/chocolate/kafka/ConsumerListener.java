package com.ebay.traffic.chocolate.kafka;

import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by yliu29 on 3/15/19.
 */
public class ConsumerListener<K, V> implements ConsumerRebalanceListener {

  private final Metrics metrics;

  private final Consumer<K, V> consumer;
  private final Map<TopicPartition, Long> offsets = new HashMap<>();
  private final Map<TopicPartition, Long> lastCommittedOffsets = new HashMap<>();

  public ConsumerListener(Consumer<K, V> consumer) {
    this.consumer = consumer;
    this.metrics = ESMetrics.getInstance();
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

    for (TopicPartition partition : partitions) {
      long lastCommittedOffset = consumer.committed(partition).offset(); // get last committed offset
      consumer.seek(partition, lastCommittedOffset); // seek to last committed offset

      offsets.put(partition, lastCommittedOffset);
      lastCommittedOffsets.put(partition, lastCommittedOffset);
    }
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    Map<TopicPartition, OffsetAndMetadata> revoked = new HashMap<>();

    for (TopicPartition partition : partitions) {
      long offset = offsets.get(partition);
      long lastCommittedOffset = lastCommittedOffsets.get(partition);
      if (offset > lastCommittedOffset) {
        revoked.put(partition, new OffsetAndMetadata(offset));
        metrics.trace2("KafkaConsumerOffset", offset,
                Field.of("topic", partition.topic()),
                Field.of("consumer", partition.partition()));
      }
      offsets.remove(partition);
      lastCommittedOffsets.remove(partition);
    }

    consumer.commitSync(revoked);
  }

  /**
   * Update partition offset
   *
   * @param partition the partition
   * @param offset the offset
   */
  public void updatePartitionOffset(TopicPartition partition, long offset) {
    offsets.put(partition, offset); // update offset
  }

  /**
   * Get partition offset
   *
   * @param partition the partition
   * @return offset
   */
  public long getPartitionOffset(TopicPartition partition) {
    return offsets.get(partition);
  }

  /**
   * CommitSync the offsets
   */
  public void commitSync() {
    Map<TopicPartition, OffsetAndMetadata> toCommitOffsets = new HashMap<>();

    for (TopicPartition partition : offsets.keySet()) {
      long offset = offsets.get(partition);
      long lastCommittedOffset = lastCommittedOffsets.get(partition);
      if (offset > lastCommittedOffset) {
        toCommitOffsets.put(partition, new OffsetAndMetadata(offset));
        metrics.trace2("KafkaConsumerOffset", offset,
                Field.of("topic", partition.topic()),
                Field.of("consumer", partition.partition()));
      }
    }

    consumer.commitSync(toCommitOffsets);
  }
}
