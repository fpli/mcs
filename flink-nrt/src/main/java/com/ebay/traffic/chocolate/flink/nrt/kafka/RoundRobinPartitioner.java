package com.ebay.traffic.chocolate.flink.nrt.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The "Round-Robin" partitioner, copied from Kafka 2.4.1
 *
 * This partitioning strategy can be used when user wants
 * to distribute the writes to all partitions equally. This
 * is the behaviour regardless of record key hash.
 *
 */
public class RoundRobinPartitioner implements Partitioner {
  private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();

  public void configure(Map<String, ?> configs) {}

  /**
   * Compute the partition for the given record.
   *
   * @param topic The topic name
   * @param key The key to partition on (or null if no key)
   * @param keyBytes serialized key to partition on (or null if no key)
   * @param value The value to partition on or null
   * @param valueBytes serialized value to partition on or null
   * @param cluster The current cluster metadata
   */
  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
    List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
    int numPartitions = partitions.size();
    int nextValue = nextValue(topic);
    List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
    if (!availablePartitions.isEmpty()) {
      int part = Utils.toPositive(nextValue) % availablePartitions.size();
      return availablePartitions.get(part).partition();
    } else {
      // no partitions are available, give a non-available partition
      return Utils.toPositive(nextValue) % numPartitions;
    }
  }

  private int nextValue(String topic) {
    AtomicInteger counter = topicCounterMap.computeIfAbsent(topic, k -> {
      return new AtomicInteger(0);
    });
    return counter.getAndIncrement();
  }

  public void close() {}

}