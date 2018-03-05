package com.ebay.traffic.chocolate.kafka;

/**
 * Created by yliu29 on 2/23/18.
 *
 * Kafka Cluster type, support we support Rheos and team maintain Kafka
 */
public enum KafkaCluster {
  RHEOS, KAFKA;

  /**
   * The delimiter used in configuration
   */
  public static final String DELIMITER = ",";

  public static KafkaCluster valueOfEx(String name) {
    if (name.equalsIgnoreCase("rheos")) {
      return RHEOS;
    } else if (name.equalsIgnoreCase("kafka")) {
      return KAFKA;
    }
    throw new IllegalArgumentException("Unsupported kafka cluster.");
  }
}