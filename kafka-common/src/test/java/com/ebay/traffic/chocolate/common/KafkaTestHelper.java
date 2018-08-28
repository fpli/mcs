package com.ebay.traffic.chocolate.common;

import java.io.IOException;

/**
 * Created by yliu29 on 3/3/18.
 *
 * Singleton of kafka cluster.
 */
public class KafkaTestHelper {

  private static MiniKafkaCluster kafkaCluster = null;
  private static int ref = 0;

  /**
   * When the method is invoked for the first time, a mini kafka cluster is created.
   * Otherwise, return the same kafka with reference count increased.
   *
   * @return the kafka cluster
   * @throws IOException
   */
  public synchronized static MiniKafkaCluster newKafkaCluster() throws IOException {
    if (kafkaCluster == null) {
      kafkaCluster = new MiniKafkaCluster();
      kafkaCluster.start();
    }

    ref++;
    return kafkaCluster;
  }

  /**
   * Shutdown the mini kafka cluster if reference count is 0.
   *
   * @throws IOException
   */
  public synchronized static void shutdown() throws IOException {
    if (--ref == 0) {
      kafkaCluster.shutdown();
      kafkaCluster = null;
    }
  }
}
