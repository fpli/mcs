package com.ebay.traffic.chocolate.common;

import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.utils.Time;
import scala.Option;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.util.*;

/**
 * Created by yliu29 on 3/2/18.
 *
 * A light-weight mini kafka cluster, by default, it only contains one broker server.
 */
public class MiniKafkaCluster {

  private static final int DEFAULT_BROKERS = 1;
  private static final int DEFAULT_PARTITIONS = 3;

  // Mini zookeeper cluster
  private final MiniZookeeperCluster zookeeper;
  private final boolean managedZk;

  private final int brokerNum;
  private List<KafkaConfig> brokerConfigs;
  private List<KafkaServer> brokerServers;

  private boolean started = false;
  private boolean shutdown = false;

  private final String dir;

  public MiniKafkaCluster() {
    this(DEFAULT_BROKERS, null);
  }

  public MiniKafkaCluster(MiniZookeeperCluster zookeeper) {
    this(DEFAULT_BROKERS, zookeeper);
  }

  public MiniKafkaCluster(int brokerNum, MiniZookeeperCluster zookeeper) {
    assert brokerNum > 0;

    this.brokerNum = brokerNum;
    if (zookeeper == null) {
      this.managedZk = true;
      this.zookeeper = new MiniZookeeperCluster();
    } else {
      this.managedZk = false;
      this.zookeeper = zookeeper;
    }

    try {
      dir = TestHelper.createTempDir("kafka").getCanonicalPath();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Start the kafka server, it internally starts mini zookeeper cluster.
   *
   * @throws IOException
   */
  public void start() throws IOException {
    if (started) {
      throw new IllegalStateException("Already setup, cannot setup again");
    }
    started = true;

    // Start Zookeeper
    zookeeper.start();

    brokerConfigs = getBrokerConfig(brokerNum);
    brokerServers = new ArrayList<>(brokerConfigs.size());
    for (KafkaConfig config : brokerConfigs) {
      brokerServers.add(startBroker(config));
    }
  }

  /**
   * Return the mini zookeeper cluster used by kafka cluster
   *
   * @return mini zookeeper
   */
  public MiniZookeeperCluster getZookeeper() {
    return zookeeper;
  }

  /**
   * Shutdown the kafka cluster
   *
   * @throws IOException
   */
  public void shutdown() throws IOException {
    assert started;
    assert !shutdown;
    shutdown = true;
    started = false;

    for (KafkaServer broker : brokerServers) {
      broker.shutdown();
    }

    if (managedZk) {
      zookeeper.shutdown();
    }
  }

  private String getBootstrapServers() {
    StringBuilder brokers = new StringBuilder();
    for (int i = 0; i < brokerConfigs.size(); ++i) {
      KafkaConfig config = brokerConfigs.get(i);
      brokers.append((i > 0) ? "," : "")
              .append(config.hostName())
              .append(":")
              .append(config.port());
    }
    return brokers.toString();
  }

  /**
   * Create a producer for the kafka.
   *
   * @param keySerializerClass key serializer class
   * @param valueSerializerClass value serializer class
   * @return the kafka producer
   */
  public <K, V> Producer<K, V> createProducer(Class<?> keySerializerClass,
                                              Class<?> valueSerializerClass) {
    Properties properties = getProducerProperties(keySerializerClass, valueSerializerClass);
    Producer<K, V> producer = new KafkaProducer<>(properties);

    return producer;
  }

  /**
   * Create a consumer for the kafka.
   *
   * @param keyDeserializerClass key deserializer class
   * @param valueDeserializerClass value deserializer class
   * @return the kafka consumer
   */
  public <K, V> Consumer<K, V> createConsumer(Class<?> keyDeserializerClass,
                                              Class<?> valueDeserializerClass) {
    return createConsumer(keyDeserializerClass, valueDeserializerClass, "testgroupID" + random.nextInt());
  }

  /**
   * Create a consumer for the kafka.
   *
   * @param keyDeserializerClass key deserializer class
   * @param valueDeserializerClass value deserializer class
   * @param groupID the consumer group
   * @return the kafka consumer
   */
  public <K, V> Consumer<K, V> createConsumer(Class<?> keyDeserializerClass,
                                              Class<?> valueDeserializerClass,
                                              String groupID) {
    Properties properties = getConsumerProperties(
            keyDeserializerClass, valueDeserializerClass, groupID);
    Consumer<K, V> consumer = new KafkaConsumer<>(properties);

    return consumer;
  }

  /**
   * Get producer properties for the kafka
   *
   * @param keySerializerClass key deserializer class
   * @param valueSerializerClass value deserializer class
   * @return the producer properties
   */
  public Properties getProducerProperties(Class<?> keySerializerClass,
                                          Class<?> valueSerializerClass) {
    Properties props = new Properties();
    props.put("bootstrap.servers", getBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());
    props.put("request.timeout.ms", "10000");
    props.put("retry.backoff.ms", "500");
    props.put("delivery.timeout.ms", 30000);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("acks", "all");
    props.put("retries", 0);
    return props;
  }

  static Random random = new Random();

  /**
   * Get consumer properties for the kafka
   *
   * @param keyDeserializerClass key deserializer class
   * @param valueDeserializerClass value deserializer class
   * @return the consumer properties
   */
  public Properties getConsumerProperties(Class<?> keyDeserializerClass,
                                          Class<?> valueDeserializerClass) {
    return getConsumerProperties(keyDeserializerClass,
            valueDeserializerClass, "testgroupID" + random.nextInt());
  }

  /**
   * Get consumer properties for the kafka
   *
   * @param keyDeserializerClass key deserializer class
   * @param valueDeserializerClass value deserializer class
   * @param groupID the consumer group
   * @return the consumer properties
   */
  public Properties getConsumerProperties(Class<?> keyDeserializerClass,
                                          Class<?> valueDeserializerClass,
                                          String groupID) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", getBootstrapServers());
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            keyDeserializerClass.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            valueDeserializerClass.getName());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

  private List<KafkaConfig> getBrokerConfig(int brokers) {
    assert brokers >=1;
    assert dir != null;
    assert zookeeper != null;

    try {
      Properties properties = new Properties();
      properties.put("zookeeper.connect", zookeeper.getConnectionString());
      properties.put("host.name", "127.0.0.1");
      properties.put("log.dir", TestHelper.createTempDir(dir, "log").getCanonicalPath());
      properties.put("log.flush.interval.messages", "1");
      properties.put("num.partitions", String.valueOf(DEFAULT_PARTITIONS));
      properties.put("default.replication.factor", String.valueOf(brokers));
      properties.put("auto.create.topics.enable", "true");
      properties.put("offsets.topic.replication.factor", "1");

      List<KafkaConfig> configs = new ArrayList<>(brokers);
      for (int i = 0; i < brokers; i++) {
        Properties props = new Properties();
        props.putAll(properties);
        props.put("broker.id", String.valueOf(i + 1));
        props.put("port", String.valueOf(TestHelper.getRandomPort()));

        configs.add(new KafkaConfig(props));
      }

      return configs;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private KafkaServer startBroker(KafkaConfig config) {
    KafkaServer server = new KafkaServer(config, new SystemTime(), Option.<String>empty(),
            JavaConversions.asScalaBuffer(Collections.<KafkaMetricsReporter>emptyList()));
    server.startup();
    return server;
  }

  private static class SystemTime implements Time {
    @Override
    public long milliseconds() {
      return System.currentTimeMillis();
    }

    @Override
    public long hiResClockMs() {
      return System.currentTimeMillis();
    }

    @Override
    public long nanoseconds() {
      return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
        // Ignore
      }
    }
  }
}
