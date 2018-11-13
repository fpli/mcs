package com.ebay.app.raptor.chocolate.listener;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.common.AbstractApplicationOptions;
import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.traffic.chocolate.kafka.KafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.ebay.traffic.chocolate.kafka.KafkaCluster.DELIMITER;

/**
 * Controls the parsing of event listener application options.
 *
 * @author xiangli4
 */
public class ApplicationOptions extends AbstractApplicationOptions implements KafkaSink.KafkaConfigurable {

  /**
   * Private logging instance
   */
  private static final Logger logger = Logger.getLogger(ApplicationOptions.class);

  /**
   * Singleton instance
   */
  private static final ApplicationOptions instance = new ApplicationOptions();

  private static final String CONFIG_SUBFOLDER = "config/";

  public static final String EVENT_LISTENER_PROPERTIES_FILE = "event-listener.properties";

  public static final String INPUT_RHEOS_KAFKA_PROPERTIES_FILE = "event-listener-rheos-consumer.properties";

  public static final String SINK_KAFKA_PROPERTIES_FILE = "event-listener-kafka-producer.properties";

  public static final String SINK_RHEOS_KAFKA_PROPERTIES_FILE = "event-listener-rheos-producer.properties";

  public static final String KAFKA_IN_TOPIC = "chocolate.event-listener.kafka.consumer.topic";

  /**
   * Out Kafka cluster, can be "kafka", "rheos", "rheos,kafka", "kafka,rheos".
   */
  public static final String KAFKA_OUT_CLUSTER = "chocolate.event-listener.kafka.out";

  /**
   * prefix of out Kafka topic for channels.
   */
  // refer to com.ebay.app.raptor.chocolate.avro.ChannelType for channels.
  // for ePN:  chocolate.filter.kafka.consumer.topic.EPN
  // for display: chocolate.filter.kafka.consumer.topic.DISPLAY
  public static final String KAFKA_OUT_TOPIC_PREFIX = "chocolate.filter.kafka.producer.topic.";

  /**
   * Static driver ID
   */
  static final int DRIVER_ID = ApplicationOptionsParser.getDriverIdFromIp();

  /**
   * kafka related
   **/
  private static Properties inputRheosKafkaProperties;
  private static Properties sinkKafkaProperties;
  private static Properties sinkRheosKafkaProperties;

  private String outKafkaCluster;
  private Map<ChannelType, String> outKafkaConfigMap = new HashMap<>();

  /**
   * Application options to load from internal jar
   *
   * @throws IOException if properties could not be loaded
   */
  public static void init() throws IOException {
    instance.initInstance(loadProperties(EVENT_LISTENER_PROPERTIES_FILE));

    inputRheosKafkaProperties = loadProperties(INPUT_RHEOS_KAFKA_PROPERTIES_FILE);
    if (sinkKafkaProperties == null) {
      sinkKafkaProperties = loadProperties(SINK_KAFKA_PROPERTIES_FILE);
    }
    sinkRheosKafkaProperties = loadProperties(SINK_RHEOS_KAFKA_PROPERTIES_FILE);
    instance.initKafkaConfigs();
  }

  private static Properties loadProperties(String file) throws IOException {
    String filePath = RuntimeContext.getConfigRoot().getFile() + CONFIG_SUBFOLDER + file;
    Properties properties = new Properties();
    FileReader reader = new FileReader(filePath);
    properties.load(reader);
    reader.close();
    return properties;
  }

  /**
   * For UT purposes mainly
   *
   * @param properties to initialize using
   */
  public static void init(final Properties properties) {
    instance.initInstance(properties);
  }

  /**
   * Can't create ApplicationOptions from outside
   */
  private ApplicationOptions() {
  }

  /**
   * Return the singleton
   *
   * @return singleton instance
   */
  public static ApplicationOptions getInstance() {
    return instance;
  }

  /**
   * Only for test
   */
  public void setInputRheosProperties(Properties properties) {
    inputRheosKafkaProperties = properties;
  }

  /**
   * Get input rheos kafka properties
   *
   * @return kafka properties
   * @throws IOException
   */
  public Properties getInputRheosKafkaProperties() throws IOException {
    return inputRheosKafkaProperties;
  }

  @Override
  public String getSinkKafkaCluster() {
    return outKafkaCluster;
  }

  /**
   * Only for test
   */
  public void setSinkKafkaProperties(Properties properties) {
    sinkKafkaProperties = properties;
  }

  /**
   * Get sink kafka properties
   *
   * @param sinkCluster kafka cluster
   * @return kafka properties
   * @throws IOException
   */
  @Override
  public Properties getSinkKafkaProperties(KafkaCluster sinkCluster) throws IOException {
    if (sinkCluster == KafkaCluster.KAFKA) {
      return sinkKafkaProperties;
    } else {
      return sinkRheosKafkaProperties;
    }
  }

  /**
   * Get sink channel kafka topic map
   *
   * @return sink channel kafka topic map
   */
  public Map<ChannelType, String> getSinkKafkaConfigs() {
    return outKafkaConfigMap;
  }

  /**
   * Kafka topic configs
   */
  private void initKafkaConfigs() {


    outKafkaCluster = ApplicationOptionsParser.getStringProperty(properties, KAFKA_OUT_CLUSTER);
    String[] outKafkaClusters = outKafkaCluster.split(DELIMITER);
    if (outKafkaClusters.length > 2) {
      throw new IllegalArgumentException("too many values in " + KAFKA_OUT_CLUSTER);
    }
    Map<String, String> outChannelKafkaTopics = getByNamePrefix(KAFKA_OUT_TOPIC_PREFIX);
    for (Map.Entry<String, String> channelTopic : outChannelKafkaTopics.entrySet()) {
      ChannelType channelType = ChannelType.valueOf(channelTopic.getKey());
      String topics = channelTopic.getValue();
      String[] topicarray = topics.split(DELIMITER);
      if (topicarray.length > 2) {
        throw new IllegalArgumentException("too many values in " + KAFKA_OUT_TOPIC_PREFIX + channelTopic
          .getKey());
      }
      outKafkaConfigMap.put(channelType, topics);
    }
  }

  /**
   * @return the properties for testing purposes only
   */
  public Properties getProperties() {
    return (Properties) properties.clone();
  }


  /**
   * @return the driver ID for the filter.
   */
  public int getDriverId() {
    return DRIVER_ID;
  }
}
