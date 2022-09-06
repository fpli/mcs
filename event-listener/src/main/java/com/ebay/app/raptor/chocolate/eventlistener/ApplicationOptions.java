package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.common.AbstractApplicationOptions;
import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.app.raptor.chocolate.eventlistener.util.CouchbaseClientV2;
import com.ebay.app.raptor.chocolate.eventlistener.util.SpringUtils;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.traffic.chocolate.kafka.KafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.ebay.traffic.chocolate.kafka.KafkaCluster.DELIMITER;

/**
 * Controls the parsing of event listener application options.
 *
 * @author xiangli4
 */
public class ApplicationOptions extends AbstractApplicationOptions implements KafkaSink.KafkaConfigurable, KafkaSink.KafkaGlobalConfig {

  /**
   * Private logging instance
   */
  private static final Logger logger = LoggerFactory.getLogger(ApplicationOptions.class);

  /**
   * Singleton instance
   */
  private static final ApplicationOptions instance = new ApplicationOptions();

  private static final String CONFIG_SUBFOLDER = "config/";

  private static final String EVENT_LISTENER_PROPERTIES_FILE = "event-listener.properties";

  private static final String SINK_KAFKA_PROPERTIES_FILE = "event-listener-kafka-producer.properties";

  private static final String SINK_RHEOS_KAFKA_PROPERTIES_FILE = "event-listener-rheos-producer.properties";

  private static final String BEHAVIOR_RHEOS_PROPERTIES_FILE = "event-listener-behavior-rheos-producer.properties";

  private static final String UNIFIED_TRACKING_RHEOS_PROPERTIES_FILE = "event-listener-unified-tracking-rheos-producer.properties";

  /**
   * Out Kafka cluster, can be "kafka", "rheos", "rheos,kafka", "kafka,rheos".
   */
  private static final String KAFKA_OUT_CLUSTER = "chocolate.event-listener.kafka.out";

  /**
   * prefix of out Kafka topic for channels.
   */
  private static final String KAFKA_OUT_TOPIC_PREFIX = "chocolate.event-listener.kafka.producer.topic.";

  /**
   * unified tracking topic
   */
  private static final String RHEOS_OUT_UNIFIED_TRACKING_TOPIC = "chocolate.event-listener.kafka.producer.unified.tracking.topic";

  /**
   * couchbase data source
   */
  private static final String COUCHBASE_DATASOURCE = "chocolate.event-listener.couchbase.datasource";

  /**
   * Environment
   */
  private static final String ENVIRONMENT = "chocolate.event-listener.env";

  /**
   * Duplicate Itm Click Kafka Topic
   */
  private static final String INTERNAL_CLICK_TOPIC = "chocolate.event-listener.kafka.producer.duplicateclick.topic";

  /**
   * default driver ID
   */
  private int driverId;

  /**
   * kafka related
   **/
  private static Properties sinkKafkaProperties;
  private static Properties sinkRheosKafkaProperties;
  private static Properties behaviorRheosProperties;
  private static Properties unifiedTrackingRheosProperties;

  private String outKafkaCluster;
  private Map<ChannelType, String> outKafkaConfigMap = new HashMap<>();

  /**
   * Application options to load from internal jar
   *
   * @throws IOException if properties could not be loaded
   */
  public static synchronized void init() throws IOException {
    instance.initInstance(loadProperties(EVENT_LISTENER_PROPERTIES_FILE));

    if (sinkKafkaProperties == null) {
      sinkKafkaProperties = loadProperties(SINK_KAFKA_PROPERTIES_FILE);
    }
    sinkRheosKafkaProperties = loadProperties(SINK_RHEOS_KAFKA_PROPERTIES_FILE);

    behaviorRheosProperties = loadProperties(BEHAVIOR_RHEOS_PROPERTIES_FILE);

    unifiedTrackingRheosProperties = loadProperties(UNIFIED_TRACKING_RHEOS_PROPERTIES_FILE);

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

  @Override
  public String getSinkKafkaCluster() {
    return outKafkaCluster;
  }

  /**
   * Only for test
   */
  public static synchronized void setSinkKafkaProperties(Properties properties) {
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
   * Get behavior rheos properties
   */
  public Properties getBehaviorRheosProperties() {
    return behaviorRheosProperties;
  }

  /**
   * Get unified tracking rheos properties
   */
  public Properties getUnifiedTrackingRheosProperties() {
    return unifiedTrackingRheosProperties;
  }

  /**
   * Get produce unified tracking topic
   */
  public String getUnifiedTrackingTopic() {
    return ApplicationOptionsParser.getStringProperty(properties, RHEOS_OUT_UNIFIED_TRACKING_TOPIC);
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
   * @return the driver ID for the event-listener.
   */
  public int getDriverId() {
    return driverId;
  }

  public void setDriverId(final int newDriverId) {
    driverId = newDriverId;
  }

  @Override
  public int getKafkaGlobalConfig() {
    try {
      CouchbaseClientV2 couchbaseClientV2 = SpringUtils.getBean("CouchbaseClientV2", CouchbaseClientV2.class);
      return couchbaseClientV2.getKafkaGlobalConfig();
    } catch (Exception e) {
    }
    return 0;
  }

  public String getCouchbaseDatasource() {
    return ApplicationOptionsParser.getStringProperty(properties, COUCHBASE_DATASOURCE);
  }

  /**
   * Get environment
   */
  public String getEnvironment() {
    return ApplicationOptionsParser.getStringProperty(properties, ENVIRONMENT);
  }

  /**
   * Get Duplicate Itm Click Topic
   */
  public String getInternalItmClickTopic() {
    if (!properties.containsKey(INTERNAL_CLICK_TOPIC)) {
      logger.error(INTERNAL_CLICK_TOPIC + " not found in properties file!");
      throw new UnsupportedOperationException(INTERNAL_CLICK_TOPIC + " not found in properties file!");
    }
    return properties.getProperty(INTERNAL_CLICK_TOPIC);
  }
}
