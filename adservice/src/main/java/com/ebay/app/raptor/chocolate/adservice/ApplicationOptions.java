package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.common.AbstractApplicationOptions;
import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.app.raptor.chocolate.adservice.util.CouchbaseClient;
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
 * Controls the parsing of adservice application options.
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

  public static final String ADSERVICE_PROPERTIES_FILE = "adservice.properties";

  public static final String SINK_KAFKA_PROPERTIES_FILE = "adservice-kafka-producer.properties";

  public static final String SINK_RHEOS_KAFKA_PROPERTIES_FILE = "adservice-rheos-producer.properties";

  /**
   * Out Kafka cluster, can be "kafka", "rheos", "rheos,kafka", "kafka,rheos".
   */
  public static final String KAFKA_OUT_CLUSTER = "chocolate.adservice.kafka.out";

  /**
   * prefix for rover rheos topic
   */
  public static final String RHEOS_INPUT_TOPIC_PREFIX = "chocolate.adservice.kafka.consumer.topic";
  /**
   * prefix of out Kafka topic for channels.
   */
  public static final String KAFKA_OUT_TOPIC_PREFIX = "chocolate.adservice.kafka.producer.topic.";

  /**
   * couchbase data source
   */
  public static final String COUCHBASE_DATASOURCE = "chocolate.adservice.couchbase.datasource";

  /**
   * Static driver ID
   */
  static final int DRIVER_ID = ApplicationOptionsParser.getDriverIdFromIp();

  /**
   * kafka related
   **/
  private static Properties consumeRheosKafkaProperties;
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
    instance.initInstance(loadProperties(ADSERVICE_PROPERTIES_FILE));
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
   * @return the driver ID.
   */
  public int getDriverId() {
    return DRIVER_ID;
  }

  @Override
  public int getKafkaGlobalConfig() {
    try {
      return CouchbaseClient.getInstance().getKafkaGlobalConfig();
    } catch (Exception e) {
    }
    return 0;
  }

  public String getCouchbaseDatasource() {
    return ApplicationOptionsParser.getStringProperty(properties, COUCHBASE_DATASOURCE);
  }
}
