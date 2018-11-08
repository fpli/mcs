package com.ebay.app.raptor.chocolate.seed;

import com.ebay.app.raptor.chocolate.common.AbstractApplicationOptions;
import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.traffic.chocolate.kafka.KafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.kafka.KafkaSink2;
import org.apache.log4j.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.ebay.traffic.chocolate.kafka.KafkaCluster.DELIMITER;

public class ApplicationOptions extends AbstractApplicationOptions implements KafkaSink2.KafkaConfigurable {
  /** Private logging instance */
  private static final Logger logger = Logger.getLogger(ApplicationOptions.class);

  public static final String SEED_PROPERTIES_FILE = "seed.properties";
  public static final String INPUT_KAFKA_PROPERTIES_FILE = "seed-kafka-consumer.properties";
  public static final String INPUT_RHEOS_KAFKA_PROPERTIES_FILE = "seed-kafka-rheos-consumer.properties";
  public static final String SINK_KAFKA_PROPERTIES_FILE = "seed-kafka-producer.properties";
  public static final String SINK_RHEOS_KAFKA_PROPERTIES_FILE = "seed-kafka-rheos-producer.properties";
  /** Kafka Cluster name */
  public static final String KAFKA_IN_CLUSTER = "chocolate.seed.kafka.in";
  /** prefix of in Kafka topic for channels. */
  public static final String KAFKA_IN_TOPIC_PREFIX = "chocolate.seed.kafka.consumer.topic";
  /** Out Kafka cluster, can be "kafka", "rheos", "rheos,kafka", "kafka,rheos". */
  public static final String KAFKA_OUT_CLUSTER = "chocolate.seed.kafka.out";
  /** prefix of out Kafka topic for channels. */
  public static final String KAFKA_OUT_TOPIC_PREFIX = "chocolate.seed.kafka.producer.topic";
  /** Purchase Journey API EndPoint. */
  public static final String REST_SEED_PJ = "chocolate.seed.pj.endpoint";

  /** Couchbase cluster list*/
  private static final String COUCHBASE_CLUSTER = "chocolate.filter.couchbase.cluster";
  /**Couchbase bucket for campaign publisher mapping*/
  private static final String COUCHBASE_BUCKET = "chocolate.filter.couchbase.bucket";
  /**Couchbase user*/
  private static final String COUCHBASE_USER = "chocolate.filter.couchbase.user";
  /**Couchbase password*/
  private static final String COUCHBASE_PASSWORD = "chocolate.filter.couchbase.password";
  /** Couchbase connection timeout*/
  private static final String COUCHBASE_TIMEOUT = "chocolate.filter.couchbase.timeout";

  /**
   * Singleton instance
   */
  private static final ApplicationOptions instance = new ApplicationOptions();
  private static final String CONFIG_SUBFOLDER = "config/";
  /** kafka related **/
  private static Properties inputKafkaProperties;
  private static Properties inputRheosKafkaProperties;
  private static Properties sinkKafkaProperties;
  private static Properties sinkRheosKafkaProperties;
  /** PJ API connection string **/
  private String seedPJEndpoint;
  /** in kafka topic map **/
  private Map<KafkaCluster, Map<String, String>> inKafkaConfigMap = new HashMap<>();
  private String outKafkaCluster;
  private Map<String, String> outKafkaConfigMap = new HashMap<>();

  /**
   * Can't create ApplicationOptions from outside
   */
  private ApplicationOptions() {
  }

  /**
   * Application options to load from internal jar
   *
   * @throws IOException if properties could not be loaded
   */
  public static void init() throws IOException {
    instance.initInstance(loadProperties(SEED_PROPERTIES_FILE));

    if (inputKafkaProperties == null) {
      inputKafkaProperties = loadProperties(INPUT_KAFKA_PROPERTIES_FILE);
    }
    inputRheosKafkaProperties = loadProperties(INPUT_RHEOS_KAFKA_PROPERTIES_FILE);
    if (sinkKafkaProperties == null) {
      sinkKafkaProperties = loadProperties(SINK_KAFKA_PROPERTIES_FILE);
    }
    sinkRheosKafkaProperties = loadProperties(SINK_RHEOS_KAFKA_PROPERTIES_FILE);
    instance.initKafkaConfigs();
  }

  private static Properties loadProperties(String file) throws IOException {
    String filePath;
    Properties properties = new Properties();
    if(RuntimeContext.getConfigRoot() != null){
      filePath = RuntimeContext.getConfigRoot().getFile() + CONFIG_SUBFOLDER + file;
      FileReader reader = new FileReader(filePath);
      properties.load(reader);
      reader.close();
    }else{
      InputStream inputStream = ApplicationOptions.class.getClassLoader().getResourceAsStream("META-INF/configuration/Dev/config/" + file);
      properties.load(inputStream);
    }

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
   * Return the singleton
   *
   * @return singleton instance
   */
  public static ApplicationOptions getInstance() {
    return instance;
  }

  /**
   * Get input kafka properties
   *
   * @param inputCluster kafka cluster
   * @return kafka properties
   * @throws IOException
   */
  public Properties getInputKafkaProperties(KafkaCluster inputCluster) throws IOException {
    if (inputCluster == KafkaCluster.KAFKA) {
      return inputKafkaProperties;
    } else {
      return inputRheosKafkaProperties;
    }
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
   * Get input channel kafka topic map
   *
   * @return input channel kafka topic map
   */
  public Map<KafkaCluster, Map<String, String>> getInputKafkaConfigs() {
    return inKafkaConfigMap;
  }

  /**
   * Get sink channel kafka topic map
   *
   * @return sink channel kafka topic map
   */
  public Map<String, String> getSinkKafkaConfigs() {
    return outKafkaConfigMap;
  }

  /**
   * Kafka topic configs
   */
  private void initKafkaConfigs() {
    String inKafkaClusterProp = ApplicationOptionsParser.getStringProperty(properties, KAFKA_IN_CLUSTER);
    String[] inKafkaClusters = inKafkaClusterProp.split(DELIMITER);
    if (inKafkaClusters.length > 2) {
      throw new IllegalArgumentException("too many values in " + KAFKA_IN_CLUSTER);
    }
    for (String cluster : inKafkaClusters) {
      inKafkaConfigMap.put(KafkaCluster.valueOfEx(cluster), new HashMap<String, String>());
    }
    Map<String, String> inChannelKafkaTopics = getByNamePrefix(KAFKA_IN_TOPIC_PREFIX);
    for (Map.Entry<String, String> channelTopic : inChannelKafkaTopics.entrySet()) {
      String channelType = channelTopic.getKey();
      String topics = channelTopic.getValue();
      String[] topicarray = topics.split(DELIMITER);

      for (int i = 0; i < inKafkaClusters.length; i++) {
        Map<String, String> channelTopicMap =
            inKafkaConfigMap.get(KafkaCluster.valueOfEx(inKafkaClusters[i]));
        if (topicarray.length > i) {
          channelTopicMap.put(channelType, topicarray[i]);
        } else {
          channelTopicMap.put(channelType, topicarray[0]);
        }
      }
    }


    outKafkaCluster = ApplicationOptionsParser.getStringProperty(properties, KAFKA_OUT_CLUSTER);
    String[] outKafkaClusters = outKafkaCluster.split(DELIMITER);
    if (outKafkaClusters.length > 2) {
      throw new IllegalArgumentException("too many values in " + KAFKA_OUT_CLUSTER);
    }
    Map<String, String> outChannelKafkaTopics = getByNamePrefix(KAFKA_OUT_TOPIC_PREFIX);
    for (Map.Entry<String, String> channelTopic : outChannelKafkaTopics.entrySet()) {
      String channelType = channelTopic.getKey();
      String topics = channelTopic.getValue();
      String[] topicarray = topics.split(DELIMITER);
      if (topicarray.length > 2) {
        throw new IllegalArgumentException("too many values in " + KAFKA_OUT_TOPIC_PREFIX + channelTopic.getKey());
      }
      outKafkaConfigMap.put(channelType, topics);
    }
  }

  /**Get Couchbase cluster string list*/
  public String getCouchBaseCluster() {
    if (!properties.containsKey(COUCHBASE_CLUSTER)) {
      logger.fatal(COUCHBASE_CLUSTER + " not found in properties file!");
      throw new UnsupportedOperationException(COUCHBASE_CLUSTER + " not found in properties file!");
    }
    return properties.getProperty(COUCHBASE_CLUSTER);
  }

  /**Get Couchbase bucket*/
  public String getCouchBaseBucket() {
    if (!properties.containsKey(COUCHBASE_BUCKET)) {
      logger.fatal(COUCHBASE_BUCKET + " not found in properties file!");
      throw new UnsupportedOperationException(COUCHBASE_BUCKET + " not found in properties file!");
    }
    return properties.getProperty(COUCHBASE_BUCKET);
  }

  /**Get Couchbase user*/
  public String getCouchBaseUser() {
    if (!properties.containsKey(COUCHBASE_USER)) {
      logger.fatal(COUCHBASE_USER + " not found in properties file!");
      throw new UnsupportedOperationException(COUCHBASE_USER + " not found in properties file!");
    }
    return properties.getProperty(COUCHBASE_USER);
  }

  /**Get Couchbase password*/
  public String getCouchbasePassword() {
    if (!properties.containsKey(COUCHBASE_PASSWORD)) {
      logger.fatal(COUCHBASE_PASSWORD + " not found in properties file!");
      throw new UnsupportedOperationException(COUCHBASE_PASSWORD + " not found in properties file!");
    }
    return properties.getProperty(COUCHBASE_PASSWORD);
  }

  /**Get Couchbase timeout*/
  public long getCouchbaseTimeout() {
    if (!properties.containsKey(COUCHBASE_TIMEOUT)) {
      logger.fatal(COUCHBASE_TIMEOUT + " not found in properties file!");
      throw new UnsupportedOperationException(COUCHBASE_TIMEOUT + " not found in properties file!");
    }
    return Long.parseLong(properties.getProperty(COUCHBASE_TIMEOUT));
  }


  /** @return the properties for testing purposes only */
  public Properties getProperties() {
    return (Properties) properties.clone();
  }

  public String getPJEndPoint() {
    return getByNameString(REST_SEED_PJ);
  }
}
