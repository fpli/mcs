package com.ebay.app.raptor.chocolate.filter;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.common.AbstractApplicationOptions;
import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleConfig;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleContent;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.traffic.chocolate.kafka.KafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.ebay.traffic.chocolate.kafka.KafkaCluster.DELIMITER;

/**
 * Controls the parsing of Chocolate application options.
 * 
 * @author jepounds
 */
public class ApplicationOptions extends AbstractApplicationOptions implements KafkaSink.KafkaConfigurable {

    /** Private logging instance */
    private static final Logger logger = Logger.getLogger(ApplicationOptions.class);

    /**
     * Singleton instance
     */
    private static final ApplicationOptions instance = new ApplicationOptions();

    private static final String CONFIG_SUBFOLDER = "config/";

    public static final String FILTER_PROPERTIES_FILE = "filter.properties";

    public static final String INPUT_KAFKA_PROPERTIES_FILE = "filter-kafka-consumer.properties";

    public static final String INPUT_RHEOS_KAFKA_PROPERTIES_FILE = "filter-kafka-rheos-consumer.properties";

    public static final String SINK_KAFKA_PROPERTIES_FILE = "filter-kafka-producer.properties";

    public static final String SINK_RHEOS_KAFKA_PROPERTIES_FILE = "filter-kafka-rheos-producer.properties";

    /** Zookeeper connection string */
    public static final String ZK_CONNECT_PROPERTY = "chocolate.filter.zkconnect";

    /** In Kafka cluster, can be "kafka", "rheos", "rheos,kafka", "kafka,rheos". */
    public static final String KAFKA_IN_CLUSTER = "chocolate.filter.kafka.in";

    /** prefix of in Kafka topic for channels. */
    // refer to com.ebay.app.raptor.chocolate.avro.ChannelType for channels.
    // for ePN:  chocolate.filter.kafka.consumer.topic.EPN
    // for display: chocolate.filter.kafka.consumer.topic.DISPLAY
    public static final String KAFKA_IN_TOPIC_PREFIX = "chocolate.filter.kafka.consumer.topic.";

    /** Out Kafka cluster, can be "kafka", "rheos", "rheos,kafka", "kafka,rheos". */
    public static final String KAFKA_OUT_CLUSTER = "chocolate.filter.kafka.out";

    /** prefix of out Kafka topic for channels. */
    // refer to com.ebay.app.raptor.chocolate.avro.ChannelType for channels.
    // for ePN:  chocolate.filter.kafka.consumer.topic.EPN
    // for display: chocolate.filter.kafka.consumer.topic.DISPLAY
    public static final String KAFKA_OUT_TOPIC_PREFIX = "chocolate.filter.kafka.producer.topic.";

    /** Configurable Filter Rules **/
    public static Map<ChannelType, Map<String, FilterRuleContent>> filterRuleConfigMap = new HashMap<ChannelType,
        Map<String, FilterRuleContent>>();

    /** Zookeeper connection string for publisher cache. */
    public static final String PUBLISHER_CACHE_ZK_CONNECT = "chocolate.filter.publisher.cache.zkconnect";

    /** Driver ID path for Zookeeper connections. */
    public static final String ZK_DRIVER_ID_PATH = "chocolate.filter.driverid.zkrootdir";

    /** Zookeeper root directory for the Chocolate filter publisher cache */
    public static final String PUBLISHER_CACHE_ZKROOTDIR = "chocolate.filter.cache.zkrootdir";

    /** Static driver ID */
    static final int DRIVER_ID = ApplicationOptionsParser.getDriverIdFromIp();

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

    /** zk connection string **/
    private String zkConnectionString;

    /** zk connection string for publisher cache **/
    private String zkConnectionStringForPublisherCache;

    /** kafka related **/
    private static Properties inputKafkaProperties;
    private static Properties inputRheosKafkaProperties;
    private static Properties sinkKafkaProperties;
    private static Properties sinkRheosKafkaProperties;

    /** in kafka topic map **/
    private Map<KafkaCluster, Map<ChannelType, String>> inKafkaConfigMap = new HashMap<>();
    private String outKafkaCluster;
    private Map<ChannelType, String> outKafkaConfigMap = new HashMap<>();

    /**
     * Application options to load from internal jar
     *
     * @throws IOException if properties could not be loaded
     */
    public static void init() throws IOException {
        instance.initInstance(loadProperties(FILTER_PROPERTIES_FILE));

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
        String filePath = RuntimeContext.getConfigRoot().getFile() + CONFIG_SUBFOLDER + file;
        Properties properties = new Properties();
        FileReader reader = new FileReader(filePath);
        properties.load(reader);
        reader.close();
        return properties;
    }

    /**
     * Application options to load from internal jar
     *
     * @param  fileName load file from
     * @throws IOException if file could not be loaded
     */
    public static void initFilterRuleConfig(String fileName) throws IOException {
        InputStream inputStream = null;
        String jsonTxt = null;
        try {
            File file = new File(RuntimeContext.getConfigRoot().getFile() + fileName);
            inputStream = new FileInputStream(file);
            jsonTxt = IOUtils.toString(inputStream, "UTF-8");
        } catch (IOException e) {
            throw e;
        } finally {
            if(inputStream != null){
                inputStream.close();
            }
        }
        FilterRuleConfig[] filterRuleConfigArray = new Gson().fromJson(jsonTxt, FilterRuleConfig[].class);

        //Get Default Rule
        Map<String, FilterRuleContent> defaultRulesMap = new HashMap<String, FilterRuleContent>();
        for(FilterRuleConfig ruleConfig : filterRuleConfigArray){
            if(ruleConfig.getChannelType().equals(ChannelType.DEFAULT)){
                for(FilterRuleContent frc : ruleConfig.getFilterRules()){
                    defaultRulesMap.put(frc.getRuleName(), frc);
                }
                break;
            }
        }
        //Set All Channel Rules into HashMap
        for(FilterRuleConfig ruleConfig : filterRuleConfigArray){
            Map<String, FilterRuleContent> filterRuleConentMap = new HashMap<String, FilterRuleContent>();
            filterRuleConentMap.putAll(defaultRulesMap);
            for(FilterRuleContent ruleConent : ruleConfig.getFilterRules()){
                filterRuleConentMap.put(ruleConent.getRuleName(), ruleConent);
            }
            filterRuleConfigMap.put(ruleConfig.getChannelType(), filterRuleConentMap);
        }
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
    private ApplicationOptions() {}

    /**
     * Return the singleton
     * @return singleton instance
     */
    public static ApplicationOptions getInstance() {
        return instance;
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

    /**
     * Gets Zookeeper properties.
     */
    public String getZookeeperString() {
        if (zkConnectionString == null) {
            if (!properties.containsKey(ZK_CONNECT_PROPERTY)) {
                logger.fatal(ZK_CONNECT_PROPERTY + " not found in properties file!");
                throw new UnsupportedOperationException(ZK_CONNECT_PROPERTY + " not found in properties file!");
            }
            zkConnectionString = properties.getProperty(ZK_CONNECT_PROPERTY);
        }
        return zkConnectionString;
    }

    /**
     * Only for test
     */
   public void setZkConnectionString(String zkConnectionString) {
        this.zkConnectionString = zkConnectionString;
    }

    /**
     * Only for test
     */
    public void setInputKafkaProperties(Properties properties) {
        inputKafkaProperties = properties;
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
    public Map<KafkaCluster, Map<ChannelType, String>> getInputKafkaConfigs() {
        return inKafkaConfigMap;
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
        String inKafkaClusterProp = ApplicationOptionsParser.getStringProperty(properties, KAFKA_IN_CLUSTER);
        String[] inKafkaClusters = inKafkaClusterProp.split(DELIMITER);
        if (inKafkaClusters.length > 2) {
            throw new IllegalArgumentException("too many values in " + KAFKA_IN_CLUSTER);
        }
        for (String cluster : inKafkaClusters) {
            inKafkaConfigMap.put(KafkaCluster.valueOfEx(cluster), new HashMap<ChannelType, String>());
        }
        Map<String, String> inChannelKafkaTopics = getByNamePrefix(KAFKA_IN_TOPIC_PREFIX);
        for (Map.Entry<String, String> channelTopic : inChannelKafkaTopics.entrySet()) {
            ChannelType channelType = ChannelType.valueOf(channelTopic.getKey());
            String topics = channelTopic.getValue();
            String[] topicarray = topics.split(DELIMITER);
            if (topicarray.length > 2) {
                throw new IllegalArgumentException("too many values in " + KAFKA_IN_TOPIC_PREFIX + channelTopic.getKey());
            }

            for (int i = 0; i < inKafkaClusters.length; i++) {
                Map<ChannelType, String> channelTopicMap =
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
            ChannelType channelType = ChannelType.valueOf(channelTopic.getKey());
            String topics = channelTopic.getValue();
            String[] topicarray = topics.split(DELIMITER);
            if (topicarray.length > 2) {
                throw new IllegalArgumentException("too many values in " + KAFKA_OUT_TOPIC_PREFIX + channelTopic.getKey());
            }
            outKafkaConfigMap.put(channelType, topics);
        }
    }

    /** @return the properties for testing purposes only */
    public Properties getProperties() {
        return (Properties) properties.clone();
    }

    /**
     * @return PublisherCache Zookeeper connection string.
     */
    public String getPublisherCacheZkConnectString() {
        if (zkConnectionStringForPublisherCache == null) {
            if (!properties.containsKey(PUBLISHER_CACHE_ZK_CONNECT)) {
                logger.fatal(PUBLISHER_CACHE_ZK_CONNECT + " not found in properties file!");
                throw new UnsupportedOperationException(PUBLISHER_CACHE_ZK_CONNECT
                        + " not found in properties file!");
            }

            zkConnectionStringForPublisherCache = properties.getProperty(PUBLISHER_CACHE_ZK_CONNECT);
        }

        return zkConnectionStringForPublisherCache;
    }

    /**
     * For test
     */
    public void setZkConnectionStringForPublisherCache(String zkConnectionString) {
        this.zkConnectionStringForPublisherCache = zkConnectionString;
    }

    /**
     * @return Zookeeper driver ID node for the Chocolate filter.
     */
    public String getZkDriverIdNode() {
        String root = ApplicationOptionsParser.getStringProperty(properties,
                ZK_DRIVER_ID_PATH);
        // Now append a trailing '/' if not present.
        if (!root.endsWith("/")) root += "/";

        // Now append the driver ID.
        root += Integer.toString(this.getDriverId());

        // Get the
        return root;
    }

    /**
     * @return Zookeeper root directory for the Chocolate filter publisher cache.
     */
    public String getPublisherCacheZkRoot() {
        return ApplicationOptionsParser.getStringProperty(properties,
                PUBLISHER_CACHE_ZKROOTDIR);
    }

    /**
     * @return the driver ID for the filter.
     */
    public int getDriverId() {
        return DRIVER_ID;
    }
}
