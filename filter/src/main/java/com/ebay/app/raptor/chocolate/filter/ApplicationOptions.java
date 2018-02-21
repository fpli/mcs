package com.ebay.app.raptor.chocolate.filter;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.common.AbstractApplicationOptions;
import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleConfig;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleContent;
import com.ebay.kernel.context.RuntimeContext;
import org.apache.avro.generic.GenericData;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.io.InputStream;
import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Controls the parsing of Chocolate application options.
 * 
 * @author jepounds
 */
public class ApplicationOptions extends AbstractApplicationOptions {

    /** Private logging instance */
    private static final Logger logger = Logger.getLogger(ApplicationOptions.class);

    /**
     * Singleton instance
     */
    private static final ApplicationOptions instance = new ApplicationOptions();

    private static Properties kafkaConfig = null;

    private static final String CONFIG_SUBFOLDER = "config/";

    /** Zookeeper connection string */
    public static final String ZK_CONNECT_PROPERTY = "chocolate.filter.zkconnect";
    
    /** Number of consumer threads per Kafka topic. */
    public static final String KAFKA_IN_TOPIC = "chocolate.filter.kafka.consumer.topic";

    /** Number of consumer threads per Kafka topic. */
    public static final String KAFKA_OUT_TOPIC = "chocolate.filter.kafka.producer.topic";

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

    /**
     * Application options to load file from
     *
     * @pre file cannot be null and must exist as a valid readable file.
     * @param inputFile to load file using
     * @throws IOException if properties could not be loaded
     */
    public static void init(final File inputFile) throws IOException {
        instance.initInstance(inputFile);
    }

    /**
     * Application options to load from internal jar
     *
     * @param propertiesFile to load file from
     * @throws IOException if properties could not be loaded
     */
    public static void init(String propertiesFile, String kafkaPropertiesFile) throws IOException {
        String configPath = RuntimeContext.getConfigRoot().getFile() + CONFIG_SUBFOLDER + propertiesFile;
        String kafkaConfigPath = RuntimeContext.getConfigRoot().getFile() + CONFIG_SUBFOLDER + kafkaPropertiesFile;

        Properties p = new Properties();
        p.load(new FileReader(configPath));
        instance.initInstance(p);
        kafkaConfig = new Properties();
        kafkaConfig.load(new FileReader(kafkaConfigPath));
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

    /**
     * Gets Kafka properties.
     * 
     * @throws IOException if couldn't read these Kafka configuration
     */
    public Properties getKafkaConfig() {
        return kafkaConfig;
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

    /**
     * Gets Zookeeper properties.
     */
    public String getZookeeperString() {
        if (!properties.containsKey(ZK_CONNECT_PROPERTY)) {
            logger.fatal(ZK_CONNECT_PROPERTY + " not found in properties file!");
            throw new UnsupportedOperationException(ZK_CONNECT_PROPERTY + " not found in properties file!");
        }
        return properties.getProperty(ZK_CONNECT_PROPERTY);
    }

    /**
     * @return Kafka inbound topic
     */
    public String getKafkaInTopic() {
        return ApplicationOptionsParser.getStringProperty(properties, KAFKA_IN_TOPIC);
    }

    /**
     * @return Kafka outbound topic
     */
    public String getKafkaOutTopic() {
        return ApplicationOptionsParser.getStringProperty(properties, KAFKA_OUT_TOPIC);
    }

    /** @return the properties for testing purposes only */
    public Properties getProperties() {
        return (Properties) properties.clone();
    }

    /**
     * @return PublisherCache Zookeeper connection string.
     */
    public String getPublisherCacheZkConnectString() {
        if (!properties.containsKey(PUBLISHER_CACHE_ZK_CONNECT)) {
            logger.fatal(PUBLISHER_CACHE_ZK_CONNECT + " not found in properties file!");
            throw new UnsupportedOperationException(PUBLISHER_CACHE_ZK_CONNECT
                    + " not found in properties file!");
        }
        return properties.getProperty(PUBLISHER_CACHE_ZK_CONNECT);
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
