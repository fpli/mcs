package com.ebay.traffic.chocolate.listener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.common.AbstractApplicationOptions;
import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.kernel.context.RuntimeContext;

import com.ebay.traffic.chocolate.kafka.KafkaCluster;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.ebay.traffic.chocolate.kafka.KafkaCluster.DELIMITER;

/**
 * This class is used for Listener specific options. Some use cases are:
 * - putting String constants in here for key names
 * - additional validation of option values
 * - grouping of constants for particular use cases (e.g. kafka)
 */
public class ListenerOptions extends AbstractApplicationOptions implements KafkaSink.KafkaConfigurable {
    /** Private logging instance */
    private static final Logger logger = Logger
            .getLogger(ListenerOptions.class);

    /**
     * Singleton instance
     */
    private static final ListenerOptions instance = new ListenerOptions();

    /** Kafka cluster, can be "kafka", "rheos", "rheos,kafka", "kafka,rheos". */
    public static final String KAFKA_CLUSTER = "chocolate.listener.kafka.cluster";

    /** Listener topic to publish to */
    /** prefix of Kafka topic for channels. */
    // refer to com.ebay.app.raptor.chocolate.avro.ChannelType for channels.
    // for ePN:  chocolate.listener.kafka.topic.EPN
    // for display: chocolate.listener.kafka.topic.DISPLAY
    public static final String KAFKA_TOPIC_PREFIX = "chocolate.listener.kafka.topic.";

    /** Whether or not we'll be using a dummy (test context only) */
    static final String KAFKA_USE_DUMMY = "chocolate.listener.kafka.usedummy";

    public static final String FRONTIER_URL = "frontier.url";
    public static final String FRONTIER_APP_SVC_NAME = "frontier.app.svc.name";

    public static final String JOURNAL_ENABLED = "chocolate.listener.journal.enabled";
    public static final String JOURNAL_PAGE_SIZE = "chocolate.listener.journal.page.size";
    public static final String JOURNAL_NUMBER_OF_PAGES = "chocolate.listener.journal.pages";
    public static final String JOURNAL_ALIGNMENT_SIZE = "chocolate.listener.journal.alignment.size";
    public static final String JOURNAL_PATH = "chocolate.listener.journal.path";

    public static final String INPUT_HTTP_PORT = "http.port";
    public static final String INPUT_HTTPS_PORT = "https.port";
    public static final String OUTPUT_HTTP_PORT = "lb.http.port";
    public static final String OUTPUT_HTTPS_PORT = "lb.https.port";
    public static final String PROXY = "proxyTo";

    public static final String MAX_THREADS = "max.threads";
    public static final String PID_FILE = "chocolate.listener.pidfile";

    private static final String KAFKA_PROPERTIES_FILE = "listener-kafka.properties";
    private static final String RHEOS_KAFKA_PROPERTIES_FILE = "listener-kafka-rheos.properties";
    private static Properties kafkaProperties;
    private static Properties rheosKafkaProperties;

    private String sinkKafkaCluster;
    private Map<ChannelType, String> sinkKafkaConfigMap = new HashMap<>();

    /** Static driver ID */
    static final int DRIVER_ID = ApplicationOptionsParser.getDriverIdFromIp();

    /**
     * Application options to load file from
     *
     * @pre file cannot be null and must exist as a valid readable file.
     * @param inputFile
     *            to load file using
     * @throws IOException
     *             if properties could not be loaded
     */
    public static void init(final File inputFile) throws IOException {
        instance.initInstance(inputFile);
    }

    /**
     * @return Return kafka cluster, can be "rheos", "kafka", "rheos,kafka", "kafka,rheos"
     */
    @Override
    public String getSinkKafkaCluster() {
        return sinkKafkaCluster;
    }

    /**
     * Return sink kafka properties.
     *
     * @param sinkCluster kafka cluster
     * @return kafka properties
     * @throws IOException
     */
    @Override
    public Properties getSinkKafkaProperties(KafkaCluster sinkCluster) throws IOException{
        if (sinkCluster == KafkaCluster.KAFKA) {
            return kafkaProperties;
        } else {
            return rheosKafkaProperties;
        }
    }

    private static Properties loadProperties(String file) throws IOException {
        String filePath = RuntimeContext.getConfigRoot().getFile() + file;
        Properties properties = new Properties();
        FileReader reader = new FileReader(filePath);
        properties.load(reader);
        reader.close();
        return properties;
    }

    /**
     * Initialize with an InputStream
     *
     * @param stream to load properties from
     * @throws IOException if properties could not be loaded
     */
    public static void init(InputStream stream) throws IOException {
        instance.initInstance(stream);
        if (kafkaProperties == null) {
            kafkaProperties = loadProperties(KAFKA_PROPERTIES_FILE);
        }
        rheosKafkaProperties = loadProperties(RHEOS_KAFKA_PROPERTIES_FILE);
        instance.initKafkaConfigs();
    }

    /**
     * Only for test
     */
    public void setKafkaProperties(Properties properties) {
        kafkaProperties = properties;
    }

    /**
     * Initialize with a properties object
     *
     * @param properties to initialize using
     */
    public static void init(final Properties properties) {
        instance.initInstance(properties);
    }

    /**
     * Can't create ListenerOptions from outside
     */
    private ListenerOptions() {
    }

    /**
     * Return the singleton
     * 
     * @return singleton instance
     */
    public static ListenerOptions getInstance() {
        return instance;
    }

    /**
     * @return the driver ID for the listener.
     */
    public int getDriverId() {
        return DRIVER_ID;
    }

    /**
     * Kafka topic configs
     */
    private void initKafkaConfigs() {
        sinkKafkaCluster = ApplicationOptionsParser.getStringProperty(properties, KAFKA_CLUSTER);
        String[] sinkKafkaClusters = sinkKafkaCluster.split(DELIMITER);
        if (sinkKafkaClusters.length > 2) {
            throw new IllegalArgumentException("too many values in " + KAFKA_CLUSTER);
        }
        Map<String, String> sinkChannelKafkaTopics = getByNamePrefix(KAFKA_TOPIC_PREFIX);
        for (Map.Entry<String, String> channelTopic : sinkChannelKafkaTopics.entrySet()) {
            ChannelType channelType = ChannelType.valueOf(channelTopic.getKey());
            String topics = channelTopic.getValue();
            String[] topicarray = topics.split(DELIMITER);
            if (topicarray.length > 2) {
                throw new IllegalArgumentException("too many values in " + KAFKA_TOPIC_PREFIX + channelTopic.getKey());
            }
            sinkKafkaConfigMap.put(channelType, topics);
        }
    }

    /**
     * @return Return kafka channel topic map
     */
    public Map<ChannelType, String> getSinkKafkaConfigs() {
        return sinkKafkaConfigMap;
    }

    /** @return true iff using a dummy (non-existent) kafka. false otherwise. */
    boolean useDummyKafka() {
        return ApplicationOptionsParser.getBooleanProperty(properties,
                KAFKA_USE_DUMMY, false);
    }


    public String getFrontierUrl() {
        return ApplicationOptionsParser.getStringProperty(properties,
                FRONTIER_URL);
    }

    public String getFrontierAppSvcName() {
        return ApplicationOptionsParser.getStringProperty(properties,
                FRONTIER_APP_SVC_NAME);
    }

    public boolean isJournalEnabled() {
        return ApplicationOptionsParser.getBooleanProperty(properties, JOURNAL_ENABLED, false);
    }

    public int getJournalNumberOfPages() {
        return getPowerOfTwo(JOURNAL_NUMBER_OF_PAGES,  4096, 8192);
    }

    public int getJournalPageSize() {
        return getPowerOfTwo(JOURNAL_PAGE_SIZE, 8192, 32768);
    }

    public short getJournalAlignmentSize() {
        return (short) getPowerOfTwo(JOURNAL_ALIGNMENT_SIZE, 128, 256);
    }

    public String getJournalPath() {
        return ApplicationOptionsParser.getStringProperty(properties, JOURNAL_PATH);
    }

    private int getPowerOfTwo(String property, int min, int max) {
        int number = ApplicationOptionsParser.getNumericProperty(properties, property, min, max);
        if (!powerOfTwo(number))
            throw new IllegalArgumentException("Pages must be a power of 2");
        return number;
    }

    private boolean powerOfTwo(int number) {
        return (number & (number - 1)) == 0;
    }

    public int getInputHttpPort() { return ApplicationOptionsParser.getNumericProperty(properties, INPUT_HTTP_PORT, 8000, 9000);
    }

    public int getInputHttpsPort() { return ApplicationOptionsParser.getNumericProperty(properties, INPUT_HTTPS_PORT, 8000, 9000);
    }

    public int getOutputHttpPort() { return ApplicationOptionsParser.getNumericProperty(properties, OUTPUT_HTTP_PORT, 80, 9000);
    }

    public int getOutputHttpsPort() { return ApplicationOptionsParser.getNumericProperty(properties, OUTPUT_HTTPS_PORT, 443, 9000);
    }

    public String getProxy() {
        return ApplicationOptionsParser.getStringProperty(properties, PROXY);
    }

    public int getMaxThreads() { return ApplicationOptionsParser.getNumericProperty(properties, MAX_THREADS, 50, 500);
    }

    public File getPidFile() throws IOException { return ApplicationOptionsParser.getFile(properties, PID_FILE, false, null, false); }
}