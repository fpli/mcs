package com.ebay.traffic.chocolate.init;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.cratchit.server.Clerk;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ListenerOptions.class})
@PowerMockIgnore( {"javax.management.*"})
public class ListenerInitializerTest {

    @BeforeClass
    public static void setUp() {
        Properties properties = new Properties();
        properties.setProperty(ListenerOptions.KAFKA_EPN_TOPIC_PROPERTY, "test");
        properties.setProperty(ListenerOptions.FRONTIER_APP_SVC_NAME, "ListenerUTs");
        properties.setProperty(ListenerOptions.FRONTIER_URL, "frontier://tenant=mp;env=qa;app_svc=%s@sherlock-ftr-qa.stratus.phx.qa.ebay.com");
        properties.setProperty(ListenerOptions.JOURNAL_ALIGNMENT_SIZE, Integer.valueOf(128).toString());
        properties.setProperty(ListenerOptions.JOURNAL_PAGE_SIZE, Integer.valueOf(8192).toString());
        properties.setProperty(ListenerOptions.JOURNAL_NUMBER_OF_PAGES, Integer.valueOf(4096).toString());
        properties.setProperty(ListenerOptions.JOURNAL_PATH, System.getProperty("java.io.tmpdir"));
        properties.setProperty(ListenerOptions.KAFKA_EPN_TOPIC_PROPERTY, "chocolate.listener.kafka.topic.epn");
        properties.setProperty(ListenerOptions.KAFKA_DISPLAY_TOPIC_PROPERTY, "chocolate.listener.kafka.topic.display");
        ListenerOptions.init(properties);
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "127.0.0.1:9092");
        kafkaProps.setProperty("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        kafkaProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        ListenerOptions.kafkaPros = kafkaProps;
        KafkaProducers.getInstance();
    }

    @AfterClass
    public static void tearDown() {
        ListenerInitializer.terminate(); // TODO this throws NPE when running individual unit tests
    }

    @Test
    public void testFrontierInit() {
        String url = "frontier://tenant=mp;env=qa;app_svc=%s@sherlock-ftr-qa.stratus.phx.qa.ebay.com";
        ListenerInitializer.initFrontier(url, "ListenerInitializerTestSvc");
        assertTrue(MetricsClient.getInstance() != null);
    }

    @Test
    public void testJournalShelfShouldBeInitializedOnStartup() throws IOException {
        ListenerInitializer.initJournal(ListenerOptions.getInstance(), null);
        assertNotNull(Clerk.getInstance());
        Clerk.getInstance().delete();
    }

    @Test
    public void testMessageObjectParserShouldBeInitialized() {
        ListenerInitializer.initMessageObjectParser();
        assertNotNull(MessageObjectParser.getInstance());
    }
}
