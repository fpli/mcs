package com.ebay.traffic.chocolate.init;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.cratchit.server.Clerk;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ListenerInitializerTest {

    @BeforeClass
    public static void setUp() {
        Properties properties = new Properties();
        properties.setProperty(ListenerOptions.FRONTIER_APP_SVC_NAME, "ListenerUTs");
        properties.setProperty(ListenerOptions.FRONTIER_URL, "frontier://tenant=mp;env=qa;app_svc=%s@sherlock-ftr-qa.stratus.phx.qa.ebay.com");
        properties.setProperty(ListenerOptions.JOURNAL_ALIGNMENT_SIZE, Integer.valueOf(128).toString());
        properties.setProperty(ListenerOptions.JOURNAL_PAGE_SIZE, Integer.valueOf(8192).toString());
        properties.setProperty(ListenerOptions.JOURNAL_NUMBER_OF_PAGES, Integer.valueOf(4096).toString());
        properties.setProperty(ListenerOptions.JOURNAL_PATH, System.getProperty("java.io.tmpdir"));
        ListenerOptions.init(properties);
    }

    @AfterClass
    public static void tearDown() {
        MetricsClient.getInstance().terminate(); // TODO this throws NPE when running individual unit tests
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
