package com.ebay.traffic.chocolate.init;

import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import com.ebay.traffic.monitoring.ESMetrics;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ListenerInitializerTest {

    @BeforeClass
    public static void setUp() {
        Properties properties = new Properties();
        properties.setProperty(ListenerOptions.FRONTIER_APP_SVC_NAME, "ListenerUTs");
        properties.setProperty(ListenerOptions.FRONTIER_URL, "frontier://tenant=mp;env=qa;app_svc=%s@sherlock-ftr-qa.stratus.phx.qa.ebay.com");
        ListenerOptions.init(properties);
    }

    @AfterClass
    public static void tearDown() {
        ESMetrics.getInstance().close();
    }

    @Test
    public void testInitElasticSearch() {
        String url = "frontier://tenant=mp;env=qa;app_svc=%s@sherlock-ftr-qa.stratus.phx.qa.ebay.com";
        String esUrl = "http://10.148.185.16:9200";
        ListenerInitializer.initElasticsearch(esUrl);
        assertTrue(ESMetrics.getInstance() != null);
    }

    @Test
    public void testMessageObjectParserShouldBeInitialized() {
        ListenerInitializer.initMessageObjectParser();
        assertNotNull(MessageObjectParser.getInstance());
    }
}
