package com.ebay.traffic.chocolate.listener.util;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * @author kanliu
 */
@SuppressWarnings("javadoc")
public class ListenerOptionsTest {

    @Test
    public void testGetDummyKafkaHappy() {
        // Test defaults
        {
            Properties prop = new Properties();
            ListenerOptions.init(prop);
            ListenerOptions options = ListenerOptions.getInstance();
            assertFalse(options.useDummyKafka());
        }

        // Test false
        {
            Properties prop = new Properties();
            prop.put(ListenerOptions.KAFKA_USE_DUMMY, "0");
            ListenerOptions.init(prop);
            ListenerOptions options = ListenerOptions.getInstance();
            assertFalse(options.useDummyKafka());
        }

        // Test true
        {
            Properties prop = new Properties();
            prop.put(ListenerOptions.KAFKA_USE_DUMMY,
                    Boolean.TRUE.toString());
            ListenerOptions.init(prop);
            ListenerOptions options = ListenerOptions.getInstance();
            assertTrue(options.useDummyKafka());
        }
    }

    @Test
    public void testGetElasticsearchData() {
        Properties prop = new Properties();
        prop.put(ListenerOptions.ELASTICSEARCH_URL, "A");
        prop.put(ListenerOptions.ELASTICSEARCH_INDEX_PREFIX, "prefix");

        ListenerOptions.init(prop);
        ListenerOptions options = ListenerOptions.getInstance();

        assertEquals("A", options.getElasticsearchUrl());
        assertEquals("prefix", options.getElasticsearchIndexPrefix());
    }

    @Test
    public void shouldReturnCorrectValuesForHttpPortAndMaxThreads() {
        Properties prop = new Properties();
        prop.put(ListenerOptions.INPUT_HTTP_PORT, "8080");
        prop.put(ListenerOptions.INPUT_HTTPS_PORT, "8082");
        prop.put(ListenerOptions.OUTPUT_HTTP_PORT, "80");
        prop.put(ListenerOptions.OUTPUT_HTTPS_PORT, "443");
        prop.put(ListenerOptions.MAX_THREADS, "250");
        prop.put(ListenerOptions.PROXY, "rover.qa.ebay.com");

        ListenerOptions.init(prop);
        ListenerOptions options = ListenerOptions.getInstance();

        assertEquals(8080, options.getInputHttpPort());
        assertEquals(8082, options.getInputHttpsPort());
        assertEquals(80, options.getOutputHttpPort());
        assertEquals(443, options.getOutputHttpsPort());
        assertEquals(250, options.getMaxThreads());
        assertEquals("rover.qa.ebay.com", options.getProxy());
    }

    @Test
    public void shouldReturnAPidFile() throws IOException {
        Properties prop = new Properties();
        String path = "/tmp/chocolate-listener.pid";
        prop.put(ListenerOptions.PID_FILE, path);

        ListenerOptions.init(prop);
        ListenerOptions options = ListenerOptions.getInstance();
        File file = options.getPidFile();

        assertNotNull(file);
        // Handle Windows case
        assertTrue(file.getCanonicalPath().replace('\\','/').contains(path));
    }
}