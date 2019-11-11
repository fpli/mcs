package com.ebay.traffic.chocolate.init;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import com.ebay.traffic.monitoring.ESMetrics;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class ListenerInitializer {
    private static final Logger logger = Logger.getLogger(ListenerInitializer.class);
    private static String METRICS_INDEX_PREFIX;

    /**
     * The initialize method
     *
     * @param options the Listener options
     * @throws IOException if problems with the journal
     */
    public static synchronized void init(ListenerOptions options) {
        KafkaSink.initialize(options, options);
        METRICS_INDEX_PREFIX = options.getElasticsearchIndexPrefix();
        initElasticsearch(options.getElasticsearchUrl());
        initMessageObjectParser();
    }

    /**
     * Initialize ElasticSearch
     * @param url
     */
    static void initElasticsearch(String url) {
        ESMetrics.init(METRICS_INDEX_PREFIX, url);
        logger.info("ElasticSearch Metrics initialized");
    }

    /**
     * The terminate method to stop all services gracefully
     */
    static void terminate() {
        logger.info("close Kafka producer");
        Producer<Long, ListenerMessage> producer = KafkaSink.get();
        producer.close();

        logger.info("stop Frontier client");
        ESMetrics.getInstance().close();
    }


    static void initMessageObjectParser() {
        MessageObjectParser.init();
    }
}