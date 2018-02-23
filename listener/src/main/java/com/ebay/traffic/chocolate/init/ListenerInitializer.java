package com.ebay.traffic.chocolate.init;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.cratchit.server.Clerk;
import com.ebay.cratchit.server.Replayable;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.Producer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public class ListenerInitializer {
    private static final Logger logger = Logger.getLogger(ListenerInitializer.class);

    /**
     * The initialize method
     *
     * @param options the Listener options
     * @throws IOException if problems with the journal
     */
    public static void init(ListenerOptions options) {
        KafkaSink.initialize(options);
        initFrontier(options.getFrontierUrl(), options.getFrontierAppSvcName());
        // We will erase Journal feature soon, currently options.isJournalEnabled() is set as false.
        /*if (options.isJournalEnabled())
            initJournal(options, new KafkaProducerWrapper());*/
        initMessageObjectParser();
    }

    /**
     * Initializes the journal system
     * @param options to use in initializing journal. 
     * @throws IOException in case of OS/filesystem issues
     */
    static void initJournal(ListenerOptions options, Replayable replayable) throws IOException {
        Validate.notNull(options.getJournalPath());
        Clerk.initialize(new File(options.getJournalPath()), replayable, options.getJournalPageSize(), options.getJournalNumberOfPages(), 
                options.getJournalAlignmentSize(), options.getDriverId());
    }

    /**
     * Initialize Frontier
     */
    static void initFrontier(String url, String appSvcName) {
        MetricsClient.init(url, appSvcName);
        logger.info("Frontier Client initialized");
    }

    /**
     * The terminate method to stop all services gracefully
     */
    static void terminate() {
        logger.info("close Kafka producer");
        Producer<Long, ListenerMessage> producer = KafkaSink.get();
        producer.close();

        logger.info("stop Frontier client");
        MetricsClient.getInstance().terminate();
    }


    static void initMessageObjectParser() {
        MessageObjectParser.init();
    }
}