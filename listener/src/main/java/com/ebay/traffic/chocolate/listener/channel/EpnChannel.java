package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.cratchit.server.Clerk;
import com.ebay.cratchit.server.Journal;
import com.ebay.cratchit.server.Page;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.*;
import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;

/**
 * ePN channel specific handler
 * <p>
 * it will forward the request to Rover and send back the response from Rover
 * and it will track all actions into Kafka
 */
public class EpnChannel implements Channel {
    private static final Logger logger = Logger.getLogger(EpnChannel.class);
    // the query key to search for in order to choose the right kafka partition
    private static final String CAMPAIGN_PATTERN = "campid";

    private static final String SNID_PATTERN = "snid";

    /**
     * Thread local variable containing the page the thread is writing to for journal
     * Will not be initialized if the feature switch is off
     */
   /* private static ThreadLocal<Page> journalPage = ThreadLocal.withInitial(() -> {
        if (ListenerOptions.getInstance().isJournalEnabled()) {
            try {
                return Clerk.getInstance().getJournalPage();
            } catch (IOException e) {
                logger.error(e.getMessage());
                return null; // inactive
            }
        }
        return null;
    });*/
    private final MessageObjectParser parser;
    private final MetricsClient metrics;
    private final String kafkaTopic;
    private final Producer<Long, ListenerMessage> producer;
    private final ChannelActionEnum channelAction;
    private final LogicalChannelEnum logicalChannel;

    EpnChannel(ChannelActionEnum action, LogicalChannelEnum logicalChannel) {
        Validate.isTrue(action != null, "Channel action must not be null");
        Validate.isTrue(LogicalChannelEnum.EPN == logicalChannel, "Logical channel should be ePN");
        this.parser = MessageObjectParser.getInstance();
        this.metrics = MetricsClient.getInstance();
        this.kafkaTopic = ListenerOptions.getInstance().getSinkKafkaConfigs().get(ChannelType.EPN);
        this.producer = KafkaSink.get();
        this.channelAction = action;
        this.logicalChannel = logicalChannel;
    }


    @Override
    public void process(HttpServletRequest request, HttpServletResponse response) {
        // Filter record based on request and response
        try {
            if (parser.responseShouldBeFiltered(request, response)) {
                return;
            }
        } catch (MalformedURLException | UnsupportedEncodingException e) {
            logger.error("Wrong with URL format/encoding", e);
        }

        // Proxy the request
        long campaignId = getPartitionKey(request);
        // Skip if campaignId is invalid;
        if (campaignId <= Channel.PARTITION_KEY_ERROR) return;

        // Start timing
        long startTime = startTimerAndLogData(request);


        // Get SNID from request URL.
        //TODO figure out a different way to acquire the snid
        String snid = getSnid(request);

        // Parse the response
        ListenerMessage message = parser.parseHeader(request, response,
                startTime, campaignId, logicalChannel, channelAction, snid);

        if (message != null) {
     /*       Page page = getJournalPage();
            if (page != null) {
                // This is just a complicated looking way of prepending campaignID into the start of the entry
                byte[] entry = ByteBuffer.allocate(Long.BYTES + json.getBytes().length).putLong(campaignId).put(json.getBytes()).array();
                writeToJournal(page, message.getSnapshotId(), entry);
            }*/
            producer.send(new ProducerRecord<>(kafkaTopic,
                    message.getSnapshotId(), message), KafkaSink.callback);
        }

        // Stop timing
        stopTimerAndLogData(startTime, message.toString());
    }

    /**
     * Validates that the journal is active and then writes to it
     *
     * @param page       the journal page for this thread
     * @param snapshotId snapshot ID of the record being written
     * @param entry      the json data to be written, in byte[] form
     */
    /*void writeToJournal(Page page, long snapshotId, byte[] entry) {
        short size = page.write(snapshotId, entry);
        if (size == Journal.ERROR) {
            getNextJournalPage(true); // the only way a page can be active is if journal is enabled
            writeToJournal(getJournalPage(), snapshotId, entry);
        }
    }*/

    /**
     * Starts the timer and logs some basic info
     *
     * @param request Incoming Http request
     * @return the start time in milliseconds
     */
    private long startTimerAndLogData(HttpServletRequest request) {
        // the main rover process is already finished at this moment
        // use the timestamp from request as the start time
        long startTime = System.currentTimeMillis();

        try {
            startTime = ((Request) request).getTimeStamp();
        } catch (ClassCastException e) {
            // ideally only touch this part in unit test
            logger.warn("Cannot get request start time, use system time instead. ", e);
        }
        logger.debug(String.format("StartTime: %d", startTime));
        metrics.meter("IncomingCount");
        return startTime;
    }

    /**
     * Attempts to convert the ListenerMessage into JSON for transmission and journaling
     *
     * @param message the ListenerMessage
     * @return the JSON, or null on error
     */
    private String convertToJson(ListenerMessage message) {
        try {
            return message.writeToJSON();
        } catch (IOException e) {
            logger.error("Failed to convert ListenerMessage record to JSON", e);
            metrics.meter("FormatErrorCount");
        }
        return null;
    }

    /**
     * Stops the timer and logs relevant debugging messages
     *
     * @param startTime    the start time, so that latency can be calculated
     * @param kafkaMessage logged to CAL for debug purposes
     */
    private void stopTimerAndLogData(long startTime, String kafkaMessage) {
        long endTime = System.currentTimeMillis();
        logger.debug(String.format("EndTime: %d", endTime));
        metrics.meter("SuccessCount");
        metrics.mean("AverageLatency", endTime - startTime);
    }

    /**
     * Initialize the journal page if it's not already
     */
    /*static void getNextJournalPage(boolean isEnabled) {
        if (isEnabled) {
            try {
                journalPage.set(Clerk.getInstance().getJournalPage());
                return;
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
        journalPage.set(null); // inactive
    }*/

    /**
     * Simple accessor
     *
     * @return the journal page for writing
     */
    /*Page getJournalPage() {
        return journalPage.get();
    }*/

    /**
     * get Partition Key based on query pattern match
     * Note: parameter is case insensitive
     *
     * @param servletRequest incoming HttpServletRequest
     * @return campaignId, default -1L if no pattern match in the query of HttpServletRequest
     */
    @Override
    public long getPartitionKey(final HttpServletRequest servletRequest) {
        long campaignId = Channel.PARTITION_KEY_ERROR;
        String campaign = null;
        try {
            String[] queries = servletRequest.getQueryString().toLowerCase().split("&");
            for (String query : queries) {
                if (query.startsWith(CAMPAIGN_PATTERN + "=")) {
                    campaign = query.split("=", 2)[1];
                    break;
                }
            }
        } catch (Exception e) {
            // Do nothing here, just consider the campaign as invalid
        }
        if (campaign != null && !campaign.isEmpty()) {
            try {
                campaignId = Long.parseLong(campaign);
            } catch (NumberFormatException e) {
                logger.warn("Invalid campaign: " + campaign);
            }
        }
        logger.debug(String.format("CampaignId: %d", campaignId));
        return campaignId;
    }

    /**
     * get SNID from
     * @param servletRequest
     * @return
     */
    public String getSnid(final HttpServletRequest servletRequest) {
        String snid = "";
        try {
            String[] queries = servletRequest.getQueryString().toLowerCase().split("&");
            for (String query : queries) {
                if (query.startsWith(SNID_PATTERN + "=")) {
                    snid = query.split("=", 2)[1];
                    break;
                }
            }
        } catch (Exception e) {
            // Do nothing here, just consider the snid as invalid
        }

        return snid;
    }
}
