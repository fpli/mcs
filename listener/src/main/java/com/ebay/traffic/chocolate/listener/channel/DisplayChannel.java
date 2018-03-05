package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.*;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;

/**
 * @author Lu huichun
 * DAP channel specific handler
 * it will forward the request to Rover and send back the response from Rover
 * and it will track all actions into Kafka
 */

public class DisplayChannel implements Channel {
    private static final Logger logger = Logger.getLogger(DisplayChannel.class);

    private final MessageObjectParser parser;
    private final MetricsClient metricsClient;
    private final String kafkaTopic;
    private final Producer<Long, ListenerMessage> producer;
    private final ChannelActionEnum channelActionEnum;
    private final LogicalChannelEnum logicalChannelEnum;

    @Override
    public long getPartitionKey(final HttpServletRequest servletRequest) {
        // There is no partitionKey here
        return 0;
    }

    @Override
    public void process(HttpServletRequest request, HttpServletResponse response) {
        try {
            if (parser.responseShouldBeFiltered(request, response))
                return;
        } catch (MalformedURLException | UnsupportedEncodingException e) {
            logger.error("Wrong with URL format/encoding", e);
        }

        long startTime = startTimerAndLogData(request);

        ListenerMessage message = parser.parseHeader(request, response, startTime,
                -1L, logicalChannelEnum, channelActionEnum, null);

        if (message != null) {
            producer.send(new ProducerRecord<>(kafkaTopic,
                    message.getSnapshotId(), message), KafkaSink.callback);
        }

        stopTimerAndLogData(startTime, message.toString());
    }

    DisplayChannel(ChannelActionEnum channelActionEnum, LogicalChannelEnum logicalChannelEnum) {
        Validate.isTrue(channelActionEnum != null, "Channel action must not be null");
        Validate.isTrue(LogicalChannelEnum.DISPLAY == logicalChannelEnum, "Logical channel should be Display");
        this.parser = MessageObjectParser.getInstance();
        this.metricsClient = MetricsClient.getInstance();
        this.kafkaTopic = ListenerOptions.getInstance().getSinkKafkaConfigs().get(ChannelType.DISPLAY);
        this.producer = KafkaSink.get();
        this.channelActionEnum = channelActionEnum;
        this.logicalChannelEnum = logicalChannelEnum;
    }

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
        metricsClient.meter("IncomingCount");
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
            metricsClient.meter("FormatErrorCount");
            return null;
        }
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
        metricsClient.meter("SuccessCount");
        metricsClient.mean("AverageLatency", endTime - startTime);
    }

}
