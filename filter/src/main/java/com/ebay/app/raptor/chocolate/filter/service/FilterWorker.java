package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.app.raptor.chocolate.filter.KafkaWrapper;
import com.ebay.app.raptor.chocolate.filter.util.CampaignPublisherMappingCache;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleType;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by spugach on 3/3/17.
 */
public class FilterWorker extends Thread {
    private static final int POLL_STEP_MS = 100;
    private static final long DEFAULT_PUBLISHER_ID = -1L;
    // Logging instance.
    private static final Logger logger = Logger.getLogger(FilterWorker.class);
    private final MetricsClient metrics;

    private FilterContainer filters;
    private KafkaWrapper kafka;

    // Shutdown signal.
    private final AtomicBoolean shutdownRequested;

    public FilterWorker(FilterContainer filters, KafkaWrapper kafka, MetricsClient metricsClient) {
        if (null == filters || null == kafka) {
            throw new IllegalArgumentException();
        }

        this.shutdownRequested = new AtomicBoolean(false);
        this.filters = filters;
        this.kafka = kafka;
        this.metrics = metricsClient;
    }

    /**
     * Signals termination.
     */
    public void shutdown() {
        logger.info("Calling shutdown in FilterWorker.");
        shutdownRequested.set(true);
    }

    @Override
    public void run() {
        // Init the metrics that we don't use often
        this.metrics.meter("FilterError", 0);
        this.metrics.meter("messageParseFailure", 0);
        this.metrics.mean("FilterPassedPPM", 0L);

        try {
            logger.info(String.format("Connecting Filter to Kafka - In: %s, Out: %s", ApplicationOptions.getInstance().getKafkaInTopic(), ApplicationOptions.getInstance().getKafkaOutTopic()));
            this.kafka.init(ApplicationOptions.getInstance().getKafkaInTopic(), ApplicationOptions.getInstance().getKafkaOutTopic());

            while (!this.shutdownRequested.get()) {
                List<ListenerMessage> messages = this.kafka.poll();

                if (null == messages || messages.size() == 0) {
                    this.metrics.mean("FilterIdle", 1);
                    Thread.sleep(POLL_STEP_MS);
                } else {
                    long cycleStart = System.currentTimeMillis();
                    long bigCycleStart = System.currentTimeMillis();
                    this.metrics.meter("FilterThroughput", messages.size());
                    int messagesPassed = 0;
                    for (ListenerMessage message : messages) {
                        this.metrics.mean("FilterLatency", System.currentTimeMillis() - message.getTimestamp());

                        FilterMessage outMessage = processMessage(message);

                        if (outMessage.getValid()) messagesPassed += 1;

                        this.kafka.send(outMessage);

                        long timeSpent = System.currentTimeMillis() - cycleStart;
                        if (timeSpent >= POLL_STEP_MS) {
                            this.metrics.mean("FilterIdle", 0);
                            cycleStart = System.currentTimeMillis();
                        }

                        Thread.yield();
                    }

                    this.kafka.flush();
                    this.metrics.mean("FilterPassedPPM", 1000000L * messagesPassed / messages.size());  // PPM (per million) of valid messages

                    long timeSpent = System.currentTimeMillis() - bigCycleStart;
                    this.metrics.mean("FilterProcessingTime", timeSpent);

                    // In low load, sleep more
                    if (10 * timeSpent < POLL_STEP_MS) {
                        this.metrics.mean("FilterIdle", 1);
                        Thread.sleep(POLL_STEP_MS);
                    } else {
                        this.metrics.mean("FilterIdle", 0);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Exception in worker thread: ", e);
            this.metrics.meter("FilterError");
            //TODO react
        } finally {
            this.kafka.close();
        }

        logger.warn("Shutting down");
    }

    public FilterMessage processMessage(ListenerMessage message) {
        FilterMessage outMessage = new FilterMessage();
        outMessage.setSnapshotId(message.getSnapshotId());
        outMessage.setTimestamp(message.getTimestamp());
        if (message.getPublisherId() == DEFAULT_PUBLISHER_ID) {
          long publisherId = getPublisherId(message.getCampaignId());
          outMessage.setPublisherId(publisherId);
          message.setPublisherId(publisherId);
        }
        else
            outMessage.setPublisherId(message.getPublisherId());
        outMessage.setCampaignId(message.getCampaignId());
        outMessage.setRequestHeaders(message.getRequestHeaders());
        outMessage.setUri(message.getUri());
        outMessage.setResponseHeaders(message.getResponseHeaders());
        outMessage.setChannelAction(message.getChannelAction());
        outMessage.setChannelType(message.getChannelType());
        outMessage.setHttpMethod(message.getHttpMethod());
        outMessage.setSnid(message.getSnid());
        outMessage.setIsTracked(message.getIsTracked());
        try {
            FilterResult filtered = this.filters.test(message);
            outMessage.setValid(filtered.isEventValid());
            outMessage.setFilterFailed(filtered.getFailedRule().toString());
        } catch (Exception e) {
            outMessage.setValid(false);
            outMessage.setFilterFailed(FilterRuleType.ERROR.getRuleType());
        }
        return outMessage;
    }

    /**
     * Lookup publisherId based on campaignId from publisher cache
     *
     * @param campaignId
     *            Long
     * @return publisher, in case no match result, use default -1L
     */
    public Long getPublisherId(Long campaignId) {
        Long publisher = CampaignPublisherMappingCache.getInstance().lookup(campaignId);
        if (publisher == null) {
            logger.debug(String.format("No match result for campaign %d, use default -1L as publisherId", campaignId));
            return DEFAULT_PUBLISHER_ID;
        }
        return publisher;
    }
}
