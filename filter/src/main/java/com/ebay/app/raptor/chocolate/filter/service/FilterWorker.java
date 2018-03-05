package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleType;
import com.ebay.app.raptor.chocolate.filter.util.CampaignPublisherMappingCache;
import com.ebay.traffic.chocolate.kafka.KafkaConsumerFactory;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by yliu29 on 2/23/18.
 */
public class FilterWorker extends Thread {
  private static final Logger LOG = Logger.getLogger(FilterWorker.class);

  private static final long POLL_STEP_MS = 100;
  private static final long DEFAULT_PUBLISHER_ID = -1L;

  private final MetricsClient metrics;
  private final FilterContainer filters;
  private final ChannelType channelType;
  private final String inputTopic;
  private final String outputTopic;

  private final Consumer<Long, ListenerMessage> consumer; // in
  private final Producer<Long, FilterMessage> producer; // out

  // Shutdown signal.
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

  public FilterWorker(ChannelType channelType, String inputTopic,
                      Properties properties, String outputTopic,
                      FilterContainer filters) {
    this.metrics = MetricsClient.getInstance();
    this.filters = filters;
    this.channelType = channelType;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;

    this.consumer = KafkaConsumerFactory.create(properties);
    this.producer = KafkaSink.get();
  }

  /**
   * Signals termination.
   */
  public void shutdown() {
    LOG.info("Calling shutdown in FilterWorker.");
    shutdownRequested.set(true);
  }

  @Override
  public void run() {
    LOG.info("Start filter worker, channel " + channelType +
            ", input topic " + inputTopic + ", output topic " + outputTopic);

    // Init the metrics that we don't use often
    this.metrics.meter("FilterError", 0);
    this.metrics.meter("messageParseFailure", 0);
    this.metrics.mean("FilterPassedPPM", 0L);

    try {
      consumer.subscribe(Arrays.asList(inputTopic));

      while (!shutdownRequested.get()) {
        ConsumerRecords<Long, ListenerMessage> records = consumer.poll(POLL_STEP_MS);
        Iterator<ConsumerRecord<Long, ListenerMessage>> iterator = records.iterator();
        int count = 0;
        int passed = 0;
        long startTime = System.currentTimeMillis();
        while (iterator.hasNext()) {
          ++count;
          ConsumerRecord<Long, ListenerMessage> record = iterator.next();
          ListenerMessage message = record.value();
          metrics.mean("FilterLatency", System.currentTimeMillis() - message.getTimestamp());

          FilterMessage outMessage = processMessage(message);

          if (outMessage.getValid()) {
            ++passed;
          }

          producer.send(new ProducerRecord<>(outputTopic, outMessage.getSnapshotId(), outMessage), KafkaSink.callback);
        }

        if (count > 2000) {
          // producer flush
          producer.flush();

          // update consumer offset
          consumer.commitSync();
        }

        if (count == 0) {
          metrics.mean("FilterIdle", 1);
          Thread.sleep(POLL_STEP_MS);
        } else {
          metrics.meter("FilterThroughput", count);
          metrics.mean("FilterPassedPPM", 1000000L * passed / count);
          long timeSpent = System.currentTimeMillis() - startTime;
          metrics.mean("FilterProcessingTime", timeSpent);

          if (timeSpent >= POLL_STEP_MS) {
            this.metrics.mean("FilterIdle", 0);
          } else {
            this.metrics.mean("FilterIdle", 1);
            Thread.sleep(POLL_STEP_MS);
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception in worker thread: ", e);
      this.metrics.meter("FilterError");
    } finally {
      consumer.close();
    }

    LOG.warn("Shutting down");
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
   * @return publisher, in case no match result, use default -1L
   */
  public Long getPublisherId(Long campaignId) {
    Long publisher = CampaignPublisherMappingCache.getInstance().lookup(campaignId);
    if (publisher == null) {
      LOG.debug(String.format("No match result for campaign %d, use default -1L as publisherId", campaignId));
      return DEFAULT_PUBLISHER_ID;
    }
    return publisher;
  }
}
