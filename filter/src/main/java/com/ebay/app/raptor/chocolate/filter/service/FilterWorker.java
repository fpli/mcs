package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleType;
import com.ebay.app.raptor.chocolate.filter.util.CampaignPublisherMappingCache;
import com.ebay.traffic.chocolate.kafka.KafkaConsumerFactory;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by yliu29 on 2/23/18.
 */
public class FilterWorker extends Thread {
  private static final Logger LOG = Logger.getLogger(FilterWorker.class);

  private static final long POLL_STEP_MS = 100;
  private static final long DEFAULT_PUBLISHER_ID = -1L;

  private final MetricsClient metrics;
  private final ESMetrics esMetrics;
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
    this.esMetrics = ESMetrics.getInstance();
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
    this.esMetrics.meter("FilterError", 0);
    this.esMetrics.meter("messageParseFailure", 0);
    this.esMetrics.mean("FilterPassedPPM", 0L);

    try {
      consumer.subscribe(Arrays.asList(inputTopic));

      long flushThreshold = 0;

      final long kafkaLagMetricInterval = 30000; // 30s
      Map<Integer, Long> offsets = new HashMap<>();
      long kafkaLagMetricStart = System.currentTimeMillis();
      while (!shutdownRequested.get()) {
        ConsumerRecords<Long, ListenerMessage> records = consumer.poll(POLL_STEP_MS);
        Iterator<ConsumerRecord<Long, ListenerMessage>> iterator = records.iterator();
        int count = 0;
        int passed = 0;
        long startTime = System.currentTimeMillis();
        while (iterator.hasNext()) {
          ConsumerRecord<Long, ListenerMessage> record = iterator.next();
          ListenerMessage message = record.value();
          metrics.meter("FilterInputCount");
          esMetrics.meter("FilterInputCount", message.getChannelAction().toString(), message.getChannelType().toString());
          long latency = System.currentTimeMillis() - message.getTimestamp();
          metrics.mean("FilterLatency", latency);
          esMetrics.mean("FilterLatency", latency);

          ++count;
          esMetrics.meter("FilterThroughput", message.getChannelAction().toString(), message.getChannelType().toString());

          FilterMessage outMessage = processMessage(message);

          if (outMessage.getRtRuleFlags() == 0) {
            ++passed;
            esMetrics.meter("FilterPassedCount", outMessage.getChannelAction().toString(), outMessage.getChannelType().toString());
          }

          // cache current offset for partition*
          offsets.put(record.partition(), record.offset());

          producer.send(new ProducerRecord<>(outputTopic, outMessage.getSnapshotId(), outMessage), KafkaSink.callback);
        }

        // flush logic
        flushThreshold += count;

        if (flushThreshold > 1000) {
          // producer flush
          producer.flush();

          // update consumer offset
          consumer.commitSync();

          // reset threshold
          flushThreshold = 0;
        }

        // kafka lag metrics logic
        long now = System.currentTimeMillis();
        if (now - kafkaLagMetricStart > kafkaLagMetricInterval) {
          Map<TopicPartition, Long> endOffsets = consumer.endOffsets(records.partitions());
          Iterator<Map.Entry<TopicPartition, Long>> iter = endOffsets.entrySet().iterator();
          while (iter.hasNext()) {
            Map.Entry<TopicPartition, Long> entry = iter.next();
            TopicPartition tp = entry.getKey();
            long endOffset = entry.getValue();
            if (offsets.containsKey(tp.partition())) {
              long offset = offsets.get(tp.partition());
              Map<String, Object> additionalFields = new HashMap<>();
              additionalFields.put("consumer", tp.partition());
              esMetrics.mean("FilterKafkaConsumerLag", endOffset - offset, additionalFields);
            }
          }

          kafkaLagMetricStart = now;
        }

        if (count == 0) {
          metrics.mean("FilterIdle", 1);
          esMetrics.mean("FilterIdle");
          Thread.sleep(POLL_STEP_MS);
        } else {
          metrics.meter("FilterThroughput", count);
          metrics.meter("FilterPassedCount", passed);
          metrics.mean("FilterPassedPPM", 1000000L * passed / count);
          esMetrics.meter("FilterThroughput", count);
          esMetrics.meter("FilterPassedCount", passed);
          esMetrics.mean("FilterPassedPPM", 1000000L * passed / count);
          long timeSpent = System.currentTimeMillis() - startTime;
          metrics.mean("FilterProcessingTime", timeSpent);
          esMetrics.mean("FilterProcessingTime", timeSpent);

          if (timeSpent >= POLL_STEP_MS) {
            this.metrics.mean("FilterIdle", 0);
            this.esMetrics.mean("FilterIdle", 0);
          } else {
            this.metrics.mean("FilterIdle", 1);
            this.esMetrics.mean("FilterIdle");
            Thread.sleep(POLL_STEP_MS);
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception in worker thread: ", e);
      this.metrics.meter("FilterError");
      this.esMetrics.meter("FilterError");
    } finally {
      consumer.close();
    }

    LOG.warn("Shutting down");
  }

  private FilterMessage processMessage(ListenerMessage message) throws InterruptedException {
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
      long rtFilterRules = this.filters.test(message);
      outMessage.setRtRuleFlags(rtFilterRules);
    } catch (Exception e) {
      outMessage.setRtRuleFlags(Long.valueOf(FilterRuleType.ERROR.getRuleDigitPosition()));
    }
    return outMessage;
  }

  /**
   * Lookup publisherId based on campaignId from publisher cache
   *
   * @param campaignId
   * @return publisher, in case no match result, use default -1L
   */
  private Long getPublisherId(Long campaignId) throws InterruptedException {
    long publisher = CampaignPublisherMappingCache.getInstance().lookup(campaignId);
    if (publisher == DEFAULT_PUBLISHER_ID) {
      LOG.debug(String.format("No match result for campaign %d, use default -1L as publisherId", campaignId));
      return DEFAULT_PUBLISHER_ID;
    }
    return publisher;
  }
}
