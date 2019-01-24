package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleType;
import com.ebay.app.raptor.chocolate.filter.lbs.LBSClient;
import com.ebay.app.raptor.chocolate.filter.util.CampaignPublisherMappingCache;
import com.ebay.traffic.chocolate.kafka.KafkaConsumerFactory;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
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

  private static final String CHANNEL_ACTION = "channelAction";
  private static final String CHANNEL_TYPE = "channelType";

  private final Metrics metrics;
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
    this.metrics = ESMetrics.getInstance();
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
    this.metrics.mean("FilterPassedPPM", 0);

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
          metrics.meter("FilterInputCount", 1, message.getTimestamp(),
              Field.of(CHANNEL_ACTION, message.getChannelAction().toString()),
              Field.of(CHANNEL_TYPE, message.getChannelType().toString()));
          long latency = System.currentTimeMillis() - message.getTimestamp();
          metrics.mean("FilterLatency", latency);

          ++count;
          metrics.meter("FilterThroughput", 1, message.getTimestamp(),
              Field.of(CHANNEL_ACTION, message.getChannelAction().toString()),
              Field.of(CHANNEL_TYPE, message.getChannelType().toString()));

          FilterMessage outMessage = processMessage(message);

          if (outMessage.getRtRuleFlags() == 0) {
            ++passed;
            metrics.meter("FilterPassedCount", 1, outMessage.getTimestamp(),
                Field.of(CHANNEL_ACTION, outMessage.getChannelAction().toString()),
                Field.of(CHANNEL_TYPE, outMessage.getChannelType().toString()));
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
              metrics.mean("FilterKafkaConsumerLag", endOffset - offset, Field.of(CHANNEL_TYPE, channelType.toString()),
                  Field.of("consumer", tp.partition()));
            }
          }

          kafkaLagMetricStart = now;
        }

        if (count == 0) {
          metrics.mean("FilterIdle");
          Thread.sleep(POLL_STEP_MS);
        } else {
          metrics.mean("FilterPassedPPM", 1000000L * passed / count);
          long timeSpent = System.currentTimeMillis() - startTime;
          metrics.mean("FilterProcessingTime", timeSpent);

          if (timeSpent >= POLL_STEP_MS) {
            this.metrics.mean("FilterIdle", 0);
          } else {
            this.metrics.mean("FilterIdle");
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

  private FilterMessage processMessage(ListenerMessage message) throws InterruptedException {
    FilterMessage outMessage = new FilterMessage();
    outMessage.setSnapshotId(message.getSnapshotId());
    outMessage.setShortSnapshotId(message.getShortSnapshotId());
    outMessage.setTimestamp(message.getTimestamp());
    outMessage.setUserId(message.getUserId());
    outMessage.setCguid(message.getCguid());
    outMessage.setGuid(message.getGuid());
    outMessage.setRemoteIp(message.getRemoteIp());
    outMessage.setLangCd(message.getLangCd());
    outMessage.setUserAgent(message.getUserAgent());
    outMessage.setGeoId(message.getGeoId());
    outMessage.setUdid(message.getUdid());
    outMessage.setReferer(message.getReferer());
    // set postcode for not EPN Channels
    if (message.getChannelType() != ChannelType.EPN) {
      outMessage.setGeoId(LBSClient.getInstance().getPostalCodeByIp(outMessage.getRemoteIp()));
    }
    // only EPN needs to get publisher id
    if (message.getPublisherId() == DEFAULT_PUBLISHER_ID && message.getChannelType() == ChannelType.EPN) {
      long publisherId = getPublisherId(message.getCampaignId());
      outMessage.setPublisherId(publisherId);
      message.setPublisherId(publisherId);
    }
    else {
      outMessage.setPublisherId(message.getPublisherId());
    }
    outMessage.setCampaignId(message.getCampaignId());
    outMessage.setSiteId(message.getSiteId());
    outMessage.setLandingPageUrl(message.getLandingPageUrl());
    outMessage.setSrcRotationId(message.getSrcRotationId());
    outMessage.setDstRotationId(message.getDstRotationId());
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
