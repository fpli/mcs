package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
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
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ebay.app.raptor.chocolate.filter.service.FilterUtils.*;

/**
 * Created by yliu29 on 2/23/18.
 */
public class FilterWorker extends Thread {
  private static final Logger LOG = Logger.getLogger(FilterWorker.class);

  private static final long POLL_STEP_MS = 100;
  private static final long RESULT_POLL_STEP_MS = 500;
  private static final long RESULT_POLL_TINEOUT_MS = 5000;
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

  private final int maxThreadNum = 10;
  private final ExecutorService executor = Executors.newFixedThreadPool(maxThreadNum);
  private final CompletionService<FilterMessage> completionService =
    new ExecutorCompletionService<>(executor);


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
    executor.shutdown();
  }

  @Override
  public void run() {
    LOG.info("Start filter worker, channel " + channelType +
            ", input topic " + inputTopic + ", output topic " + outputTopic);

    // Init the metrics that we don't use often
    metrics.meter("FilterError", 0);
    metrics.meter("messageParseFailure", 0);
    metrics.mean("FilterPassedPPM", 0);

    try {
      consumer.subscribe(Arrays.asList(inputTopic));

      long flushThreshold = 0;

      final long kafkaLagMetricInterval = 30000; // 30s
      final long kafkaCommitInterval = 15000; // 15s
      long kafkaLagMetricStart = System.currentTimeMillis();
      long kafkaCommitTime = System.currentTimeMillis();
      while (!shutdownRequested.get()) {
        try {
          long pollStartTime = System.currentTimeMillis();
          ConsumerRecords<Long, ListenerMessage> records = consumer.poll(Duration.ofMillis(POLL_STEP_MS));
          metrics.mean("PollLatency", System.currentTimeMillis() - pollStartTime);
          Iterator<ConsumerRecord<Long, ListenerMessage>> iterator = records.iterator();
          int count = 0;
          int passed = 0;
          long startTime = System.currentTimeMillis();
          while (iterator.hasNext()) {
            int threadNum = 0;
            long theadPoolstartTime = System.currentTimeMillis();
            Map<Long, ListenerMessage> inputMessages = new HashMap<>();

            for (int i = 0; i < maxThreadNum && iterator.hasNext(); i++) {
              ConsumerRecord<Long, ListenerMessage> record = iterator.next();

              // cache input messages
              inputMessages.put(record.key(), record.value());

              ListenerMessage message = record.value();
              metrics.meter("FilterInputCount", 1, message.getTimestamp(),
                      Field.of(CHANNEL_ACTION, message.getChannelAction().toString()),
                      Field.of(CHANNEL_TYPE, message.getChannelType().toString()));
              long latency = System.currentTimeMillis() - message.getTimestamp();
              metrics.mean("FilterLatency", latency, Field.of(CHANNEL_TYPE, message.getChannelType().toString()));

              ++count;
              metrics.meter("FilterThroughput", 1, message.getTimestamp(),
                      Field.of(CHANNEL_ACTION, message.getChannelAction().toString()),
                      Field.of(CHANNEL_TYPE, message.getChannelType().toString()));

              completionService.submit(() -> processMessage(record.value(), false));
              threadNum++;
            }

            // get result
            int received = 0;
            long start = System.currentTimeMillis();
            long end = start;
            Map<Long, FilterMessage> outputMessages = new HashMap<>();
            while (received < threadNum && (end - start < RESULT_POLL_TINEOUT_MS)) {
              Future<FilterMessage> resultFuture = completionService.poll(RESULT_POLL_STEP_MS,
                TimeUnit.MILLISECONDS);
              FilterMessage outMessage;
              if (resultFuture != null) {
                outMessage = resultFuture.get();
                outputMessages.put(outMessage.getSnapshotId(), outMessage);
                received++;
              }
            }

            // rerun filter rules with default geo_id and publisher_id for messages not polled out
            for (Map.Entry<Long, ListenerMessage> entry : inputMessages.entrySet()) {
              if (!outputMessages.containsKey(entry.getKey())) {
                metrics.meter("FilterPollResultFailure");
                LOG.warn("Poll failed, rerun filter rules");
                FilterMessage missingMessage = processMessage(entry.getValue(), true);
                outputMessages.put(missingMessage.getSnapshotId(), missingMessage);
              }
            }

            // send output messages to kafka
            for (FilterMessage outputMessage : outputMessages.values()) {
              if (outputMessage.getRtRuleFlags() == 0) {
                ++passed;
                metrics.meter("FilterPassedCount", 1, outputMessage.getTimestamp(),
                  Field.of(CHANNEL_ACTION, outputMessage.getChannelAction().toString()),
                  Field.of(CHANNEL_TYPE, outputMessage.getChannelType().toString()));
              }
              long sendKafkaStartTime = System.currentTimeMillis();
              // If the traffic is received from rover bes pipeline, we will send it to NewROITopic
              // this traffic will not be tracked into imk table
              if (isRoverBESRoi(outputMessage)) {
                producer.send(new ProducerRecord<>(ApplicationOptions.getInstance().getNewROITopic(), outputMessage.getSnapshotId(), outputMessage), KafkaSink.callback);
                metrics.mean("SendKafkaLatency", System.currentTimeMillis() - sendKafkaStartTime);
                metrics.meter("NewROICount", 1, outputMessage.getTimestamp(),
                  Field.of(CHANNEL_ACTION, outputMessage.getChannelAction().toString()),
                  Field.of(CHANNEL_TYPE, outputMessage.getChannelType().toString()));
              } else {
                producer.send(new ProducerRecord<>(outputTopic, outputMessage.getSnapshotId(), outputMessage), KafkaSink.callback);
                metrics.mean("SendKafkaLatency", System.currentTimeMillis() - sendKafkaStartTime);
              }
            }

            // For messages not polled out successfully,
            metrics.mean("FilterThreadPoolLatency", System.currentTimeMillis() - theadPoolstartTime);
          }

          long now = System.currentTimeMillis();

          // flush logic
          flushThreshold += count;

          if (flushThreshold > 1000 || now - kafkaCommitTime > kafkaCommitInterval) {
            long flushStartTime = System.currentTimeMillis();
            // producer flush
            producer.flush();

            // update consumer offset
            consumer.commitSync();

            metrics.mean("FlushLatency", System.currentTimeMillis() - flushStartTime);

            // reset threshold
            flushThreshold = 0;

            // reset commit time
            kafkaCommitTime = now;
          }

          // kafka lag metrics logic
          if (now - kafkaLagMetricStart > kafkaLagMetricInterval) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(records.partitions());
            Iterator<Map.Entry<TopicPartition, Long>> iter = endOffsets.entrySet().iterator();
            while (iter.hasNext()) {
              Map.Entry<TopicPartition, Long> entry = iter.next();
              TopicPartition tp = entry.getKey();
              long endOffset = entry.getValue();
              long offset = consumer.position(tp);
              metrics.mean("FilterKafkaConsumerLag", endOffset - offset,
                      Field.of("topic", tp.topic()),
                      Field.of("consumer", tp.partition()));
            }

            kafkaLagMetricStart = now;
          }

          if (count == 0) {
            metrics.mean("FilterIdle");
            Thread.sleep(POLL_STEP_MS);
          } else {
            metrics.mean("FilterPassedPPM", 100L * passed / count);
            long timeSpent = System.currentTimeMillis() - startTime;
            metrics.mean("FilterProcessingTime", timeSpent);

            if (timeSpent >= POLL_STEP_MS) {
              this.metrics.mean("FilterIdle", 0);
            } else {
              this.metrics.mean("FilterIdle");
              Thread.sleep(POLL_STEP_MS);
            }
          }
        } catch (Exception e) {
          if (e instanceof IllegalStateException &&
              e.getMessage().startsWith("Coordinator selected invalid")) {
            LOG.warn("Exception in worker thread: ", e);
            metrics.meter("CoordinatorSelectedError");
            Thread.sleep(30000); // sleep for 30s
          } else if (e instanceof SchemaException) {
            LOG.warn("Exception in worker thread: ", e);
            metrics.meter("SchemaReadError");
            Thread.sleep(30000);
          } else {
            LOG.warn("Exception in worker thread: ", e);
            metrics.meter("FilterError");
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception in worker thread: ", e);
      this.metrics.meter("FilterSubscribeError");
    } finally {
      consumer.close();
    }

    LOG.warn("Shutting down");
  }

  private FilterMessage processMessage(ListenerMessage message, Boolean isPollFailed) throws InterruptedException {
    long processStartTime = System.currentTimeMillis();
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
    // checked imk history data, only click has geoid
    if (message.getChannelType() != ChannelType.EPN && message.getChannelAction() == ChannelAction.CLICK) {
      if (!isPollFailed) {
        try {
          outMessage.setGeoId(LBSClient.getInstance().getPostalCodeByIp(outMessage.getRemoteIp()));
        } catch (Exception e) {
          LOG.warn("Exception in call GEO service: ", e);
          this.metrics.meter("FilterGEOError");
        }
      } else {
        // not change geoid for missing messages
        outMessage.setGeoId(message.getGeoId());
      }
    }
    // only EPN needs to get publisher id
    if (message.getPublisherId() == DEFAULT_PUBLISHER_ID && message.getChannelType() == ChannelType.EPN &&
      !isPollFailed) {
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
      LOG.warn("Exception when execute rtFilterRules: ", e);
      this.metrics.meter("FilterRtRulesError");
    }
    metrics.mean("ProcessLatency", System.currentTimeMillis() - processStartTime);
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
