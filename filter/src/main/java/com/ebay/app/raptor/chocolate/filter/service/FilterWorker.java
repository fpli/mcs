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
import com.ebay.traffic.chocolate.spark.monitor.MonitorUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ebay.app.raptor.chocolate.filter.service.FilterUtils.*;

/**
 * Created by yliu29 on 2/23/18.
 */
public class FilterWorker extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(FilterWorker.class);

  private static final long POLL_STEP_MS = 100;
  private static final long RESULT_POLL_STEP_MS = 100;
  private static final long RESULT_POLL_TIMEOUT_MS = 10000;
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

  private static final int maxThreadNum = 200;
  private static final ExecutorService executor = Executors.newFixedThreadPool(maxThreadNum);
  private static final BlockingQueue<Future<FilterMessage>> completionQueue =
          new EmptyQueue<>();
  private static final CompletionService<FilterMessage> completionService =
    new ExecutorCompletionService<>(executor, completionQueue);

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
    MonitorUtil.info("FilterError", 0);
    MonitorUtil.info("messageParseFailure", 0);
    MonitorUtil.latency("FilterPassedPPM", 0);

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
          MonitorUtil.latency("PollLatency", System.currentTimeMillis() - pollStartTime);
          Iterator<ConsumerRecord<Long, ListenerMessage>> iterator = records.iterator();
          int count = 0;
          int passed = 0;
          long startTime = System.currentTimeMillis();
          while (iterator.hasNext()) {
            long theadPoolstartTime = System.currentTimeMillis();
            Map<Long, ListenerMessage> inputMessages = new HashMap<>();
            List<Future<FilterMessage>> filterFuture = new ArrayList<>();
            for (int i = 0; i < maxThreadNum && iterator.hasNext(); i++) {
              ConsumerRecord<Long, ListenerMessage> record = iterator.next();

              ListenerMessage message = record.value();
              // cache input messages
              inputMessages.put(message.getSnapshotId(), message);

              MonitorUtil.info("FilterInputCount", 1,
                      Field.of(CHANNEL_ACTION, message.getChannelAction().toString()),
                      Field.of(CHANNEL_TYPE, message.getChannelType().toString()));

              long latency = System.currentTimeMillis() - message.getTimestamp();
              MonitorUtil.latency("FilterLatency", latency, Field.of(CHANNEL_TYPE, message.getChannelType().toString()));

              ++count;
              MonitorUtil.info("FilterThroughput", 1,
                      Field.of(CHANNEL_ACTION, message.getChannelAction().toString()),
                      Field.of(CHANNEL_TYPE, message.getChannelType().toString()));

              Future<FilterMessage> result = completionService.submit(() -> processMessage(message, false));
              filterFuture.add(result);
            }

            // get result
            long start = System.currentTimeMillis();
            long end = start;
            Map<Long, FilterMessage> outputMessages = new HashMap<>();
            while ((end - start < RESULT_POLL_TIMEOUT_MS) && (outputMessages.size() < inputMessages.size())) {

              Iterator<Future<FilterMessage>> fi = filterFuture.iterator();
              while (fi.hasNext()) {
                Future<FilterMessage> fm = fi.next();
                if (fm.isDone()) {
                  FilterMessage m = null;
                  try {
                    m = fm.get();
                  } catch (Exception e) {
                  }
                  if (m != null) {
                    outputMessages.put(m.getSnapshotId(), m);
                  }
                  fi.remove();
                }
              }

              if (outputMessages.size() < inputMessages.size()) {
                Thread.sleep(RESULT_POLL_STEP_MS);
              }
              end = System.currentTimeMillis();
            }

            Iterator<Future<FilterMessage>> fi = filterFuture.iterator();
            while (fi.hasNext()) {
              Future<FilterMessage> fm = fi.next();
              try {
                fm.cancel(true);
              } catch (Exception e) {
              }
            }

            // rerun filter rules with default geo_id and publisher_id for messages not polled out
            if (outputMessages.size() < inputMessages.size()) {
              for (Map.Entry<Long, ListenerMessage> entry : inputMessages.entrySet()) {
                if (!outputMessages.containsKey(entry.getKey())) {
                  MonitorUtil.info("FilterPollResultFailure");

                  LOG.warn("Poll failed, rerun filter rules");
                  FilterMessage missingMessage = processMessage(entry.getValue(), true);
                  outputMessages.put(missingMessage.getSnapshotId(), missingMessage);
                }
              }
            }

            // send output messages to kafka
            for (FilterMessage outputMessage : outputMessages.values()) {
              if (outputMessage.getRtRuleFlags() == 0) {
                ++passed;
                MonitorUtil.info("FilterPassedCount", 1,
                  Field.of(CHANNEL_ACTION, outputMessage.getChannelAction().toString()),
                  Field.of(CHANNEL_TYPE, outputMessage.getChannelType().toString()));

              }
              long sendKafkaStartTime = System.currentTimeMillis();
              // If the traffic is received from rover bes pipeline, we will send it to NewROITopic
              // this traffic will not be tracked into imk table
              if (isRoverBESRoi(outputMessage)) {
                producer.send(new ProducerRecord<>(ApplicationOptions.getInstance().getNewROITopic(), outputMessage.getSnapshotId(), outputMessage), KafkaSink.callback);
                MonitorUtil.latency("SendKafkaLatency", System.currentTimeMillis() - sendKafkaStartTime);
                MonitorUtil.info("NewROICount", 1,
                  Field.of(CHANNEL_ACTION, outputMessage.getChannelAction().toString()),
                  Field.of(CHANNEL_TYPE, outputMessage.getChannelType().toString()));

              } else {
                producer.send(new ProducerRecord<>(outputTopic, outputMessage.getSnapshotId(), outputMessage), KafkaSink.callback);
                MonitorUtil.latency("SendKafkaLatency", System.currentTimeMillis() - sendKafkaStartTime);
              }
            }

            MonitorUtil.latency("FilterThreadPoolLatency", System.currentTimeMillis() - theadPoolstartTime);
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

            MonitorUtil.latency("FlushLatency", System.currentTimeMillis() - flushStartTime);

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
              MonitorUtil.latency("FilterKafkaConsumerLag", endOffset - offset,
                      Field.of("topic", tp.topic()),
                      Field.of("consumer", tp.partition()));
            }

            kafkaLagMetricStart = now;
          }

          if (count == 0) {
            MonitorUtil.latency("FilterIdle");
            Thread.sleep(POLL_STEP_MS);
          } else {
            MonitorUtil.latency("FilterPassedPPM", 100L * passed / count);
            long timeSpent = System.currentTimeMillis() - startTime;
            MonitorUtil.latency("FilterProcessingTime", timeSpent);

            if (timeSpent >= POLL_STEP_MS) {
              MonitorUtil.latency("FilterIdle", 0);
            } else {
              MonitorUtil.latency("FilterIdle");
              Thread.sleep(POLL_STEP_MS);
            }
          }
        } catch (Exception e) {
          if (e instanceof IllegalStateException &&
              e.getMessage().startsWith("Coordinator selected invalid")) {
            LOG.warn("Exception in worker thread: ", e);
            MonitorUtil.info("CoordinatorSelectedError");

            Thread.sleep(30000); // sleep for 30s
          } else if (e instanceof SchemaException) {
            LOG.warn("Exception in worker thread: ", e);
            MonitorUtil.info("SchemaReadError");

            Thread.sleep(30000);
          } else {
            LOG.warn("Exception in worker thread: ", e);
            MonitorUtil.info("FilterError");

            Thread.sleep(5000);
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception in worker thread: ", e);
      MonitorUtil.info("FilterSubscribeError");

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
    if (message.getChannelType() != ChannelType.EPN && message.getChannelAction() == ChannelAction.CLICK &&
      !isPollFailed && (outMessage.getGeoId() == null || outMessage.getGeoId() == 0)) {
      try {
        outMessage.setGeoId(LBSClient.getInstance().getPostalCodeByIp(outMessage.getRemoteIp()));
      } catch (Exception e) {
        LOG.warn("Exception in call GEO service: ", e);
        MonitorUtil.info("FilterGEOError");

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
      MonitorUtil.info("FilterRtRulesError");

    }
    MonitorUtil.latency("ProcessLatency", System.currentTimeMillis() - processStartTime);

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
