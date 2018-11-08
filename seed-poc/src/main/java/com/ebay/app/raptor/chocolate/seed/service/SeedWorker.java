package com.ebay.app.raptor.chocolate.seed.service;

import com.ebay.app.raptor.chocolate.seed.entity.SeedOutMessage;
import com.ebay.app.raptor.chocolate.seed.util.CouchbaseClient;
import com.ebay.app.raptor.chocolate.seed.util.RestClient;
import com.ebay.kernel.util.StringUtils;
import com.ebay.traffic.chocolate.kafka.KafkaConsumerFactory2;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.kafka.KafkaSink2;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by yliu29 on 2/23/18.
 */
public class SeedWorker extends Thread {
  private static final Logger LOG = Logger.getLogger(SeedWorker.class);

  private static final long POLL_STEP_MS = 100;
  private static final String MSG_TIME = "time";
  private static final String MSG_USER_ID = "userId";
  private static final String MSG_DEVICE_ID = "deviceId";
  private static final String MSG_DEVICE_TYPE = "deviceType";
  private static final String MSG_DEVICE_UNKNOW = "UNKNOW";
  private static final String MSG_DEVICE_DESKTOP = "desktop";
  private final ESMetrics esMetrics;
  private final String channelType;
  private final String inputTopic;
  private final String outputTopic;
  private String seedPJEndpoint;
  private final Consumer<String, JSONObject> consumer; // in
  private final Producer<Long, SeedOutMessage> producer; // out
  // Shutdown signal.
  private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
  private final CouchbaseClient cbClient;


  public SeedWorker(String channelType, String inputTopic, Properties kafkaProperties, String outputTopic, String seedPJEndpoint,CouchbaseClient cbClient) {
    this.esMetrics = ESMetrics.getInstance();
    this.channelType = channelType;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;

    this.consumer = KafkaConsumerFactory2.create(kafkaProperties);
    this.producer = KafkaSink2.get();
    this.seedPJEndpoint = seedPJEndpoint;
    this.cbClient = cbClient;
  }

  /**
   * Signals termination.
   */
  public void shutdown() {
    LOG.info("Calling shutdown in SeedWorker.");
    shutdownRequested.set(true);
  }

  @Override
  public void run() {
    LOG.info("Start seed worker, channel input topic " + inputTopic + ", output topic " + outputTopic);

    // Init the metrics that we don't use often
    this.esMetrics.meter("SeedError", 0);
    this.esMetrics.meter("messageParseFailure", 0);
    this.esMetrics.mean("SeedPassedPPM", 0);

    try {
      consumer.subscribe(Arrays.asList(inputTopic));

      long flushThreshold = 0;
      Gson gson = new Gson();
      final long kafkaLagMetricInterval = 30000; // 30s
      Map<Integer, Long> offsets = new HashMap<>();
      long kafkaLagMetricStart = System.currentTimeMillis();
      ConsumerRecords<String, JSONObject> recordList;
      Iterator<ConsumerRecord<String, JSONObject>> iterator;
      ConsumerRecord<String, JSONObject> record;
      SeedOutMessage seedOutMessage;
      while (!shutdownRequested.get()) {
        recordList = consumer.poll(POLL_STEP_MS);
        iterator = recordList.iterator();
        int count = 0;
        int passed = 0;
        long startTime = System.currentTimeMillis();
        while (iterator.hasNext()) {
          record = iterator.next();

          // the timestamp in input message
          Long msgTimestamp = 0l;
          if (record.value().get(MSG_TIME) != null) msgTimestamp = record.value().getLong(MSG_TIME);
          msgTimestamp = msgTimestamp == 0 ? record.timestamp() : msgTimestamp;
          // the deviceType in input message
          String deviceType = MSG_DEVICE_UNKNOW;
          if (record.value().get(MSG_DEVICE_TYPE) != null) deviceType = record.value().getString(MSG_DEVICE_TYPE);
          // the userId in input message
          String userId = null;
          if (record.value().get(MSG_USER_ID) != null) userId = record.value().getString(MSG_USER_ID);
          if (userId == null) {
            //TODO GET userId by XID API
            String deviceId = record.value().getString(MSG_DEVICE_ID);
            if (MSG_DEVICE_DESKTOP.equals(deviceType)) {
              //userId = XIDAPI.getByCguid()
            } else {
              //userId = XIDAPI.getByDeviceId()
            }
            esMetrics.meter("SeedUserNull", 1, msgTimestamp, channelType, deviceType);
            continue;
          } else {
            seedPJEndpoint = String.format(seedPJEndpoint, userId);
            cbClient.upsert(userId, System.currentTimeMillis());
          }
          esMetrics.meter("SeedInputCount", 1, msgTimestamp, channelType, deviceType);
          long latency = System.currentTimeMillis() - msgTimestamp;
          esMetrics.mean("SeedLatency", latency);

          ++count;
          esMetrics.meter("SeedThroughput", 1, msgTimestamp, channelType, deviceType);


          String pjResp = RestClient.get(seedPJEndpoint);
          seedOutMessage = StringUtils.isEmpty(pjResp) ? null : gson.fromJson(pjResp, SeedOutMessage.class);

          if (seedOutMessage.getGroup() == null || seedOutMessage.getGroup().length == 0) {
            ++passed;
            esMetrics.meter("Seed0GroupCount", 1, seedOutMessage.getTimestamp(), channelType, deviceType);
          }

          // cache current offset for partition*
          offsets.put(record.partition(), record.offset());

          producer.send(new ProducerRecord<>(outputTopic, seedOutMessage.getTimestamp(), seedOutMessage), KafkaSink.callback);
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
          Map<TopicPartition, Long> endOffsets = consumer.endOffsets(recordList.partitions());
          Iterator<Map.Entry<TopicPartition, Long>> iter = endOffsets.entrySet().iterator();
          while (iter.hasNext()) {
            Map.Entry<TopicPartition, Long> entry = iter.next();
            TopicPartition tp = entry.getKey();
            long endOffset = entry.getValue();
            if (offsets.containsKey(tp.partition())) {
              long offset = offsets.get(tp.partition());
              Map<String, Object> additionalFields = new HashMap<>();
              additionalFields.put("consumer", tp.partition());
              esMetrics.mean("SeedKafkaConsumerLag", endOffset - offset, additionalFields);
            }
          }

          kafkaLagMetricStart = now;
        }

        if (count == 0) {
          esMetrics.mean("SeedIdle");
          Thread.sleep(POLL_STEP_MS);
        } else {
          esMetrics.mean("SeedPassedPPM", 1000000L * passed / count);
          long timeSpent = System.currentTimeMillis() - startTime;
          esMetrics.mean("SeedProcessingTime", timeSpent);

          if (timeSpent >= POLL_STEP_MS) {
            this.esMetrics.mean("SeedIdle", 0);
          } else {
            this.esMetrics.mean("SeedIdle");
            Thread.sleep(POLL_STEP_MS);
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Exception in worker thread: ", e);
      this.esMetrics.meter("SeedError");
    } finally {
      consumer.close();
    }

    LOG.warn("Shutting down");
  }
}
