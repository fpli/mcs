package com.ebay.traffic.chocolate.flink.nrt.app;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Receive messages from rheos topics, apply ETL and send messages to another topics.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public abstract class BaseRheosCompatibleApp<IN, OUT> {
  protected StreamExecutionEnvironment streamExecutionEnvironment;

  private static final long DEFAULT_CHECK_POINT_PERIOD = TimeUnit.SECONDS.toMillis(2);

  private static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS = TimeUnit.SECONDS.toMillis(1);

  private static final long DEFAULT_CHECK_POINT_TIMEOUT = TimeUnit.SECONDS.toMillis(60);

  private static final int DEFAULT_MAX_CONCURRENT_CHECK_POINTS = 1;

  protected void run() throws Exception {
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    prepareBaseExecutionEnvironment();
    DataStreamSource<IN> tuple2DataStreamSource = streamExecutionEnvironment.addSource(getKafkaConsumer());
    DataStream<OUT> output = transform(tuple2DataStreamSource);
    output.addSink(getKafkaProducer());
    streamExecutionEnvironment.execute();
  }

  protected void prepareBaseExecutionEnvironment() {
    streamExecutionEnvironment.enableCheckpointing(DEFAULT_CHECK_POINT_PERIOD);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(DEFAULT_CHECK_POINT_TIMEOUT);
    streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(DEFAULT_MAX_CONCURRENT_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
  }

  protected abstract DataStream<OUT> transform(DataStreamSource<IN> dataStreamSource);

  protected abstract FlinkKafkaConsumer<IN> getKafkaConsumer();

  protected abstract FlinkKafkaProducer<OUT> getKafkaProducer();

  /**
   * Get consumer topics from config file
   * @return kafka consumer topics
   */
  protected abstract List<String> getConsumerTopics();

  /**
   * Get properties from config file
   * @return kafka consumer properties
   */
  protected abstract Properties getConsumerProperties();

  /**
   * Get producer topics from config file
   * @return kafka producer topics
   */
  protected abstract String getProducerTopic();

  /**
   * Get producer properties from config file
   * @return kafka producer properties
   */
  protected abstract Properties getProducerProperties();

}
