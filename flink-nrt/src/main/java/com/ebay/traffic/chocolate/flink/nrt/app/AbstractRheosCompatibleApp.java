package com.ebay.traffic.chocolate.flink.nrt.app;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Receive messages from rheos topics, apply ETL and send output messages to another topics.
 *
 * @param <IN>  Type of the elements in the DataStream created from the this source
 * @param <OUT> The type of the elements in this stream.
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public abstract class AbstractRheosCompatibleApp<IN, OUT> {

  protected StreamExecutionEnvironment streamExecutionEnvironment;

  // Time interval between state checkpoints in milliseconds
  protected static final long CHECK_POINT_PERIOD = TimeUnit.SECONDS.toMillis(180);

  // The minimal pause before the next checkpoint is triggered
  protected static final long MIN_PAUSE_BETWEEN_CHECK_POINTS = TimeUnit.SECONDS.toMillis(1);

  // The checkpoint timeout, in milliseconds
  protected static final long CHECK_POINT_TIMEOUT = TimeUnit.SECONDS.toMillis(300);

  // The maximum number of concurrent checkpoint attempts
  protected static final int MAX_CONCURRENT_CHECK_POINTS = 1;

  void run() throws Exception {
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    prepareBaseExecutionEnvironment();
    DataStreamSource<IN> tuple2DataStreamSource = streamExecutionEnvironment.addSource(getKafkaConsumer());
    DataStream<OUT> output = transform(tuple2DataStreamSource);
    output.addSink(getKafkaProducer());
    streamExecutionEnvironment.execute(this.getClass().getSimpleName());
  }

  protected void prepareBaseExecutionEnvironment() {
    streamExecutionEnvironment.enableCheckpointing(CHECK_POINT_PERIOD);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(MIN_PAUSE_BETWEEN_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(CHECK_POINT_TIMEOUT);
    streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig()
        .enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
  }

  /**
   * Override this method to define ETL logic
   *
   * @param dataStreamSource source data stream
   * @return target data stream
   */
  protected abstract DataStream<OUT> transform(DataStreamSource<IN> dataStreamSource);

  /**
   * Override this method to define Kafka consumer
   *
   * @return Kafka consumer
   */
  protected abstract SourceFunction<IN> getKafkaConsumer();

  /**
   * Override this method to define Kafka producer
   *
   * @return Kafka producer
   */
  protected abstract SinkFunction<OUT> getKafkaProducer();

  /**
   * Oerride this method to get the actual consumer topics from config file
   *
   * @return kafka consumer topics
   */
  protected abstract List<String> getConsumerTopics();

  /**
   * Override this method to get the actual consumer properties from config file
   *
   * @return kafka consumer properties
   */
  protected abstract Properties getConsumerProperties();

  /**
   * Override this method to get the actual get producer topics from config file
   *
   * @return kafka producer topics
   */
  protected abstract String getProducerTopic();

  /**
   * Override this method to get producer properties from config file
   *
   * @return kafka producer properties
   */
  protected abstract Properties getProducerProperties();

}
