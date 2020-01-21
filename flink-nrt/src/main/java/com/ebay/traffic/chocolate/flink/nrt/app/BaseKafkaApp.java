package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.flink.nrt.deserialization.Tuple2KeyedDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.Tuple2KeyedSerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.List;
import java.util.Properties;

/**
 * This class
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public abstract class BaseKafkaApp {
  protected StreamExecutionEnvironment streamExecutionEnvironment;

  private static final int DEFAULT_CHECK_POINT_PERIOD = 2000;

  private static final int DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS = 1000;

  private static final int DEFAULT_CHECK_POINT_TIMEOUT = 60000;

  private static final int DEFAULT_MAX_CONCURRENT_CHECK_POINTS = 1;

  protected void run() throws Exception {
    prepareExecutionEnvironment();
    DataStreamSource<Tuple2<Long, byte[]>> tuple2DataStreamSource = streamExecutionEnvironment.addSource(getKafkaConsumer());
    DataStream output = transform(tuple2DataStreamSource);
    output.addSink(getKafkaProducer());
    streamExecutionEnvironment.execute();
  }

  protected void prepareExecutionEnvironment() {
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    streamExecutionEnvironment.enableCheckpointing(DEFAULT_CHECK_POINT_PERIOD);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(DEFAULT_CHECK_POINT_TIMEOUT);
    streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(DEFAULT_MAX_CONCURRENT_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
  }

  protected abstract DataStream<Tuple2<Long, byte[]>> transform(DataStreamSource<Tuple2<Long, byte[]>> dataStreamSource);

  private FlinkKafkaConsumer<Tuple2<Long, byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(), new Tuple2KeyedDeserializationSchema(), getConsumerProperties());
  }

  private FlinkKafkaProducer<Tuple2<Long, byte[]>> getKafkaProducer() {
    return new FlinkKafkaProducer<>(getProducerTopic(), new Tuple2KeyedSerializationSchema(), getProducerProperties());
  }

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
