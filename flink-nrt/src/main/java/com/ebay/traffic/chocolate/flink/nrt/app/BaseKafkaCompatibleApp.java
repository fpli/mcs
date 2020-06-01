package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.Tuple2KeyedDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.Tuple2KeyedSerializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
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
 * Receive messages from Kafka topics, apply ETL and send messages to another topics.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public abstract class BaseKafkaCompatibleApp {
  protected StreamExecutionEnvironment streamExecutionEnvironment;

  private static final long DEFAULT_CHECK_POINT_PERIOD = TimeUnit.SECONDS.toMillis(2);

  private static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS = TimeUnit.SECONDS.toMillis(1);

  private static final long DEFAULT_CHECK_POINT_TIMEOUT = TimeUnit.SECONDS.toMillis(60);

  private static final int DEFAULT_MAX_CONCURRENT_CHECK_POINTS = 1;

  protected void run() throws Exception {
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    prepareBaseExecutionEnvironment();
    setGlobalJobParameters();
    DataStreamSource<Tuple2<Long, byte[]>> tuple2DataStreamSource = streamExecutionEnvironment.addSource(getKafkaConsumer());
    DataStream<Tuple2<Long, byte[]>> output = transform(tuple2DataStreamSource);
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

  private void setGlobalJobParameters() {
    streamExecutionEnvironment.getConfig().setGlobalJobParameters(getGlobalJobParameters());
  }

  protected Configuration getGlobalJobParameters() {
    Configuration configuration = new Configuration();
    configuration.addAll(getESMetricsConfiguration());
    return configuration;
  }

  private Configuration getESMetricsConfiguration() {
    Configuration configuration = new Configuration();
    configuration.setString(PropertyConstants.ELASTICSEARCH_URL, PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.APPLICATION_PROPERTIES)
            .getProperty(PropertyConstants.ELASTICSEARCH_URL));
    configuration.setString(PropertyConstants.ELASTICSEARCH_INDEX_PREFIX, PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.APPLICATION_PROPERTIES)
            .getProperty(PropertyConstants.ELASTICSEARCH_INDEX_PREFIX));
    return configuration;
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
