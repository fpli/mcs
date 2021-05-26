package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.flink.nrt.function.ElasticSearchRestClientFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Receive messages from Rheos and sink them to Elastic Search.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public abstract class AbstractRheosElasticSearchCompatibleApp<IN, OUT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRheosElasticSearchCompatibleApp.class);
  private static final String URI = "uri";
  private static final String APP_KEY = "appKey";
  private static final String APP_SECRET = "appSecret";
  // maximum number of actions to buffer per bulk request.
  private static final int NUM_MAX_ACTIONS = 1000;
  protected StreamExecutionEnvironment streamExecutionEnvironment;

  private static final long DEFAULT_CHECK_POINT_PERIOD = TimeUnit.MINUTES.toMillis(3);

  private static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS = TimeUnit.SECONDS.toMillis(30);

  private static final long DEFAULT_CHECK_POINT_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

  private static final int DEFAULT_MAX_CONCURRENT_CHECK_POINTS = 1;

  protected void run() throws Exception {

    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    prepareBaseExecutionEnvironment();
    DataStreamSource<IN> tuple2DataStreamSource = streamExecutionEnvironment.addSource(getKafkaConsumer());
    DataStream<OUT> output = transform(tuple2DataStreamSource);
    output.addSink(getElasticSearchSink());
    streamExecutionEnvironment.execute(this.getClass().getSimpleName());
  }

  private ElasticsearchSink<OUT> getElasticSearchSink() {
    Properties properties = getElasticSearchProperties();
    ElasticsearchSink.Builder<OUT> esSinkBuilder = new ElasticsearchSink.Builder<>(
            Collections.singletonList(HttpHost.create(properties.getProperty(URI))), getElasticSearchSinkFunction()
    );
    esSinkBuilder.setFailureHandler((ActionRequestFailureHandler) (actionRequest, throwable, i, requestIndexer) -> {
      // log error but not throw exceptions
      LOGGER.warn("Fail to write es.");
    });

    esSinkBuilder.setBulkFlushMaxActions(NUM_MAX_ACTIONS);
    String appKey = properties.getProperty(APP_KEY);
    String appSecret = properties.getProperty(APP_SECRET);
    esSinkBuilder.setRestClientFactory(new ElasticSearchRestClientFactory(appKey, appSecret));
    return esSinkBuilder.build();
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

  protected abstract List<String> getConsumerTopics();

  protected abstract Properties getConsumerProperties();

  protected abstract Properties getElasticSearchProperties();

  protected abstract ElasticsearchSinkFunction<OUT> getElasticSearchSinkFunction();

}