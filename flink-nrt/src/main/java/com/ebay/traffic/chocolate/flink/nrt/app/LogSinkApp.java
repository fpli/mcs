package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.flink.nrt.function.ElasticSearchRestClientFactory;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Receive log messages, and write them to elasticsearch.
 *
 * @author Zhiyuan Wang
 * @since 2021/08/01
 */
public class LogSinkApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogSinkApp.class);

  protected StreamExecutionEnvironment streamExecutionEnvironment;

  private static final long DEFAULT_CHECK_POINT_PERIOD = TimeUnit.MINUTES.toMillis(3);

  private static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS = TimeUnit.SECONDS.toMillis(30);

  private static final long DEFAULT_CHECK_POINT_TIMEOUT = TimeUnit.MINUTES.toMillis(3);

  private static final int DEFAULT_MAX_CONCURRENT_CHECK_POINTS = 1;

  protected Map<String, Object> config;

  public static void main(String[] args) throws Exception {
    LogSinkApp sinkApp = new LogSinkApp();
    sinkApp.run();
  }

  protected void loadProperty() {
    this.config = PropertyMgr.getInstance().loadYaml("log-sink-app.yaml");
  }

  protected void run() throws Exception {
    loadProperty();
    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    prepareBaseExecutionEnvironment();
    addSink(addSource());
    execute();
  }

  @SuppressWarnings("unchecked")
  private void execute() throws Exception {
    Map<String, Object> job = (Map<String, Object>) config.get("job");
    streamExecutionEnvironment.execute((String) job.get("name"));
  }

  @SuppressWarnings("unchecked")
  protected SingleOutputStreamOperator<Tuple2<String, String>> addSource() throws IOException {
    Map<String, Object> source = (Map<String, Object>) config.get("source");
    String name = (String) source.get("name");
    String uid = (String) source.get("uid");
    return streamExecutionEnvironment.addSource(buildConsumer()).name(name).uid(uid);
  }

  @SuppressWarnings("unchecked")
  protected void addSink(SingleOutputStreamOperator<Tuple2<String, String>> output) {
    Map<String, Object> sink = (Map<String, Object>) config.get("sink");
    String name = (String) sink.get("name");
    String uid = (String) sink.get("uid");
    ElasticsearchSink.Builder<Tuple2<String, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(
            Collections.singletonList(HttpHost.create((String) sink.get("uri"))), getElasticSearchSinkFunction()
    );
    esSinkBuilder.setFailureHandler((ActionRequestFailureHandler) (actionRequest, throwable, i, requestIndexer) -> {
      LOGGER.warn("Fail to write es.");
    });

    esSinkBuilder.setBulkFlushMaxActions(1000);
    String appKey = (String) sink.get("appKey");
    String appSecret = (String) sink.get("appSecret");
    esSinkBuilder.setRestClientFactory(new ElasticSearchRestClientFactory(appKey, appSecret));
    ElasticsearchSink<Tuple2<String, String>> build = esSinkBuilder.build();
    output.addSink(build).name(name).uid(uid);
  }

  protected ElasticsearchSinkFunction<Tuple2<String, String>> getElasticSearchSinkFunction() {
    return new CustomElasticsearchSinkFunction();
  }

  private static class CustomElasticsearchSinkFunction implements ElasticsearchSinkFunction<Tuple2<String, String>> {
    public IndexRequest createIndexRequest(Tuple2<String, String> element) {
      final String id = UUID.randomUUID().toString();

      Type type = new TypeToken<Map<String, Object>>() {}.getType();
      Gson gson = new Gson();
      Map<String, Object> json = gson.fromJson(element.f1, type);
      String timestamp = (String) json.get("@timestamp");
      String date = OffsetDateTime.parse(timestamp).format(DateTimeFormatter.ofPattern("yyyy.MM.dd"));
      return Requests.indexRequest()
              .index(element.f0 + "-" + date)
              .type("_doc")
              .id(id)
              .source(json);
    }

    @Override
    public void process(Tuple2<String, String> element, RuntimeContext ctx, RequestIndexer indexer) {
      indexer.add(createIndexRequest(element));
    }
  }

  @SuppressWarnings("unchecked")
  protected FlinkKafkaConsumer<Tuple2<String, String>> buildConsumer() throws IOException {
    Map<String, Object> source = (Map<String, Object>) config.get("source");
    String topics = (String) source.get("topic");

    Properties consumerProperties = new Properties();
    consumerProperties.load(new StringReader((String) source.get("properties")));

    return new FlinkKafkaConsumer<>(Arrays.asList(topics.split(",")), new KafkaDeserializationSchema(), consumerProperties);
  }

  protected void prepareBaseExecutionEnvironment() {
    streamExecutionEnvironment.enableCheckpointing(DEFAULT_CHECK_POINT_PERIOD);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(DEFAULT_CHECK_POINT_TIMEOUT);
    streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(DEFAULT_MAX_CONCURRENT_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
  }

  public static class KafkaDeserializationSchema implements org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema<Tuple2<String, String>> {
    @Override
    public Tuple2<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
      return new Tuple2<>(record.topic(), new String(record.value(), StandardCharsets.UTF_8));
    }

    @Override
    public TypeInformation<Tuple2<String, String>> getProducedType() {
      return TypeInformation.of(new TypeHint<Tuple2<String, String>>() { });
    }

    @Override
    public boolean isEndOfStream(Tuple2<String, String> nextElement) {
      return false;
    }
  }

}
