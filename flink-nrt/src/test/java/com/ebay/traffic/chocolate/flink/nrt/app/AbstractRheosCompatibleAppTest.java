package com.ebay.traffic.chocolate.flink.nrt.app;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class AbstractRheosCompatibleAppTest {

  private RheosCompatibleApp rheosCompatibleApp;
  private static class RheosCompatibleApp extends AbstractRheosCompatibleApp<String, String> {
    @Override
    protected DataStream<String> transform(DataStreamSource<String> dataStreamSource) {
      return dataStreamSource.map((MapFunction<String, String>) String::toUpperCase);
    }

    @Override
    protected SourceFunction<String> getKafkaConsumer() {
      try {
        return new CustomSource(StringSerializer.INSTANCE, "hello");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    protected SinkFunction<String> getKafkaProducer() {
      return new CustomSink();
    }

    @Override
    protected List<String> getConsumerTopics() {
      return Collections.singletonList("");
    }

    @Override
    protected Properties getConsumerProperties() {
      return new Properties();
    }

    @Override
    protected String getProducerTopic() {
      return "";
    }

    @Override
    protected Properties getProducerProperties() {
      return new Properties();
    }
  }

  @Before
  public void setUp() throws Exception {
    rheosCompatibleApp = new RheosCompatibleApp();
  }

  @Test
  public void run() throws Exception {
    CustomSink.values.clear();
    rheosCompatibleApp.run();
    Set<String> expected = new HashSet<>();
    expected.add("HELLO");
    assertEquals(expected, CustomSink.values);
  }

}