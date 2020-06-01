package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.MetricConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.function.ESMetricsCompatibleRichMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.transformer.BaseTransformer;
import com.ebay.traffic.chocolate.flink.nrt.transformer.TransformerFactory;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.chocolate.flink.nrt.model.TrackingEvent;
import com.ebay.traffic.monitoring.ESMetrics;
import com.google.gson.Gson;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TrackingEventTransformApp extends BaseKafkaCompatibleApp {
  private final static Schema RHEOS_HEADER_SCHEMA =
          RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();
  private static final Gson GSON = new Gson();
  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

  public static void main(String[] args) throws Exception {
    TrackingEventTransformApp transformApp = new TrackingEventTransformApp();
    transformApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.KAFKA_TRACKING_EVENT_TRANSFORM_CONSUMER_RHEOS_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.KAFKA_TRACKING_EVENT_TRANSFORM_CONSUMER_RHEOS_PROPERTIES);
  }

  @Override
  protected String getProducerTopic() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.KAFKA_TRACKING_EVENT_TRANSFORM_PRODUCER_TOPIC_PROPERTIES)
            .getProperty(PropertyConstants.TOPIC);
  }

  @Override
  protected Properties getProducerProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.KAFKA_TRACKING_EVENT_TRANSFORM_PRODUCER_PROPERTIES);
  }

  @Override
  protected DataStream<Tuple2<Long, byte[]>> transform(DataStreamSource<Tuple2<Long, byte[]>> dataStreamSource) {
    return dataStreamSource.map(new TransformRichMapFunction());
  }

  private static class TransformRichMapFunction extends ESMetricsCompatibleRichMapFunction<Tuple2<Long, byte[]>, Tuple2<Long, byte[]>> {
    @Override
    public Tuple2<Long, byte[]> map(Tuple2<Long, byte[]> tuple) throws Exception {
      FilterMessage filterMessage = FilterMessage.decodeRheos(RHEOS_HEADER_SCHEMA, tuple.f1);
      BaseTransformer concreteTransformer = TransformerFactory.getConcreteTransformer(filterMessage);
      TrackingEvent trackingEvent = new TrackingEvent();
      concreteTransformer.transform(trackingEvent);
      return new Tuple2<Long, byte[]>(trackingEvent.getRvrId(), GSON.toJson(trackingEvent).getBytes(StandardCharsets.UTF_8));
    }
  }
}
