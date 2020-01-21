package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.FlatMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.transformer.BaseTransformer;
import com.ebay.traffic.chocolate.flink.nrt.transformer.TransformerFactory;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class TransformApp extends BaseKafkaApp {
  private final static Schema RHEOS_HEADER_SCHEMA =
          RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();
  private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();

  public static void main(String[] args) throws Exception {
    TransformApp transformApp = new TransformApp();
    transformApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty("kafka.transform.consumer.rheos.topic.properties")
                    .getProperty("topic").split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance().loadProperty("kafka.transform.consumer.rheos.properties");
  }

  @Override
  protected String getProducerTopic() {
    return PropertyMgr.getInstance().loadProperty("kafka.transform.producer.topic.properties")
            .getProperty("topic");
  }

  @Override
  protected Properties getProducerProperties() {
    return PropertyMgr.getInstance().loadProperty("kafka.transform.producer.properties");
  }

  @Override
  protected DataStream<Tuple2<Long, byte[]>> transform(DataStreamSource<Tuple2<Long, byte[]>> dataStreamSource) {
    System.out.println(dataStreamSource);
    return dataStreamSource.map(new MapFunction<Tuple2<Long, byte[]>, Tuple2<Long, byte[]>>() {
      @Override
      public Tuple2<Long, byte[]> map(Tuple2<Long, byte[]> tuple) throws Exception {
        FilterMessage filterMessage = FilterMessage.decodeRheos(RHEOS_HEADER_SCHEMA, tuple.f1);
        BaseTransformer concreteTransformer = TransformerFactory.getConcreteTransformer(filterMessage);
        FlatMessage flatMessage = new FlatMessage();
        concreteTransformer.transform(flatMessage);
        byte[] value;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
          BinaryEncoder encoder = ENCODER_FACTORY.directBinaryEncoder(out, null);
          DatumWriter<FlatMessage> datumWriter = new SpecificDatumWriter<FlatMessage>(FlatMessage.class);
          datumWriter.write(flatMessage, encoder);
          encoder.flush();
          value = out.toByteArray();
        } catch (Exception e) {
          throw new SerializationException("Unable to serialize common message", e);
        }
        return new Tuple2<Long, byte[]>(flatMessage.getRvrId(), value);
      }
    });
  }
}
