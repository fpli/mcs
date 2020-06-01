package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.*;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.RheosSerializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.function.ESMetricsCompatibleRichFlatMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.transformer.BaseTransformer;
import com.ebay.traffic.chocolate.flink.nrt.transformer.TransformerFactory;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ImkRvrTrckngEventTransformApp extends BaseRheosCompatibleApp<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, RheosEvent>> {

  public static void main(String[] args) throws Exception {
    ImkRvrTrckngEventTransformApp transformApp = new ImkRvrTrckngEventTransformApp();
    transformApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_CONSUMER_PROPERTIES);
  }

  @Override
  protected String getProducerTopic() {
    return StringConstants.EMPTY;
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(), new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected FlinkKafkaProducer<Tuple3<String, Long, RheosEvent>> getKafkaProducer() {
    return new FlinkKafkaProducer<>(getProducerTopic(), new RheosSerializationSchema(), getProducerProperties(), FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
  }

  @Override
  protected Properties getProducerProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_PRODUCER_PROPERTIES);
  }

  @Override
  protected DataStream<Tuple3<String, Long, RheosEvent>> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    return dataStreamSource.flatMap(new TransformRichFlatMapFunction());
  }

  private static class TransformRichFlatMapFunction extends ESMetricsCompatibleRichFlatMapFunction<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, RheosEvent>> {
    private Schema rheosHeaderSchema;
    private String rheosProducer;
    private String imkRvrTrckngEventMessageTopic;
    private String imkRvrTrckngEventDtlMessageTopic;
    private int imkRvrTrckngEventMessageSchemaId;
    private int imkRvrTrckngEventDtlMessageSchemaId;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      rheosHeaderSchema = RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();
      Properties topicProperties = PropertyMgr.getInstance().loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_PRODUCER_TOPIC_PROPERTIES);
      rheosProducer = topicProperties.getProperty(PropertyConstants.RHEOS_PRODUCER);
      imkRvrTrckngEventMessageTopic = topicProperties.getProperty(PropertyConstants.TOPIC_IMK_RVR_TRCKNG_EVENT);
      imkRvrTrckngEventDtlMessageTopic = topicProperties.getProperty(PropertyConstants.TOPIC_IMK_RVR_TRCKNG_EVENT_DTL);
      imkRvrTrckngEventMessageSchemaId = Integer.parseInt(topicProperties.getProperty(PropertyConstants.SCHEMA_ID_IMK_RVR_TRCKNG_EVENT));
      imkRvrTrckngEventDtlMessageSchemaId = Integer.parseInt(topicProperties.getProperty(PropertyConstants.SCHEMA_ID_IMK_RVR_TRCKNG_EVENT_DTL));
    }

    @Override
    public void flatMap(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<Tuple3<String, Long, RheosEvent>> collector) throws Exception {
      long currentTimeMillis = System.currentTimeMillis();

      FilterMessage filterMessage = FilterMessage.decodeRheos(rheosHeaderSchema, consumerRecord.value());
      BaseTransformer concreteTransformer = TransformerFactory.getConcreteTransformer(filterMessage);

      ImkRvrTrckngEventMessage imkRvrTrckngEventMessage = new ImkRvrTrckngEventMessage();
      imkRvrTrckngEventMessage.setRheosHeader(getRheosHeader(currentTimeMillis, imkRvrTrckngEventMessageSchemaId));
      concreteTransformer.transform(imkRvrTrckngEventMessage);

      ImkRvrTrckngEventDtlMessage imkRvrTrckngEventDtlMessage = new ImkRvrTrckngEventDtlMessage();
      imkRvrTrckngEventDtlMessage.setRheosHeader(getRheosHeader(currentTimeMillis, imkRvrTrckngEventDtlMessageSchemaId));
      concreteTransformer.transform(imkRvrTrckngEventDtlMessage);

      collector.collect(new Tuple3<>(imkRvrTrckngEventMessageTopic, imkRvrTrckngEventMessage.getRvrId(), getRheosEvent(imkRvrTrckngEventMessage)));
      collector.collect(new Tuple3<>(imkRvrTrckngEventDtlMessageTopic, imkRvrTrckngEventDtlMessage.getRvrId(), getRheosEvent(imkRvrTrckngEventDtlMessage)));
    }

    private RheosEvent getRheosEvent(GenericRecord v) {
      return new RheosEvent(v);
    }

    private RheosHeader getRheosHeader(long currentTimeMillis, int schemaId) {
      RheosHeader rheosHeader = new RheosHeader();
      rheosHeader.setEventCreateTimestamp(currentTimeMillis);
      rheosHeader.setEventSentTimestamp(currentTimeMillis);
      rheosHeader.setProducerId(rheosProducer);
      rheosHeader.setSchemaId(schemaId);
      return rheosHeader;
    }
  }

}
