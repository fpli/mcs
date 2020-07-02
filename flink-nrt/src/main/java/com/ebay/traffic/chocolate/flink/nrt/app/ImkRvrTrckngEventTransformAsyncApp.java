package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.*;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV4;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.DefaultKafkaSerializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.function.ESMetricsCompatibleRichFlatMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.function.ESMetricsCompatibleRichMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.provider.AsyncDataRequest;
import com.ebay.traffic.chocolate.flink.nrt.transformer.BaseTransformer;
import com.ebay.traffic.chocolate.flink.nrt.transformer.TransformerFactory;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Receive messages from rheos topics, apply IMK ETL and send messages to another topics.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 *
 */
public class ImkRvrTrckngEventTransformAsyncApp
    extends AbstractRheosCompatibleApp<ConsumerRecord<byte[], byte[]>, Tuple3<String, Long, byte[]>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ImkRvrTrckngEventTransformAsyncApp.class);

  /**
   * App entrance
   * @param args input args
   */
  public static void main(String[] args) {
    ImkRvrTrckngEventTransformAsyncApp transformApp = new ImkRvrTrckngEventTransformAsyncApp();
    try {
      transformApp.run();
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      System.exit(1);
    }
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance()
        .loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_CONSUMER_PROPERTIES);
  }

  @Override
  protected String getProducerTopic() {
    return StringConstants.EMPTY;
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(),
        new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected FlinkKafkaProducer<Tuple3<String, Long, byte[]>> getKafkaProducer() {
    return new FlinkKafkaProducer<>(getProducerTopic(), new DefaultKafkaSerializationSchema(),
        getProducerProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
  }

  @Override
  protected Properties getProducerProperties() {
    return PropertyMgr.getInstance()
        .loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_PRODUCER_PROPERTIES);
  }

  @Override
  protected DataStream<Tuple3<String, Long, byte[]>> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    SingleOutputStreamOperator<FilterMessageV4> map = dataStreamSource.map(new TransformRichMapFunction());
    SingleOutputStreamOperator<FilterMessageV4> filter = map.filter(new FilterEbaySites());
    DataStream<FilterMessageV4> resultStream =
            AsyncDataStream.unorderedWait(filter, new AsyncDataRequest(), 10000, TimeUnit.MILLISECONDS, 100);
    return resultStream.flatMap(new TransformRichFlatMapFunction());
  }

  private static class TransformRichMapFunction
          extends ESMetricsCompatibleRichMapFunction<ConsumerRecord<byte[], byte[]>, FilterMessageV4> {
    private Schema rheosHeaderSchema;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      rheosHeaderSchema = RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();
    }

    @Override
    public FilterMessageV4 map(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
      return FilterMessage.decodeRheos(rheosHeaderSchema, consumerRecord.value());
    }
  }

  private static class TransformRichFlatMapFunction
      extends ESMetricsCompatibleRichFlatMapFunction<FilterMessageV4, Tuple3<String, Long, byte[]>> {
    private String rheosProducer;
    private String imkEventMessageTopic;
    private String imkEventDtlMessageTopic;
    private int imkEventMessageSchemaId;
    private int imkEventDtlMessageSchemaId;
    private transient EncoderFactory encoderFactory;

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      encoderFactory = EncoderFactory.get();
      Properties topicProperties = PropertyMgr.getInstance()
          .loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_TRANSFORM_APP_RHEOS_PRODUCER_TOPIC_PROPERTIES);
      rheosProducer = topicProperties.getProperty(PropertyConstants.RHEOS_PRODUCER);
      imkEventMessageTopic = topicProperties.getProperty(PropertyConstants.TOPIC_IMK_RVR_TRCKNG_EVENT);
      imkEventDtlMessageTopic = topicProperties.getProperty(PropertyConstants.TOPIC_IMK_RVR_TRCKNG_EVENT_DTL);
      imkEventMessageSchemaId = Integer
          .parseInt(topicProperties.getProperty(PropertyConstants.SCHEMA_ID_IMK_RVR_TRCKNG_EVENT));
      imkEventDtlMessageSchemaId = Integer
          .parseInt(topicProperties.getProperty(PropertyConstants.SCHEMA_ID_IMK_RVR_TRCKNG_EVENT_DTL));
    }

    @Override
    public void flatMap(FilterMessageV4 filterMessage,
                        Collector<Tuple3<String, Long, byte[]>> collector) throws Exception {
      long currentTimeMillis = System.currentTimeMillis();

      BaseTransformer concreteTransformer = TransformerFactory.getConcreteTransformer(filterMessage);

      ImkRvrTrckngEventMessage imkRvrTrckngEventMessage = new ImkRvrTrckngEventMessage();
      imkRvrTrckngEventMessage.setRheosHeader(getRheosHeader(currentTimeMillis, imkEventMessageSchemaId));
      concreteTransformer.transform(imkRvrTrckngEventMessage);

      ImkRvrTrckngEventDtlMessage imkRvrTrckngEventDtlMessage = new ImkRvrTrckngEventDtlMessage();
      imkRvrTrckngEventDtlMessage.setRheosHeader(getRheosHeader(currentTimeMillis, imkEventDtlMessageSchemaId));
      concreteTransformer.transform(imkRvrTrckngEventDtlMessage);

      collector.collect(new Tuple3<>(imkEventMessageTopic, imkRvrTrckngEventMessage.getRvrId(),
          serializeRheosEvent(getRheosEvent(imkRvrTrckngEventMessage))));
      collector.collect(new Tuple3<>(imkEventDtlMessageTopic, imkRvrTrckngEventDtlMessage.getRvrId(),
          serializeRheosEvent(getRheosEvent(imkRvrTrckngEventDtlMessage))));
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

    private byte[] serializeRheosEvent(RheosEvent data) {
      try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
        DatumWriter<GenericRecord> writer = getWriter(data);
        writer.write(data, encoder);
        encoder.flush();
        return out.toByteArray();
      } catch (Exception e) {
        throw new SerializationException(e);
      }
    }

    private DatumWriter<GenericRecord> getWriter(RheosEvent rheosEvent) {
      return new GenericDatumWriter<>(rheosEvent.getSchema());
    }
  }

  private static class FilterEbaySites extends RichFilterFunction<FilterMessageV4> {
    private transient Counter counter;
    private transient Pattern ebaySites;


    @Override
    public void open(Configuration config) {
      this.counter = getRuntimeContext()
          .getMetricGroup()
          .addGroup("test_label","1")
          .counter("ebaySitesReferer");
      this.ebaySites = Pattern.compile("^(http[s]?:\\/\\/)?([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)", Pattern.CASE_INSENSITIVE);
    }

    @Override
    public boolean filter(FilterMessageV4 value) throws Exception {
      if (value.getChannelType() == ChannelType.ROI) {
        return true;
      }
      if (ebaySites.matcher(value.getReferer()).find()) {
        this.counter.inc();
        return false;
      }
      return true;
    }
  }
}
