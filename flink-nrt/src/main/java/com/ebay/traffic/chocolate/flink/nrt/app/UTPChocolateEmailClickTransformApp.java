package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.RheosConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.kafka.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.transformer.UTPChocolateEmailClickTransformer;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.chocolate.utp.common.model.rheos.UnifiedTrackingRheosMessage;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Receive messages from pulsar topic, and send chocolate email clicks to unified tracking topic.
 *
 * @author Zhiyuan Wang
 * @since 2021/11/28
 */
public class UTPChocolateEmailClickTransformApp
        extends AbstractRheosCompatibleApp<ConsumerRecord<byte[], byte[]>, Tuple3<String, String, byte[]>> {

  public static void main(String[] args) throws Exception {
    UTPChocolateEmailClickTransformApp transformApp = new UTPChocolateEmailClickTransformApp();
    transformApp.run();
  }

  @Override
  protected void loadProperty() {
    this.env_config = PropertyMgr.getInstance().loadYaml("utp-chocolate-email-click-transform-app.yaml");
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<String> getConsumerTopics() {
    Map<String, Object> source = (Map<String, Object>) env_config.get(PropertyConstants.SOURCE);
    String topics = (String) source.get(PropertyConstants.TOPIC);
    return Arrays.asList(topics.split(StringConstants.COMMA));
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Properties getConsumerProperties() throws IOException {
    Map<String, Object> source = (Map<String, Object>) env_config.get(PropertyConstants.SOURCE);
    Properties consumerProperties = new Properties();
    consumerProperties.load(new StringReader((String) source.get(PropertyConstants.PRORERTIES)));
    return consumerProperties;
  }

  @Override
  protected String getProducerTopic() {
    return StringConstants.EMPTY;
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() throws IOException {
    return new FlinkKafkaConsumer<>(getConsumerTopics(),
            new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected FlinkKafkaProducer<Tuple3<String, String, byte[]>> getKafkaProducer() throws IOException {
    return new FlinkKafkaProducer<>(getProducerTopic(), new CustomKafkaSerializationSchema(),
            getProducerProperties(), FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected Properties getProducerProperties() throws IOException {
    Map<String, Object> sink = (Map<String, Object>) env_config.get("sink");
    Properties producerProperties = new Properties();
    producerProperties.load(new StringReader((String) sink.get(PropertyConstants.PRORERTIES)));
    return producerProperties;
  }

  @Override
  protected DataStream<Tuple3<String, String, byte[]>> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    return dataStreamSource.flatMap(new TransformFlatMapFunction()).name("transform").uid("transform");
  }

  protected static class TransformFlatMapFunction extends RichFlatMapFunction<ConsumerRecord<byte[], byte[]>, Tuple3<String, String, byte[]>> {
    private transient GenericRecordDomainDataDecoder decoder;
    private transient RheosEventDeserializer deserializer;
    private transient EncoderFactory encoderFactory;
    private int schemaId;
    private transient Schema schema;
    private String producer;
    private String topic;

    private transient Meter numInvalidDateInRate;
    private transient Meter numChocolateSiteEmailClickInRate;
    private transient Meter numChocolateSiteMCClickInRate;
    private transient Meter numChocolateMrktEmailClickInRate;
    private transient Meter numChocolateMrktMCClickInRate;
    private transient Meter numNoPageIdInRate;
    private transient Meter numNoChnlInRate;
    private transient Meter numNotChocolateClickInRate;

    @SuppressWarnings("unchecked")
    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      deserializer = new RheosEventDeserializer();
      Map<String, Object> config = new HashMap<>();

      Map<String, Object> envConfig = PropertyMgr.getInstance().loadYaml("utp-chocolate-email-click-transform-app.yaml");
      Map<String, Object> source = (Map<String, Object>) envConfig.get("source");
      Properties consumerProperties = new Properties();
      consumerProperties.load(new StringReader((String) source.get(PropertyConstants.PRORERTIES)));

      String rheosServiceUrl = consumerProperties.getProperty(StreamConnectorConfig.RHEOS_SERVICES_URLS);
      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
      decoder = new GenericRecordDomainDataDecoder(config);
      encoderFactory = EncoderFactory.get();

      Map<String, Object> sink = (Map<String, Object>) envConfig.get("sink");
      Properties producerProperties = new Properties();
      producerProperties.load(new StringReader((String) sink.get(PropertyConstants.PRORERTIES)));

      config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, rheosServiceUrl);
      SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
              new SchemaRegistryAwareAvroSerializerHelper<>(config, GenericRecord.class);
      String schemaName = (String) producerProperties.get(RheosConstants.RHEOS_TOPIC_SCHEMA);
      schemaId = serializerHelper.getSchemaId(schemaName);
      schema = serializerHelper.getSchema(schemaName);
      producer = (String) producerProperties.get(RheosConstants.RHEOS_PRODUCER);

      topic = (String) sink.get(PropertyConstants.TOPIC);

      numInvalidDateInRate = getRuntimeContext().getMetricGroup().meter("numInvalidDateInRate", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));

      numChocolateSiteEmailClickInRate = getRuntimeContext().getMetricGroup().meter("numChocolateSiteEmailClickInRate", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
      numChocolateSiteMCClickInRate = getRuntimeContext().getMetricGroup().meter("numChocolateSiteMCClickInRate", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
      numChocolateMrktEmailClickInRate = getRuntimeContext().getMetricGroup().meter("numChocolateMrktEmailClickInRate", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
      numChocolateMrktMCClickInRate = getRuntimeContext().getMetricGroup().meter("numChocolateMrktMCClickInRate", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));

      numNoPageIdInRate = getRuntimeContext().getMetricGroup().meter("numNoPageIdInRate", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
      numNoChnlInRate = getRuntimeContext().getMetricGroup().meter("numNoChnlInRate", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
      numNotChocolateClickInRate = getRuntimeContext().getMetricGroup().meter("numNotChocolateClickInRate", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    }

    @Override
    public void flatMap(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<Tuple3<String, String, byte[]>> out) throws Exception {
      Headers headers = consumerRecord.headers();
      String schemaVersion = StringConstants.EMPTY;
      if (headers != null) {
        for (Header header : headers) {
          if ("schemaVersion".equals(header.key())) {
            schemaVersion = new String(header.value());
          }
        }
      }
      String consumerTopic = consumerRecord.topic();
      RheosEvent sourceRheosEvent = deserializer.deserialize(consumerTopic, consumerRecord.value());

      GenericRecord sourceRecord = decoder.decode(sourceRheosEvent);
      UTPChocolateEmailClickTransformer transformer = new UTPChocolateEmailClickTransformer(consumerRecord.topic(),
              consumerRecord.partition(), consumerRecord.offset(), sourceRecord, sourceRheosEvent, schemaVersion,
              numInvalidDateInRate, numNoPageIdInRate, numNoChnlInRate, numNotChocolateClickInRate);
      if (!transformer.isValid()) {
        return;
      }

      UnifiedTrackingRheosMessage message = new UnifiedTrackingRheosMessage();
      transformer.transform(message);

      switch (message.getChannelType()) {
        case "SITE_EMAIL":
          numChocolateSiteEmailClickInRate.markEvent();
          break;
        case "MRKT_EMAIL":
          numChocolateMrktEmailClickInRate.markEvent();
          break;
        case "SITE_MESSAGE_CENTER":
          numChocolateSiteMCClickInRate.markEvent();
          break;
        case "MRKT_MESSAGE_CENTER":
          numChocolateMrktMCClickInRate.markEvent();
          break;
        default:
          throw new IllegalArgumentException(String.format("Unknown channel of %s", message));
      }

      RheosEvent rheosEvent = getRheosEvent(message);

      out.collect(new Tuple3<>(this.topic, message.getEventId(), serializeRheosEvent(rheosEvent)));
    }

    public RheosEvent getRheosEvent(GenericRecord v) {
      RheosEvent rheosEvent = new RheosEvent(schema);
      long t = System.currentTimeMillis();
      rheosEvent.setEventCreateTimestamp(t);
      rheosEvent.setEventSentTimestamp(t);
      rheosEvent.setSchemaId(schemaId);
      rheosEvent.setProducerId(producer);

      for (Schema.Field field : v.getSchema().getFields()) {
        String fn = field.name();
        Object fv = v.get(fn);
        if (fv != null) {
          rheosEvent.put(fn, fv);
        }
      }
      return rheosEvent;
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

  private static class CustomKafkaSerializationSchema implements KafkaSerializationSchema<Tuple3<String, String, byte[]>> {

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple3<String, String, byte[]> element, @Nullable Long timestamp) {
      return new ProducerRecord<>(element.f0, element.f1.getBytes(StandardCharsets.UTF_8), element.f2);
    }

  }

}
