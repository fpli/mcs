package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.ImkRvrTrckngEventDtlMessage;
import com.ebay.app.raptor.chocolate.avro.ImkRvrTrckngEventMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.DateConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import com.ebay.traffic.chocolate.flink.nrt.deserialization.DefaultKafkaDeserializationSchema;
import com.ebay.traffic.chocolate.flink.nrt.function.ESMetricsCompatibleRichFlatMapFunction;
import com.ebay.traffic.chocolate.flink.nrt.function.EventDateTimeBucketAssigner;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ImkRvrTrckngEventSinkApp extends BaseRheosHDFSCompatibleApp<ConsumerRecord<byte[], byte[]>, GenericRecord> {

  private static final OutputTag<ConsumerRecord<byte[], byte[]>> imk =
          new OutputTag<ConsumerRecord<byte[], byte[]>>("marketing.tracking.ssl.imk-rvr-trckng-event-transform") {
  };
  private static final OutputTag<ConsumerRecord<byte[], byte[]>> dtl =
          new OutputTag<ConsumerRecord<byte[], byte[]>>("marketing.tracking.ssl.imk-rvr-trckng-event-dtl-transform") {
  };

  public static void main(String[] args) throws Exception {
    ImkRvrTrckngEventSinkApp transformApp = new ImkRvrTrckngEventSinkApp();
    transformApp.run();
  }

  @Override
  protected List<String> getConsumerTopics() {
    return  Arrays.asList(PropertyMgr.getInstance()
                    .loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_SINK_APP_RHEOS_CONSUMER_TOPIC_PROPERTIES)
                    .getProperty(PropertyConstants.TOPIC).split(StringConstants.COMMA));
  }

  @Override
  protected Properties getConsumerProperties() {
    return PropertyMgr.getInstance().loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_SINK_APP_RHEOS_CONSUMER_PROPERTIES);
  }

  @Override
  protected FlinkKafkaConsumer<ConsumerRecord<byte[], byte[]>> getKafkaConsumer() {
    return new FlinkKafkaConsumer<>(getConsumerTopics(), new DefaultKafkaDeserializationSchema(), getConsumerProperties());
  }

  @Override
  protected Path getSinkBasePath() {
    return Path.fromLocalFile(new File(PropertyConstants.KAFKA_FILTER_SINK_PATH));
  }

  @Override
  protected BulkWriter.Factory<GenericRecord> getSinkWriterFactory() {
    return ParquetAvroWriters.forGenericRecord(FilterMessage.getClassSchema());
  }

  @Override
  protected DataStream<GenericRecord> transform(DataStreamSource<ConsumerRecord<byte[], byte[]>> dataStreamSource) {
    dataStreamSource.process(new ProcessFunction<ConsumerRecord<byte[], byte[]>, GenericRecord>() {
      @Override
      public void processElement(ConsumerRecord<byte[], byte[]> value, Context ctx, Collector<GenericRecord> out) throws Exception {
        String topic = value.topic();
        if ("marketing.tracking.ssl.imk-rvr-trckng-event-transform".equals(topic)) {
          ctx.output(imk, value);
        } else if ("marketing.tracking.ssl.imk-rvr-trckng-event-dtl-transform".equals(topic)) {
          ctx.output(dtl, value);
        }
      }
    });

    dataStreamSource.getSideOutput(imk).print();

    dataStreamSource.getSideOutput(imk).print();

    return dataStreamSource.flatMap(new TransformRichMapFunction());
  }

  private static class TransformRichMapFunction extends ESMetricsCompatibleRichFlatMapFunction<ConsumerRecord<byte[], byte[]>, GenericRecord> {
    private Schema rheosHeaderSchema;
    private DatumReader<ImkRvrTrckngEventMessage> imkReader = new SpecificDatumReader<>(
            ImkRvrTrckngEventMessage.getClassSchema());
    private DatumReader<ImkRvrTrckngEventDtlMessage> dtlReader = new SpecificDatumReader<>(
            ImkRvrTrckngEventDtlMessage.getClassSchema());

    @Override
    public void open(Configuration parameters) throws Exception {
      super.open(parameters);
      rheosHeaderSchema = RheosEvent.BASE_SCHEMA.getField(RheosEvent.RHEOS_HEADER).schema();
    }

    @Override
    public void flatMap(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<GenericRecord> collector) throws Exception {
      byte[] value = consumerRecord.value();
      String topic = consumerRecord.topic();
      if ("marketing.tracking.ssl.imk-rvr-trckng-event-transform".equals(topic)) {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(value, null);
        ImkRvrTrckngEventMessage datum = new ImkRvrTrckngEventMessage();
        try {
//          datum = imkReader.read(datum, decoder);
//          GenericData.Record record = new GenericData.Record(datum.getSchema());
//          return record;
        } catch (Exception e) {
          // Nothing to do, need to try the upgrading reader first
        }

      } else if ("marketing.tracking.ssl.imk-rvr-trckng-event-dtl-transform".equals(topic)) {

      }
      System.out.println("");
    }
  }

  @Override
  protected BucketAssigner<GenericRecord, String> getSinkBucketAssigner() {
    return new CustomEventDateTimeBucketAssigner(DateConstants.YYYY_MM_DD, DateConstants.YYYY_MM_DD_HH_MM_SS_SSS);
  }

  private static class CustomEventDateTimeBucketAssigner extends EventDateTimeBucketAssigner<GenericRecord> {
    public CustomEventDateTimeBucketAssigner(String formatString, String eventTsFormatString) {
      super(formatString, eventTsFormatString);
    }

    @Override
    public String getBucketId(GenericRecord element, Context context) {
      if (dateTimeFormatter == null) {
        dateTimeFormatter = DateTimeFormatter.ofPattern(formatString).withZone(zoneId);
      }
      if (eventTsFormatter == null) {
        eventTsFormatter = DateTimeFormatter.ofPattern(eventTsFormatString).withZone(zoneId);
      }

      return dateTimeFormatter.format(Instant.ofEpochMilli((Long) element.get(TransformerConstants.TIMESTAMP)));
    }
  }
}
