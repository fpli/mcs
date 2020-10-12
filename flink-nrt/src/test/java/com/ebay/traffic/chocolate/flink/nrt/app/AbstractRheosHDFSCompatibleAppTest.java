package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class AbstractRheosHDFSCompatibleAppTest {
  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static File tempdir;
  private static File file;
  private static Path path;
  private static LocalFileSystem lfs;
  private static Schema schema;
  private RheosHDFSCompatibleApp rheosHDFSCompatibleApp;

  private static final String SCHEMA_STRING = "{\"type\":\"record\","
          + "\"name\":\"myrecord\","
          + "\"fields\":"
          + "[{\"type\":\"string\",\"name\":\"id\"},"
          + " {\"type\":\"string\",\"name\":\"event_dt\", \"default\": \"2020-01-01\"}]}";

  private static class RheosHDFSCompatibleApp extends AbstractRheosHDFSCompatibleApp<String, GenericRecord> {
    @Override
    protected DataStream<GenericRecord> transform(DataStreamSource<String> dataStreamSource) {
      return dataStreamSource.map((MapFunction<String, GenericRecord>) value -> {
        String[] split = value.split(",");
        GenericData.Record record = new GenericData.Record(schema);
        record.put("id", split[0]);
        record.put("event_dt", split[1]);
        return record;
      });
    }

    @Override
    protected SourceFunction<String> getKafkaConsumer() {
      try {
        return new CustomSource(StringSerializer.INSTANCE, "1,2020-02-01", "2,2020-03-01");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
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
    protected Path getSinkBasePath() {
      return path;
    }

    @Override
    protected BulkWriter.Factory<GenericRecord> getSinkWriterFactory() {
      return ParquetAvroWriters.forGenericRecord(schema);
    }

    @Override
    protected BucketAssigner<GenericRecord, String> getSinkBucketAssigner() {
      return new CustomEventDateTimeBucketAssigner();
    }

    protected static class CustomEventDateTimeBucketAssigner implements BucketAssigner<GenericRecord, String> {

      @Override
      public String getBucketId(GenericRecord element, Context context) {
        return StringConstants.BUCKET_PREFIX + element.get("event_dt");
      }

      @Override
      public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    tempdir = new File(TEMPORARY_FOLDER.getRoot(), UUID.randomUUID().toString());
    file = new File(tempdir, UUID.randomUUID().toString());
    path = new Path(file.toURI().getPath());
    lfs = new LocalFileSystem();
    schema = new Schema.Parser().parse(SCHEMA_STRING);
    rheosHDFSCompatibleApp = new RheosHDFSCompatibleApp();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void run() throws Exception {
    rheosHDFSCompatibleApp.run();
    assertTrue(lfs.exists(new Path(file.toURI().getPath() + "/dt=2020-02-01")));
    assertTrue(lfs.exists(new Path(file.toURI().getPath() + "/dt=2020-03-01")));
  }
}