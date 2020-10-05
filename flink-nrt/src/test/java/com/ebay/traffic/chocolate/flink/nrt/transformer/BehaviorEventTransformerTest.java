package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.BehaviorEvent;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroSerializerHelper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class BehaviorEventTransformerTest {
  private BehaviorEventTransformer behaviorEventTransformer;
  private GenericRecord sourceRecord;

  @Before
  public void setUp() throws Exception {
    String json = PropertyMgr.getInstance().loadFile("behavior-message.json");
    sourceRecord = createSourceRecord(json);
    behaviorEventTransformer = new BehaviorEventTransformer(sourceRecord);
  }

  private GenericRecord createSourceRecord(String json) throws IOException {
    Map<String, Object> map = new HashMap<>();
    map.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");

    SchemaRegistryAwareAvroSerializerHelper<GenericRecord> serializerHelper =
            new SchemaRegistryAwareAvroSerializerHelper<>(map, GenericRecord.class);

    Schema schema = serializerHelper.getSchema("marketing.tracking.behavior.schema");

    DecoderFactory decoderFactory = new DecoderFactory();
    Decoder decoder = decoderFactory.jsonDecoder(schema, json);
    DatumReader<GenericData.Record> reader =
            new GenericDatumReader<>(schema);
    return reader.read(null, decoder);
  }

  @Test
  public void transform() {
    BehaviorEvent behaviorEvent = new BehaviorEvent();
    behaviorEventTransformer.transform(behaviorEvent);
    assertEquals("", behaviorEvent.getGuid());
    assertEquals("acd12d5b79ac46b7b5eef0d76510cb1e", behaviorEvent.getAdguid());
    assertEquals(Long.valueOf(6694873385117159424L), behaviorEvent.getSessionskey());
    assertEquals(Long.valueOf(6694873385117159424L), behaviorEvent.getSnapshotid());
    assertEquals(Integer.valueOf(1), behaviorEvent.getSeqnum());
    assertEquals(Integer.valueOf(0), behaviorEvent.getSiteid());
    assertEquals(Integer.valueOf(3962), behaviorEvent.getPageid());
    assertEquals("impression", behaviorEvent.getPagename());
    assertNull(behaviorEvent.getRefererhash());
    assertEquals(Long.valueOf(1596182198056L), behaviorEvent.getEventtimestamp());
    assertEquals("/marketingtracking/v1/impression?mkevt=4&mkcid=7&mkpid=0&sojTags=bu=bu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623",
            behaviorEvent.getUrlquerystring());
    assertEquals("Script=marketingtracking&Agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit" +
                    "/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36,GingerClient/0.11.0-RELEASE&" +
                    "Server=mktcollectionsvc.vip.ebay.com&RemoteIP=10.249.75.9&ContentLength=211&TName=impression" +
                    "&AcceptEncoding=gzip, deflate, br&ForwardedFor=10.249.75.9&TMachine=10.249.75.54",
            behaviorEvent.getClientdata());
    assertEquals("ext=56623&" +
                    "bu=43551630917&euid=c527526a795a414cb4ad11bfaba21b5d&emsid=e11051.m44.l1139" +
                    "&emid=43551630917",
            behaviorEvent.getApplicationpayload());
    assertEquals("mktcollectionsvc.vip.ebay.com", behaviorEvent.getWebserver());
    assertNull(behaviorEvent.getReferrer());
    assertEquals("43551630917", behaviorEvent.getUserid());
    assertEquals(Integer.valueOf(0), behaviorEvent.getRdt());
    assertEquals("SITE_EMAIL", behaviorEvent.getChanneltype());
    assertEquals("EMAIL_OEPN", behaviorEvent.getChannelaction());
    assertEquals("", behaviorEvent.getDispatchid());
    assertEquals(1, behaviorEvent.getData().size());
    assertTrue(behaviorEvent.getData().iterator().next().isEmpty());
  }

  @Test
  public void getGuid() {
    assertEquals("", behaviorEventTransformer.getGuid());
  }

  @Test
  public void getAdguid() {
    assertEquals("acd12d5b79ac46b7b5eef0d76510cb1e", behaviorEventTransformer.getAdguid());
  }

  @Test
  public void getSessionskey() {
    assertEquals(Long.valueOf(6694873385117159424L), behaviorEventTransformer.getSessionskey());

    sourceRecord.put("snapshotId", new Utf8("snapshotId"));
    behaviorEventTransformer = new BehaviorEventTransformer(sourceRecord);
    assertNull(behaviorEventTransformer.getSessionskey());

    sourceRecord.put("snapshotId", null);
    behaviorEventTransformer = new BehaviorEventTransformer(sourceRecord);
    assertNull(behaviorEventTransformer.getSessionskey());
  }

  @Test
  public void getSnapshotid() {
    assertEquals(Long.valueOf(6694873385117159424L), behaviorEventTransformer.getSnapshotid());

    sourceRecord.put("snapshotId", new Utf8("snapshotId"));
    behaviorEventTransformer = new BehaviorEventTransformer(sourceRecord);
    assertNull(behaviorEventTransformer.getSnapshotid());

    sourceRecord.put("snapshotId", null);
    behaviorEventTransformer = new BehaviorEventTransformer(sourceRecord);
    assertNull(behaviorEventTransformer.getSnapshotid());
  }

  @Test
  public void getSeqnum() {
    assertEquals(Integer.valueOf(1), behaviorEventTransformer.getSeqnum());

    sourceRecord.put("seqNum", new Utf8("seqNum"));
    behaviorEventTransformer = new BehaviorEventTransformer(sourceRecord);
    assertNull(behaviorEventTransformer.getSeqnum());

    sourceRecord.put("seqNum", null);
    behaviorEventTransformer = new BehaviorEventTransformer(sourceRecord);
    assertNull(behaviorEventTransformer.getSeqnum());
  }

  @Test
  public void getSiteid() {
    assertEquals(Integer.valueOf(0), behaviorEventTransformer.getSiteid());

    sourceRecord.put("siteId", new Utf8("siteId"));
    behaviorEventTransformer = new BehaviorEventTransformer(sourceRecord);
    assertNull(behaviorEventTransformer.getSiteid());

    sourceRecord.put("siteId", null);
    behaviorEventTransformer = new BehaviorEventTransformer(sourceRecord);
    assertNull(behaviorEventTransformer.getSiteid());
  }

  @Test
  public void getPageid() {
    assertEquals(Integer.valueOf(3962), behaviorEventTransformer.getPageid());
  }

  @Test
  public void getPagename() {
    assertEquals("impression", behaviorEventTransformer.getPagename());
  }

  @Test
  public void getRefererhash() {
    assertNull(behaviorEventTransformer.getRefererhash());
  }

  @Test
  public void getEventtimestamp() {
    assertEquals(Long.valueOf(1596182198056L), behaviorEventTransformer.getEventtimestamp());
  }

  @Test
  public void getUrlquerystring() {
    assertEquals("/marketingtracking/v1/impression?mkevt=4&mkcid=7&mkpid=0&sojTags=bu=bu&bu=43551630917&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623",
            behaviorEventTransformer.getUrlquerystring());
  }

  @Test
  public void getClientdata() {
    assertEquals("Script=marketingtracking&Agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit" +
                    "/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36,GingerClient/0.11.0-RELEASE&" +
                    "Server=mktcollectionsvc.vip.ebay.com&RemoteIP=10.249.75.9&ContentLength=211&TName=impression" +
                    "&AcceptEncoding=gzip, deflate, br&ForwardedFor=10.249.75.9&TMachine=10.249.75.54",
            behaviorEventTransformer.getClientdata());
  }

  @Test
  public void getApplicationpayload() {
    assertEquals("ext=56623&" +
                    "bu=43551630917&euid=c527526a795a414cb4ad11bfaba21b5d&emsid=e11051.m44.l1139" +
                    "&emid=43551630917",
            behaviorEventTransformer.getApplicationpayload());
  }

  @Test
  public void getWebserver() {
    assertEquals("mktcollectionsvc.vip.ebay.com", behaviorEventTransformer.getWebserver());
  }

  @Test
  public void getReferrer() {
    assertNull(behaviorEventTransformer.getReferrer());
  }

  @Test
  public void getUserid() {
    assertEquals("43551630917", behaviorEventTransformer.getUserid());
  }

  @Test
  public void getRdt() {
    assertEquals(Integer.valueOf(0), behaviorEventTransformer.getRdt());
  }

  @Test
  public void getChanneltype() {
    assertEquals("SITE_EMAIL", behaviorEventTransformer.getChanneltype());
  }

  @Test
  public void getChannelaction() {
    assertEquals("EMAIL_OEPN", behaviorEventTransformer.getChannelaction());
  }

  @Test
  public void getDispatchid() {
    assertEquals("", behaviorEventTransformer.getDispatchid());
  }

  @Test
  public void getData() {
    assertEquals(1, behaviorEventTransformer.getData().size());
    assertTrue(behaviorEventTransformer.getData().iterator().next().isEmpty());
  }
}