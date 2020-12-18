package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.versions.UnifiedTrackingRheosMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum;
import com.ebay.traffic.chocolate.utp.common.EmailPartnerIdEnum;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;

public class UTPRoverEventTransformerTest {
  private static final String MRKT_EMAIL_URL_QUERY_STRING = "/roveropen/4/0/8?osub=b104444a19d75f58b856404c3b16d970%7ETE75001_T_AGM_CT1&crd=20201205091000&sojTags=emid%3Dbu%2Cut%3Dut%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2Cosub%3Dosub&ch=osgood&segname=TE75001_T_AGM_CT1&bu=43212588313";
  private static final String SITE_EMAIL_URL_QUERY_STRING = "/roveropen/4/0/7?osub=b104444a19d75f58b856404c3b16d970%7ETE75001_T_AGM_CT1&crd=20201205091000&sojTags=emid%3Dbu%2Cut%3Dut%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2Cosub%3Dosub&ch=osgood&segname=TE75001_T_AGM_CT1&bu=43212588313";

  private static Schema schema;
  private GenericRecord genericRecord;
  private RheosEvent rheosEvent;
  private UTPRoverEventTransformer transformer;
  private Map<Utf8, Utf8> applicationMap;

  @BeforeClass
  public static void beforeClass() throws Exception {
    String json = PropertyMgr.getInstance().loadFile("behavior.pulsar.sojevent.schema-undefined-384.avsc");
    schema = new Schema.Parser().parse(json);
  }

  @Before
  public void setUp() throws Exception {
    Properties properties = PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);
    SherlockioMetrics.init(properties.getProperty(PropertyConstants.SHERLOCKIO_NAMESPACE),
            properties.getProperty(PropertyConstants.SHERLOCKIO_ENDPOINT),
            properties.getProperty(PropertyConstants.SHERLOCKIO_USER));
    applicationMap = new HashMap<>();
    Map<Utf8, Utf8> clientData = new HashMap<>();
    genericRecord = new GenericData.Record(schema);
    rheosEvent = new RheosEvent(schema);
    genericRecord.put("pageId", 3962);
    genericRecord.put("pageName", new Utf8("roveropen"));
    genericRecord.put("applicationPayload", applicationMap);
    genericRecord.put("clientData", clientData);
    genericRecord.put("urlQueryString", new Utf8(MRKT_EMAIL_URL_QUERY_STRING));
    genericRecord.put("eventTimestamp", 123456789L);
    genericRecord.put("guid", new Utf8("test-guid"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
  }

  @Test
  public void isValid() {
    assertTrue(transformer.isValid());
  }

  @Test
  public void transform() {
    UnifiedTrackingRheosMessage message = new UnifiedTrackingRheosMessage();
    transformer.transform(message);
    assertNotNull(message.getEventId());
  }

  @Test
  public void getField() {
  }

  @Test
  public void getEventId() {
    assertNotNull(transformer.getEventId());
  }

  @Test
  public void getProducerEventId() {
    assertEquals("", transformer.getProducerEventId());

    applicationMap.put(new Utf8("euid"), new Utf8("1234546"));
    genericRecord.put("urlQueryString", new Utf8(MRKT_EMAIL_URL_QUERY_STRING));
    genericRecord.put("applicationPayload", applicationMap);
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("", transformer.getProducerEventId());

    applicationMap.put(new Utf8("euid"), new Utf8("1234546"));
    genericRecord.put("urlQueryString", new Utf8("/roveropen/4/0/7?osub=b104444a19d75f58b856404c3b16d970%7ETE75001_T_AGM_CT1&crd=20201205091000&sojTags=emid%3Dbu%2Cut%3Dut%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2Cosub%3Dosub&ch=osgood&segname=TE75001_T_AGM_CT1&bu=43212588313"));
    genericRecord.put("applicationPayload", applicationMap);
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("1234546", transformer.getProducerEventId());
  }

  @Test
  public void getRlogId() {
    assertEquals("", transformer.getRlogId());
    genericRecord.put("rlogid", new Utf8("123456"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("123456", transformer.getRlogId());
  }

  @Test
  public void getTrackingId() {
    assertEquals("", transformer.getTrackingId());
  }

  @Test
  public void getUserId() {
    assertEquals(0L, transformer.getUserId());

    genericRecord.put("userId", new Utf8("userId"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals(0L, transformer.getUserId());

    genericRecord.put("userId", new Utf8("123"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals(123L, transformer.getUserId());
  }

  @Test
  public void getPublicUserId() {
    assertEquals("", transformer.getPublicUserId());

    applicationMap.put(new Utf8("adcamppu"), new Utf8("1234546"));
    genericRecord.put("applicationPayload", applicationMap);
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("1234546", transformer.getPublicUserId());
  }

  @Test
  public void getEncryptedUserId() {
    assertEquals(43212588313L, transformer.getEncryptedUserId());
  }

  @Test
  public void getGuid() {
    assertEquals("test-guid", transformer.getGuid());
  }

  @Test
  public void getIdfa() {
    applicationMap.put(new Utf8("idfa"), new Utf8("1234546"));
    genericRecord.put("applicationPayload", applicationMap);
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("1234546", transformer.getIdfa());
  }

  @Test
  public void getGadid() {
    applicationMap.put(new Utf8("gadid"), new Utf8("1234546"));
    genericRecord.put("applicationPayload", applicationMap);
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("1234546", transformer.getGadid());
  }

  @Test
  public void getDeviceId() {
    applicationMap.put(new Utf8("udid"), new Utf8("1234546"));
    genericRecord.put("applicationPayload", applicationMap);
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("1234546", transformer.getDeviceId());
  }

  @Test
  public void getChannelType() {
    assertEquals(ChannelTypeEnum.MRKT_EMAIL.getValue(), transformer.getChannelType());
  }

  @Test
  public void getActionType() {
    assertEquals(ActionTypeEnum.OPEN.getValue(), transformer.getActionType());
  }

  @Test
  public void getPartner() {
    assertEquals(EmailPartnerIdEnum.parse("4"), transformer.getPartner());
  }

  @Test
  public void getCampaignId() {
    assertEquals("", transformer.getCampaignId());

    applicationMap.put(new Utf8("segname"), new Utf8("123456"));
    genericRecord.put("applicationPayload", applicationMap);
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("123456", transformer.getCampaignId());

    applicationMap.put(new Utf8("emsid"), new Utf8("e11051.m44.l1139"));
    genericRecord.put("applicationPayload", applicationMap);
    genericRecord.put("urlQueryString", new Utf8(SITE_EMAIL_URL_QUERY_STRING));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("11051", transformer.getCampaignId());

    applicationMap.put(new Utf8("sid"), new Utf8("e11051.m44.l1139"));
    genericRecord.put("applicationPayload", applicationMap);
    genericRecord.put("urlQueryString", new Utf8(SITE_EMAIL_URL_QUERY_STRING));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("11051", transformer.getCampaignId());
  }

  @Test
  public void getRotationId() {
    assertEquals("", transformer.getRotationId());
  }

  @Test
  public void getSiteId() {
    assertEquals(0, transformer.getSiteId());

    genericRecord.put("siteId", new Utf8("siteId"));
    assertEquals(0, transformer.getSiteId());

    genericRecord.put("siteId", new Utf8("2"));
    assertEquals(2, transformer.getSiteId());
  }

  @Test
  public void getUrl() {
    assertEquals("https://rover.ebay.com" + MRKT_EMAIL_URL_QUERY_STRING, transformer.getUrl());
  }

  @Test
  public void getReferer() {
  }

  @Test
  public void getUserAgent() {
  }

  @Test
  public void getDeviceFamily() {
    genericRecord.put("deviceFamily", new Utf8("test"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("test", transformer.getDeviceFamily());
  }

  @Test
  public void getDeviceType() {
    genericRecord.put("deviceType", new Utf8("test"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("test", transformer.getDeviceType());
  }

  @Test
  public void getBrowserVersion() {
    genericRecord.put("browserVersion", new Utf8("test"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("test", transformer.getBrowserVersion());
  }

  @Test
  public void getBrowserFamily() {
    genericRecord.put("browserFamily", new Utf8("test"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("test", transformer.getBrowserFamily());
  }

  @Test
  public void getOsFamily() {
    genericRecord.put("osFamily", new Utf8("test"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("test", transformer.getOsFamily());
  }

  @Test
  public void getOsVersion() {
    genericRecord.put("enrichedOsVersion", new Utf8("test"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("test", transformer.getOsVersion());
  }

  @Test
  public void getAppId() {
    genericRecord.put("appId", new Utf8("test"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("test", transformer.getAppId());
  }

  @Test
  public void getAppVersion() {
    genericRecord.put("appVersion", new Utf8("test"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("test", transformer.getAppVersion());
  }

  @Test
  public void getService() {
    assertEquals("ROVER", transformer.getService());
  }

  @Test
  public void getServer() {
    genericRecord.put("webServer", new Utf8("rover.ebay.com"));
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals("rover.ebay.com", transformer.getServer());
  }

  @Test
  public void getRemoteIp() {
    assertEquals("", transformer.getRemoteIp());
  }

  @Test
  public void getPageId() {
    assertEquals(3962, transformer.getPageId());
  }

  @Test
  public void getGeoId() {
    assertEquals(0, transformer.getGeoId());

    applicationMap.put(new Utf8("uc"), new Utf8("uc"));
    genericRecord.put("applicationPayload", applicationMap);
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals(0, transformer.getGeoId());

    applicationMap.put(new Utf8("uc"), new Utf8("1"));
    genericRecord.put("applicationPayload", applicationMap);
    transformer = new UTPRoverEventTransformer("", 0L, genericRecord, rheosEvent);
    assertEquals(1, transformer.getGeoId());
  }

  @Test
  public void getIsBot() {
    assertFalse(transformer.getIsBot());
    transformer = new UTPRoverEventTransformer("behavior.pulsar.misc.bot", 0L, genericRecord, rheosEvent);
    assertTrue(transformer.getIsBot());
  }

  @Test
  public void getPayload() {
  }
}