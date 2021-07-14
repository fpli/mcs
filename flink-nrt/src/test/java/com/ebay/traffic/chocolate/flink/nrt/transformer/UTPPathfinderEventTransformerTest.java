package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum;
import com.ebay.traffic.chocolate.utp.common.EmailPartnerIdEnum;
import com.ebay.traffic.chocolate.utp.common.model.rheos.UnifiedTrackingRheosMessage;
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
import static org.junit.Assert.assertTrue;

public class UTPPathfinderEventTransformerTest {
    private static final String URL_QUERY_STRING = "/base/tracking/v1/batchtrack";

    private static Schema schema;
    private GenericRecord genericRecord;
    private RheosEvent rheosEvent;
    private UTPPathfinderEventTransformer transformer;
    private Map<Utf8, Utf8> applicationMap;
    private Map<Utf8, Utf8> clientData;
    private String schemaVersion;

    @BeforeClass
    public static void beforeClass() throws Exception {
        String json = PropertyMgr.getInstance().loadFile("behavior.pulsar.sojevent.schema-undefined-384.avsc");
        schema = new Schema.Parser().parse(json);
    }

    @Before
    public void setUp() throws Exception {
        schemaVersion = StringConstants.EMPTY;
        Properties properties = PropertyMgr.getInstance()
                .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);
        SherlockioMetrics.init(properties.getProperty(PropertyConstants.SHERLOCKIO_NAMESPACE),
                properties.getProperty(PropertyConstants.SHERLOCKIO_ENDPOINT),
                properties.getProperty(PropertyConstants.SHERLOCKIO_USER));
        applicationMap = new HashMap<>();
        clientData = new HashMap<>();
        genericRecord = new GenericData.Record(schema);
        rheosEvent = new RheosEvent(schema);
        genericRecord.put("pageId", 2050535);
        genericRecord.put("pageName", new Utf8("Ginger.v1.batchtrack.POST"));
        genericRecord.put("applicationPayload", applicationMap);
        genericRecord.put("clientData", clientData);
        genericRecord.put("urlQueryString", new Utf8(URL_QUERY_STRING));
        genericRecord.put("eventTimestamp", 123456789L);
        genericRecord.put("guid", new Utf8("test-guid"));
        genericRecord.put("webServer", new Utf8("edgetrksvc.vip.qa.ebay.com"));
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
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
    }

    @Test
    public void getRlogId() {
        assertNull(transformer.getRlogId());
        genericRecord.put("rlogid", new Utf8("123456"));
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("123456", transformer.getRlogId());
    }

    @Test
    public void getTrackingId() {
        assertNull(transformer.getTrackingId());
    }

    @Test
    public void getUserId() {
        assertEquals(0L, transformer.getUserId());

        genericRecord.put("userId", new Utf8("userId"));
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals(0L, transformer.getUserId());

        genericRecord.put("userId", new Utf8("123"));
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals(123L, transformer.getUserId());
    }

    @Test
    public void getPublicUserId() {
        assertNull(transformer.getPublicUserId());
    }

    @Test
    public void getEncryptedUserId() {
        assertEquals(0L, transformer.getEncryptedUserId());

        applicationMap.put(new Utf8("bu"), new Utf8("userId"));
        genericRecord.put("applicationPayload", applicationMap);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals(0L, transformer.getEncryptedUserId());

        applicationMap.put(new Utf8("bu"), new Utf8("1234"));
        genericRecord.put("applicationPayload", applicationMap);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals(1234L, transformer.getEncryptedUserId());
    }

    @Test
    public void getGuid() {
        assertEquals("test-guid", transformer.getGuid());
    }

    @Test
    public void getIdfa() {
        applicationMap.put(new Utf8("idfa"), new Utf8("1234546"));
        genericRecord.put("applicationPayload", applicationMap);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("1234546", transformer.getIdfa());
    }

    @Test
    public void getGadid() {
        applicationMap.put(new Utf8("gadid"), new Utf8("1234546"));
        genericRecord.put("applicationPayload", applicationMap);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("1234546", transformer.getGadid());
    }

    @Test
    public void getDeviceId() {
        applicationMap.put(new Utf8("udid"), new Utf8("1234546"));
        genericRecord.put("applicationPayload", applicationMap);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("1234546", transformer.getDeviceId());
    }

    @Test
    public void getChannelType() {
        assertEquals(ChannelTypeEnum.GENERIC.getValue(), transformer.getChannelType());
    }

    @Test
    public void getActionType() {
        assertEquals(ActionTypeEnum.LAUNCH.getValue(), transformer.getActionType());
    }

    @Test
    public void getPartner() {
        applicationMap.put(new Utf8("mppid"), new Utf8("777"));
        genericRecord.put("applicationPayload", applicationMap);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("777", transformer.getPartner());
    }

    @Test
    public void getCampaignId() {
        assertNull(transformer.getCampaignId());
    }

    @Test
    public void getRotationId() {
        assertNull(transformer.getRotationId());
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
        assertNull(transformer.getUrl());
    }

    @Test
    public void getReferer() {
        assertNull(transformer.getReferer());

        applicationMap.put(new Utf8("referrer"), new Utf8("www.google.com"));
        genericRecord.put("applicationPayload", applicationMap);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("www.google.com", transformer.getReferer());

        genericRecord.put("referrer", "www.facebook.com");
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("www.facebook.com", transformer.getReferer());
    }

    @Test
    public void getUserAgent() {
        assertNull(transformer.getUserAgent());

        clientData.put(new Utf8("Agent"), new Utf8("Safari"));
        genericRecord.put("clientData", clientData);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("Safari", transformer.getUserAgent());
    }

    @Test
    public void getDeviceFamily() {
        clientData.put(new Utf8("Agent"), new Utf8("ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;"));
        genericRecord.put("clientData", clientData);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("Other", transformer.getDeviceFamily());
    }

    @Test
    public void getDeviceType() {
        clientData.put(new Utf8("Agent"), new Utf8("ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;"));
        genericRecord.put("clientData", clientData);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("iOS", transformer.getDeviceType());
    }

    @Test
    public void getBrowserVersion() {
        clientData.put(new Utf8("Agent"), new Utf8("ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;"));
        genericRecord.put("clientData", clientData);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertNull(transformer.getBrowserVersion());
    }

    @Test
    public void getBrowserFamily() {
        clientData.put(new Utf8("Agent"), new Utf8("ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;"));
        genericRecord.put("clientData", clientData);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertNull(transformer.getBrowserFamily());
    }

    @Test
    public void getOsFamily() {
        clientData.put(new Utf8("Agent"), new Utf8("ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;"));
        genericRecord.put("clientData", clientData);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("iOS", transformer.getOsFamily());
    }

    @Test
    public void getOsVersion() {
        clientData.put(new Utf8("Agent"), new Utf8("ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;"));
        genericRecord.put("clientData", clientData);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("11.2", transformer.getOsVersion());
    }

    @Test
    public void getAppId() {
        genericRecord.put("appId", new Utf8("test"));
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("test", transformer.getAppId());
    }

    @Test
    public void getAppVersion() {
        genericRecord.put("appVersion", new Utf8("test"));
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("test", transformer.getAppVersion());
    }

    @Test
    public void getService() {
        assertEquals("CHOCOLATE", transformer.getService());
    }

    @Test
    public void getServer() {
        genericRecord.put("webServer", new Utf8("edgetrksvc.vip.qa.ebay.com"));
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals("edgetrksvc.vip.qa.ebay.com", transformer.getServer());
    }

    @Test
    public void getRemoteIp() {
        assertEquals("", transformer.getRemoteIp());

        clientData.put(new Utf8("ForwardedFor"), new Utf8("localhost2"));
        genericRecord.put("clientData", clientData);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, "2");
        assertEquals("localhost2", transformer.getRemoteIp());
    }

    @Test
    public void getPageId() {
        assertEquals(2050535, transformer.getPageId());
    }

    @Test
    public void getGeoId() {
        assertEquals(0, transformer.getGeoId());

        applicationMap.put(new Utf8("uc"), new Utf8("uc"));
        genericRecord.put("applicationPayload", applicationMap);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals(0, transformer.getGeoId());

        applicationMap.put(new Utf8("uc"), new Utf8("1"));
        genericRecord.put("applicationPayload", applicationMap);
        transformer = new UTPPathfinderEventTransformer("", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertEquals(1, transformer.getGeoId());
    }

    @Test
    public void getIsBot() {
        assertFalse(transformer.getIsBot());
        transformer = new UTPPathfinderEventTransformer("behavior.pulsar.customized.tracking.install.bot", 0, 0L, genericRecord, rheosEvent, schemaVersion);
        assertTrue(transformer.getIsBot());
    }

    @Test
    public void getPayload() {
    }
}
