package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.traffic.monitoring.ESMetrics;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static org.junit.Assert.*;

public class BaseTransformerTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private BaseTransformer transformer;
  private FilterMessage filterMessage;

  @Before
  public void setUp() throws Exception {
    filterMessage = new FilterMessage();
    transformer = new BaseTransformer(filterMessage);
    ESMetrics.init("test", "localhost");
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void transform() {
    SpecificRecordBase specificRecordBase1 = new MySpecificRecordBase();

    filterMessage.setUserId(123L);
    filterMessage.setShortSnapshotId(1234L);
    BaseTransformer transformer1 = new BaseTransformer(filterMessage);
    transformer1.transform(specificRecordBase1);

    assertEquals(123L, specificRecordBase1.get(0));
    assertEquals(1234L, specificRecordBase1.get(1));

    // test fieldCache
    assertEquals(1234L, transformer1.getField("rvr_id"));

    SpecificRecordBase specificRecordBase2 = new MySpecificRecordBase();

    filterMessage.setShortSnapshotId(4567L);

    BaseTransformer transformer2 = new BaseTransformer(filterMessage);
    transformer2.transform(specificRecordBase2);

    // test FIELD_GET_METHOD_CACHE
    assertEquals(4567L, transformer2.getField("rvr_id"));
  }

  @Test
  public void getField() {
    filterMessage.setShortSnapshotId(123456789L);
    transformer = new BaseTransformer(filterMessage);
    assertEquals(123456789L,  transformer.getField("rvr_id"));

    expectedException.expect(IllegalArgumentException.class);
    transformer.getField("param_value_from_query");

    expectedException.expect(NullPointerException.class);
    transformer.getField("invalid_field");
  }

  @Test
  public void getTempUriQuery() {
    filterMessage.setUri("http://www.ebay.com?a=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("a=test",  transformer.getTempUriQuery());

    filterMessage.setUri("http://www.ebay.com");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("",  transformer.getTempUriQuery());

    filterMessage.setUri("http://www.ebay.com?");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("",  transformer.getTempUriQuery());

    filterMessage.setUri("http://www.ebay.com?a=test&b=test2");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("a=test&b=test2",  transformer.getTempUriQuery());

    filterMessage.setUri("www.ebay.com?a=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("",  transformer.getTempUriQuery());
  }

  @Test
  public void getFileId() {
    assertEquals(Integer.valueOf(0), transformer.getFileId());
  }

  @Test
  public void getFileSchmVrsn() {
    assertEquals(Integer.valueOf(4), transformer.getFileSchmVrsn());
  }

  @Test
  public void getRvrId() {
    filterMessage.setShortSnapshotId(12345L);
    transformer = new BaseTransformer(filterMessage);
    assertEquals(Long.valueOf(12345L),  transformer.getRvrId());
  }

  @Test
  public void getEventDt() {
    // Tue Jun 30 23:38:44 -07 2020
    filterMessage.setTimestamp(1593585524000L);
    transformer = new BaseTransformer(filterMessage);
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(1593585524L), ZoneOffset.ofHours(-7));
    ZonedDateTime prodZonedDateTime = zonedDateTime.withZoneSameInstant(ZoneId.systemDefault());
    assertEquals(DateTimeFormatter.ofPattern("yyyy-MM-dd").format(prodZonedDateTime), transformer.getEventDt());
  }

  @Test
  public void getSrvdPstn() {
    assertEquals(Integer.valueOf(0), transformer.getSrvdPstn());
  }

  @Test
  public void getRvrCmndTypeCd() {
    filterMessage.setChannelAction(ChannelAction.IMPRESSION);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("4",  transformer.getRvrCmndTypeCd());

    filterMessage.setChannelAction(ChannelAction.SERVE);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("4",  transformer.getRvrCmndTypeCd());

    filterMessage.setChannelAction(ChannelAction.ROI);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("2",  transformer.getRvrCmndTypeCd());

    filterMessage.setChannelAction(ChannelAction.EMAIL_OPEN);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("1",  transformer.getRvrCmndTypeCd());

    filterMessage.setChannelAction(ChannelAction.CLICK);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("1",  transformer.getRvrCmndTypeCd());
  }

  @Test
  public void getRvrChnlTypeCd() {
    filterMessage.setChannelType(ChannelType.EPN);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("1",  transformer.getRvrChnlTypeCd());

    filterMessage.setChannelType(ChannelType.PAID_SEARCH);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("2",  transformer.getRvrChnlTypeCd());

    filterMessage.setChannelType(ChannelType.DISPLAY);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("4",  transformer.getRvrChnlTypeCd());

    filterMessage.setChannelType(ChannelType.SOCIAL_MEDIA);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("16",  transformer.getRvrChnlTypeCd());

    filterMessage.setChannelType(ChannelType.PAID_SOCIAL);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("20",  transformer.getRvrChnlTypeCd());

    filterMessage.setChannelType(ChannelType.NATURAL_SEARCH);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("3",  transformer.getRvrChnlTypeCd());

    filterMessage.setChannelType(ChannelType.ROI);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("0",  transformer.getRvrChnlTypeCd());
  }

  @Test
  public void getCntryCd() {
    assertEquals("",  transformer.getCntryCd());
  }

  @Test
  public void getLangCd() {
    assertEquals("",  transformer.getLangCd());
  }

  @Test
  public void getTrckngPrtnrId() {
    assertEquals(Integer.valueOf(0), transformer.getTrckngPrtnrId());
  }

  @Test
  public void getCguid() {
    assertEquals("",  transformer.getCguid());
  }

  @Test
  public void getGuid() {
    filterMessage.setGuid("1234567");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("1234567",  transformer.getGuid());
  }

  @Test
  public void getUserId() {
    filterMessage.setUserId(12345L);
    transformer = new BaseTransformer(filterMessage);
    assertEquals(Long.valueOf(12345L),  transformer.getUserId());
  }

  @Test
  public void getClntRemoteIp() {
    filterMessage.setRemoteIp("1234567");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("1234567",  transformer.getClntRemoteIp());
  }

  @Test
  public void getBrwsrTypeId() {
    assertEquals(Integer.valueOf(-99),  transformer.getBrwsrTypeId());

    filterMessage.setUserAgent("WEBTV");
    transformer = new BaseTransformer(filterMessage);
    assertEquals(Integer.valueOf(6),  transformer.getBrwsrTypeId());

    filterMessage.setUserAgent("TVWEB");
    transformer = new BaseTransformer(filterMessage);
    assertEquals(Integer.valueOf(-99),  transformer.getBrwsrTypeId());
  }

  @Test
  public void getBrwsrName() {
    filterMessage.setUserAgent("WEBTV");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("WEBTV",  transformer.getBrwsrName());
  }

  @Test
  public void getRfrrDmnName() {
    assertEquals("",  transformer.getRfrrDmnName());

    filterMessage.setReferer("https://www.google.com/search");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("www.google.com",  transformer.getRfrrDmnName());

    filterMessage.setReferer("www.google.com/search");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("",  transformer.getRfrrDmnName());
  }

  @Test
  public void getRfrrUrl() {
    filterMessage.setReferer("google.com");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("google.com",  transformer.getRfrrUrl());
  }

  @Test
  public void getUrlEncrptdYnInd() {
    assertEquals(Integer.valueOf(0),  transformer.getUrlEncrptdYnInd());
  }

  @Test
  public void getSrcRotationId() {
    filterMessage.setSrcRotationId(1234567L);
    transformer = new BaseTransformer(filterMessage);
    assertEquals(Long.valueOf(1234567L),  transformer.getSrcRotationId());
  }

  @Test
  public void getDstRotationId() {
    filterMessage.setDstRotationId(12345678L);
    transformer = new BaseTransformer(filterMessage);
    assertEquals(Long.valueOf(12345678L),  transformer.getDstRotationId());
  }

  @Test
  public void getDstClientId() {
    assertEquals(Integer.valueOf(0),  transformer.getDstClientId());

    filterMessage.setUri("mkrid=123");
    transformer = new BaseTransformer(filterMessage);
    assertEquals(Integer.valueOf(0),  transformer.getDstClientId());

    filterMessage.setUri("https://www.ebay.com?mkrid=123-123");
    transformer = new BaseTransformer(filterMessage);
    assertEquals(Integer.valueOf(123),  transformer.getDstClientId());
  }

  @Test
  public void getPblshrId() {
    assertEquals("",  transformer.getPblshrId());
  }

  @Test
  public void getLndngPageDmnName() {
    assertEquals("",  transformer.getLndngPageDmnName());

    filterMessage.setUri("www.ebay.com");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("",  transformer.getLndngPageDmnName());

    filterMessage.setUri("http://www.ebay.com");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("www.ebay.com",  transformer.getLndngPageDmnName());
  }

  @Test
  public void getLndngPageUrl() {
    assertEquals("",  transformer.getLndngPageUrl());

    filterMessage.setUri("mkgroupid=1&mktype=2");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("adgroupid=1&adtype=2",  transformer.getLndngPageUrl());
  }

  @Test
  public void getUserQuery() {
    filterMessage.setReferer("www.google.com");
    filterMessage.setUri("http://www.ebay.com?keyword=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getUserQuery());

    filterMessage.setReferer("http://www.google.com");
    filterMessage.setUri("http://www.ebay.com?keyword=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getUserQuery());

    filterMessage.setReferer("http://www.google.com?q=helloworld");
    filterMessage.setUri("http://www.ebay.com?keyword=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("helloworld",  transformer.getUserQuery());
  }

  @Test
  public void getRuleBitFlagStrng() {
    assertEquals("",  transformer.getRuleBitFlagStrng());
  }

  @Test
  public void getFlexFieldVrsnNum() {
    assertEquals(Integer.valueOf(0),  transformer.getFlexFieldVrsnNum());
  }

  @Test
  public void getFlexField1() {
    filterMessage.setUri("http://www.ebay.com?ff1=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField1());
  }

  @Test
  public void getFlexField2() {
    filterMessage.setUri("http://www.ebay.com?ff2=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField2());
  }

  @Test
  public void getFlexField3() {
    filterMessage.setUri("http://www.ebay.com?ff3=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField3());
  }

  @Test
  public void getFlexField4() {
    filterMessage.setUri("http://www.ebay.com?ff4=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField4());
  }

  @Test
  public void getFlexField5() {
    filterMessage.setUri("http://www.ebay.com?ff5=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField5());
  }

  @Test
  public void getFlexField6() {
    filterMessage.setUri("http://www.ebay.com?ff6=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField6());
  }

  @Test
  public void getFlexField7() {
    filterMessage.setUri("http://www.ebay.com?ff7=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField7());
  }

  @Test
  public void getFlexField8() {
    filterMessage.setUri("http://www.ebay.com?ff8=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField8());
  }

  @Test
  public void getFlexField9() {
    filterMessage.setUri("http://www.ebay.com?ff9=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField9());
  }

  @Test
  public void getFlexField10() {
    filterMessage.setUri("http://www.ebay.com?ff10=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField10());
  }

  @Test
  public void getFlexField11() {
    filterMessage.setUri("http://www.ebay.com?ff11=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField11());
  }

  @Test
  public void getFlexField12() {
    filterMessage.setUri("http://www.ebay.com?ff12=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField12());
  }

  @Test
  public void getFlexField13() {
    filterMessage.setUri("http://www.ebay.com?ff13=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField13());
  }

  @Test
  public void getFlexField14() {
    filterMessage.setUri("http://www.ebay.com?ff14=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField14());
  }

  @Test
  public void getFlexField15() {
    filterMessage.setUri("http://www.ebay.com?ff15=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField15());
  }

  @Test
  public void getFlexField16() {
    filterMessage.setUri("http://www.ebay.com?ff16=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField16());
  }

  @Test
  public void getFlexField17() {
    filterMessage.setUri("http://www.ebay.com?ff17=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField17());
  }

  @Test
  public void getFlexField18() {
    filterMessage.setUri("http://www.ebay.com?ff18=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField18());
  }

  @Test
  public void getFlexField19() {
    filterMessage.setUri("http://www.ebay.com?ff19=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField19());
  }

  @Test
  public void getFlexField20() {
    filterMessage.setUri("http://www.ebay.com?ff20=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getFlexField20());
  }

  @Test
  public void getEventTs() {
    // Tue Jun 30 23:38:44 -07 2020
    filterMessage.setTimestamp(1593585524000L);
    transformer = new BaseTransformer(filterMessage);
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochSecond(1593585524L), ZoneOffset.ofHours(-7));
    ZonedDateTime prodZoneDateTime = zonedDateTime.withZoneSameInstant(ZoneId.systemDefault());
    assertEquals(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").format(prodZoneDateTime), transformer.getEventTs());
  }

  @Test
  public void getDfltBhrvId() {
    assertEquals(Integer.valueOf(0),  transformer.getDfltBhrvId());
  }

  @Test
  public void getPerfTrackNameValue() {
    filterMessage.setUri("http://www.ebay.com?a=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("^a=test",  transformer.getPerfTrackNameValue());
  }

  @Test
  public void getKeyword() {
    filterMessage.setUri("http://www.ebay.com?kw=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("test",  transformer.getKeyword());
  }

  @Test
  public void getKwId() {
    assertEquals(Long.valueOf(-999L),  transformer.getKwId());
  }

  @Test
  public void getMtId() {
    filterMessage.setUri("http://www.ebay.com?mt_id=test");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("",  transformer.getMtId());

    filterMessage.setUri("http://www.ebay.com?mt_id=123");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("123",  transformer.getMtId());
  }

  @Test
  public void getGeoId() {
    filterMessage.setGeoId(12345L);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("12345",  transformer.getGeoId());
  }

  @Test
  public void getCrlp() {
    filterMessage.setUri("http://www.ebay.com?crlp=123");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("123",  transformer.getCrlp());
  }

  @Test
  public void getParamValueFromQuery() {
    assertEquals("test",  transformer.getParamValueFromQuery("a=test", "a"));
    assertEquals("test",  transformer.getParamValueFromQuery("a=test&b=test2", "a"));
    assertEquals("",  transformer.getParamValueFromQuery("", "a"));
    assertEquals("",  transformer.getParamValueFromQuery("a=test=test2", "a"));
    assertEquals("test",  transformer.getParamValueFromQuery("A=test", "a"));
  }

  @Test
  public void getMfeName() {
    filterMessage.setUri("http://www.ebay.com?crlp=123");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("123",  transformer.getMfeName());
  }

  @Test
  public void getMfeId() {
    filterMessage.setUri("http://www.ebay.com?crlp=123");
    transformer = new BaseTransformer(filterMessage);
    assertEquals(Integer.valueOf(-999),  transformer.getMfeId());

    filterMessage.setUri("http://www.ebay.com?crlp=message");
    transformer = new BaseTransformer(filterMessage);
    assertEquals(Integer.valueOf(33),  transformer.getMfeId());
  }

  @Test
  public void getUserMapInd() {
    assertEquals("0",  transformer.getUserMapInd());

    filterMessage.setUserId(0L);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("0",  transformer.getUserMapInd());

    filterMessage.setUserId(12345L);
    transformer = new BaseTransformer(filterMessage);
    assertEquals("1",  transformer.getUserMapInd());
  }

  @Test
  public void getCreativeId() {
    assertEquals(Long.valueOf(-999L),  transformer.getCreativeId());
  }

  @Test
  public void getTestCtrlFlag() {
    assertEquals(Integer.valueOf(0),  transformer.getTestCtrlFlag());
  }

  @Test
  public void getTransactionType() {
    assertEquals("",  transformer.getTransactionType());
  }

  @Test
  public void getTransactionId() {
    assertEquals("",  transformer.getTransactionId());
  }

  @Test
  public void getItemId() {
    assertEquals("",  transformer.getItemId());

    filterMessage.setUri("http://www.ebay.com/itm/aaa/123");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("123",  transformer.getItemId());

    filterMessage.setUri("http://www.ebay.com/itm/123");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("123",  transformer.getItemId());

    filterMessage.setUri("http://www.ebay.com/itm/aaa/123a");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("",  transformer.getItemId());

    filterMessage.setUri("http://www.ebay.com/i/aaa/123");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("123",  transformer.getItemId());

    filterMessage.setUri("http://www.ebay.com/item/aaa/123");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("",  transformer.getItemId());
  }

  @Test
  public void getRoiItemId() {
    assertEquals("",  transformer.getRoiItemId());
  }

  @Test
  public void getCartId() {
    assertEquals("",  transformer.getCartId());
  }

  @Test
  public void getExtrnlCookie() {
    assertEquals("",  transformer.getExtrnlCookie());
  }

  @Test
  public void getEbaySiteId() {
    assertEquals("",  transformer.getEbaySiteId());
  }

  @Test
  public void getRvrUrl() {
    assertEquals("",  transformer.getRvrUrl());

    filterMessage.setUri("mkgroupid=1&mktype=2");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("adgroupid=1&adtype=2",  transformer.getRvrUrl());
  }

  @Test
  public void getMgvalue() {
    assertEquals("",  transformer.getMgvalue());
  }

  @Test
  public void getMgvaluereason() {
    assertEquals("",  transformer.getMgvaluereason());
  }

  @Test
  public void getMgvalueRsnCd() {
    assertEquals("",  transformer.getMgvalueRsnCd());
  }

  @Test
  public void isBotByUserAgent() {
    assertFalse(transformer.isBotByUserAgent("", BaseTransformer.userAgentBotDawgDictionary));
    assertTrue(transformer.isBotByUserAgent("Mozilla/4.0 (compatible; Cache)", BaseTransformer.userAgentBotDawgDictionary));
  }

  @Test
  public void isBotByIp() {
    assertFalse(transformer.isBotByIp("", BaseTransformer.ipBotDawgDictionary));
    assertTrue(transformer.isBotByIp("12.72.151.128", BaseTransformer.ipBotDawgDictionary));
  }

  @Test
  public void isBot() {
    assertFalse(transformer.isBotByUserAgent("", BaseTransformer.userAgentBotDawgDictionary));
    assertTrue(transformer.isBotByUserAgent("Mozilla/4.0 (compatible; Cache)", BaseTransformer.userAgentBotDawgDictionary));

    assertFalse(transformer.isBotByIp("", BaseTransformer.ipBotDawgDictionary));
    assertTrue(transformer.isBotByIp("12.72.151.128", BaseTransformer.ipBotDawgDictionary));
  }

  @Test
  public void getClientIdFromRotationId() {
    transformer = new BaseTransformer(filterMessage);

    assertEquals(Integer.valueOf(0), transformer.getClientIdFromRotationId("123"));
    assertEquals(Integer.valueOf(711), transformer.getClientIdFromRotationId("711-123-223"));
    assertEquals(Integer.valueOf(0), transformer.getClientIdFromRotationId(""));
  }

  @Test
  public void getCreDate() {
    assertEquals("",  transformer.getCreDate());
  }

  @Test
  public void getCreUser() {
    assertEquals("",  transformer.getCreUser());
  }

  @Test
  public void getUpdDate() {
    assertEquals("",  transformer.getUpdDate());
  }

  @Test
  public void getUpdUser() {
    assertEquals("",  transformer.getUpdUser());
  }

  private static class MySpecificRecordBase extends SpecificRecordBase {
    private long user_id;
    private long rvr_id;

    public MySpecificRecordBase() {
    }

    @Override
    public Schema getSchema() {
      Schema.Field user_id = new Schema.Field("user_id", Schema.create(Schema.Type.LONG), "", 0);
      Schema.Field rvr_id = new Schema.Field("rvr_id", Schema.create(Schema.Type.LONG), "", 0);
      return Schema.createRecord(Arrays.asList(user_id, rvr_id));
    }

    @Override
    public Object get(int field) {
      switch (field) {
        case 0:
          return user_id;
        case 1:
          return rvr_id;
        default:
          throw new AvroRuntimeException("Bad index");
      }
    }

    @Override
    public void put(int field, Object value) {
      switch (field) {
        case 0:
          user_id = (Long)value;
          break;
        case 1:
          rvr_id = (Long)value;
          break;
        default:
          throw new AvroRuntimeException("Bad index");
      }
    }
  }
}