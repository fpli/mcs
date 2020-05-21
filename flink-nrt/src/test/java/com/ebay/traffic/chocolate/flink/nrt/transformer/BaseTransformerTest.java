package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.traffic.monitoring.ESMetrics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class BaseTransformerTest {
  private BaseTransformer transformer;
  private FilterMessage filterMessage;

  @Before
  public void setUp() throws Exception {
    filterMessage = new FilterMessage();
    ESMetrics.init("test", "localhost");
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void transform() {
  }

  @Test
  public void getField() {
  }

  @Test
  public void setField() {
  }

  @Test
  public void getTempUriQuery() {
  }

  @Test
  public void getBatchId() {
  }

  @Test
  public void getFileId() {
    assertEquals(Integer.valueOf(0), transformer.getFileId());
  }

  @Test
  public void getFileSchmVrsn() {
  }

  @Test
  public void getRvrId() {
  }

  @Test
  public void getEventDt() {
  }

  @Test
  public void getSrvdPstn() {
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
    transformer = new BaseTransformer(filterMessage);
    assertEquals("",  transformer.getCntryCd());
  }

  @Test
  public void getLangCd() {
    transformer = new BaseTransformer(filterMessage);
    assertEquals("",  transformer.getLangCd());
  }

  @Test
  public void getTrckngPrtnrId() {
    assertEquals(Integer.valueOf(0), transformer.getTrckngPrtnrId());
  }

  @Test
  public void getCguid() {
    transformer = new BaseTransformer(filterMessage);
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
    assertEquals(Long.valueOf(0), transformer.getUserId());
  }

  @Test
  public void getClntRemoteIp() {
    filterMessage.setRemoteIp("1234567");
    transformer = new BaseTransformer(filterMessage);
    assertEquals("1234567",  transformer.getClntRemoteIp());
  }

  @Test
  public void getBrwsrTypeId() {
    transformer = new BaseTransformer(filterMessage);
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
    transformer = new BaseTransformer(filterMessage);
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
  }

  @Test
  public void getUrlEncrptdYnInd() {
  }

  @Test
  public void getSrcRotationId() {
  }

  @Test
  public void getDstRotationId() {
  }

  @Test
  public void getDstClientId() {
  }

  @Test
  public void getPblshrId() {
  }

  @Test
  public void getLndngPageDmnName() {
  }

  @Test
  public void getLndngPageUrl() {
  }

  @Test
  public void getUserQuery() {
  }

  @Test
  public void getRuleBitFlagStrng() {
  }

  @Test
  public void getFlexFieldVrsnNum() {
  }

  @Test
  public void getFlexField1() {
  }

  @Test
  public void getFlexField2() {
  }

  @Test
  public void getFlexField3() {
  }

  @Test
  public void getFlexField4() {
  }

  @Test
  public void getFlexField5() {
  }

  @Test
  public void getFlexField6() {
  }

  @Test
  public void getFlexField7() {
  }

  @Test
  public void getFlexField8() {
  }

  @Test
  public void getFlexField9() {
  }

  @Test
  public void getFlexField10() {
  }

  @Test
  public void getFlexField11() {
  }

  @Test
  public void getFlexField12() {
  }

  @Test
  public void getFlexField13() {
  }

  @Test
  public void getFlexField14() {
  }

  @Test
  public void getFlexField15() {
  }

  @Test
  public void getFlexField16() {
  }

  @Test
  public void getFlexField17() {
  }

  @Test
  public void getFlexField18() {
  }

  @Test
  public void getFlexField19() {
  }

  @Test
  public void getFlexField20() {
  }

  @Test
  public void getEventTs() {
  }

  @Test
  public void getDfltBhrvId() {
  }

  @Test
  public void getKeyword() {
  }

  @Test
  public void getKwId() {
  }

  @Test
  public void getMtId() {
  }

  @Test
  public void getGeoId() {
  }

  @Test
  public void getCrlp() {
  }

  @Test
  public void getParamValueFromQuery() {
  }

  @Test
  public void getMfeName() {
  }

  @Test
  public void getMfeId() {
  }

  @Test
  public void getUserMapInd() {
  }

  @Test
  public void getCreativeId() {
  }

  @Test
  public void getTestCtrlFlag() {
  }

  @Test
  public void getTransactionType() {
  }

  @Test
  public void getTransactionId() {
  }

  @Test
  public void getItemId() {
  }

  @Test
  public void getRoiItemId() {
  }

  @Test
  public void getCartId() {
  }

  @Test
  public void getExtrnlCookie() {
  }

  @Test
  public void getEbaySiteId() {
  }

  @Test
  public void getRvrUrl() {
  }

  @Test
  public void getMgvalue() {
  }

  @Test
  public void getMgvalueRsnCd() {
  }

  @Test
  public void getClientIdFromRotationId() {
  }

  @Test
  public void getCreDate() {
  }

  @Test
  public void getCreUser() {
  }

  @Test
  public void getUpdDate() {
  }

  @Test
  public void getUpdUser() {
  }
}