package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.traffic.monitoring.ESMetrics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RoiTransformerTest {
  private RoiTransformer transformer;
  private FilterMessage filterMessage;

  @Before
  public void setUp() throws Exception {
    filterMessage = new FilterMessage();
    transformer = new RoiTransformer(filterMessage);
    ESMetrics.init("test", "localhost");
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void getDstClientId() {
    assertEquals(Integer.valueOf(0), transformer.getDstClientId());
  }

  @Test
  public void getEbaySiteId() {
    assertEquals("",  transformer.getEbaySiteId());

    filterMessage.setUri("http://www.ebay.com?siteId=123");
    transformer = new RoiTransformer(filterMessage);
    assertEquals("123",  transformer.getEbaySiteId());
  }

  @Test
  public void getCartId() {
    assertEquals("0", transformer.getCartId());

    filterMessage.setUri("https://rover.ebay.com/roverroi/1/711-518-1801-10?mpuid=187937644;223525488837;2288208264012;123&siteId=0&BIN-Store=1&ff1=ss&ff2=CHECKOUT");
    transformer = new RoiTransformer(filterMessage);
    assertEquals("123", transformer.getCartId());
  }

  @Test
  public void getTransactionId() {
    assertEquals("0", transformer.getTransactionId());

    filterMessage.setUri("https://rover.ebay.com/roverroi/1/711-518-1801-10?mpuid=187937644;223525488837;2288208264012;&siteId=0&BIN-Store=1&ff1=ss&ff2=CHECKOUT");
    transformer = new RoiTransformer(filterMessage);
    assertEquals("2288208264012", transformer.getTransactionId());
  }

  @Test
  public void getTransactionType() {
    assertEquals("",  transformer.getTransactionType());

    filterMessage.setUri("http://www.ebay.com?tranType=123");
    transformer = new RoiTransformer(filterMessage);
    assertEquals("123",  transformer.getTransactionType());
  }

  @Test
  public void getItemId() {
    assertEquals("0", transformer.getItemId());

    filterMessage.setUri("https://rover.ebay.com/roverroi/1/711-518-1801-10?mpuid=187937644;223525488837;2288208264012;&siteId=0&BIN-Store=1&ff1=ss&ff2=CHECKOUT");
    transformer = new RoiTransformer(filterMessage);
    assertEquals("223525488837", transformer.getItemId());
  }

  @Test
  public void getRoiItemId() {
    assertEquals("0", transformer.getRoiItemId());

    filterMessage.setUri("https://rover.ebay.com/roverroi/1/711-518-1801-10?mpuid=187937644;223525488837;2288208264012;&siteId=0&BIN-Store=1&ff1=ss&ff2=CHECKOUT");
    transformer = new RoiTransformer(filterMessage);
    assertEquals("223525488837", transformer.getRoiItemId());
  }

  @Test
  public void getMgvalue() {
    assertEquals("0",  transformer.getMgvalue());
  }

  @Test
  public void getMgvaluereason() {
    assertEquals("",  transformer.getMgvaluereason());

    filterMessage.setUserAgent("Mozilla/4.0 (compatible; Cache)");
    transformer = new RoiTransformer(filterMessage);
    assertEquals("4",  transformer.getMgvaluereason());


    filterMessage.setRemoteIp("12.72.151.128");
    transformer = new RoiTransformer(filterMessage);
    assertEquals("4",  transformer.getMgvaluereason());
  }

  @Test
  public void getMgvalueRsnCd() {
    assertEquals("",  transformer.getMgvalueRsnCd());

    filterMessage.setUserAgent("Mozilla/4.0 (compatible; Cache)");
    transformer = new RoiTransformer(filterMessage);
    assertEquals("4",  transformer.getMgvalueRsnCd());


    filterMessage.setRemoteIp("12.72.151.128");
    transformer = new RoiTransformer(filterMessage);
    assertEquals("4",  transformer.getMgvalueRsnCd());
  }
}