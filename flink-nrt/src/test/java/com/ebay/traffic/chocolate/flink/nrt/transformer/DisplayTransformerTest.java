package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.traffic.monitoring.ESMetrics;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DisplayTransformerTest {
  private DisplayTransformer transformer;
  private FilterMessage filterMessage;

  @Before
  public void setUp() throws Exception {
    filterMessage = new FilterMessage();
    transformer = new DisplayTransformer(filterMessage);
    ESMetrics.init("test", "localhost");
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void getMgvalue() {
    assertEquals("0",  transformer.getMgvalue());
  }

  @Test
  public void getMgvaluereason() {
    assertEquals("",  transformer.getMgvaluereason());

    filterMessage.setUserAgent("Mozilla/4.0 (compatible; Cache)");
    transformer = new DisplayTransformer(filterMessage);
    assertEquals("4",  transformer.getMgvaluereason());


    filterMessage.setRemoteIp("12.72.151.128");
    transformer = new DisplayTransformer(filterMessage);
    assertEquals("4",  transformer.getMgvaluereason());
  }

  @Test
  public void getMgvalueRsnCd() {
    assertEquals("",  transformer.getMgvalueRsnCd());

    filterMessage.setUserAgent("Mozilla/4.0 (compatible; Cache)");
    transformer = new DisplayTransformer(filterMessage);
    assertEquals("4",  transformer.getMgvalueRsnCd());


    filterMessage.setRemoteIp("12.72.151.128");
    transformer = new DisplayTransformer(filterMessage);
    assertEquals("4",  transformer.getMgvalueRsnCd());
  }
}