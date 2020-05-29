package com.ebay.app.raptor.chocolate.filter.util;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.PublisherCacheEntry;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import com.ebay.traffic.chocolate.common.MiniZookeeperCluster;
import com.ebay.traffic.monitoring.ESMetrics;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import scala.xml.Null;

import java.io.IOException;

import static com.ebay.app.raptor.chocolate.filter.service.FilterUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FilterUtilsTest {


    @BeforeClass
    public static void setUp() throws Exception{
    }

    @After
    public void destroy() {
    }

    @AfterClass
  public static void tearDown() throws IOException{

  }

    @Test
    public void testGetParamValueFromQuery() throws Exception {
      String parameterNmae1 = "transType";
      String parameterNmae2 = "";
      String parameterNmae3 = null;
      String parameterNmae4 = "aafqffqqf";
      String testURL1 = "www.ebay.com";
      String testURL2 = "http://mktcollectionsvc.vip.qa.ebay.com/marketingtracking/v1/roi?transType=BIN-Store" +
          "&uniqueTransactionId=10319119003&itemId=130013049856&transactionTimestamp=1587346164000&roisrc=1&siteId=77";
      String testURL3 = "http://mktcollectionsvc.vip.qa.ebay.com/marketingtracking/v1/roi?transTypessBIN-Store";
      String testURL4 = null;
      String testURL5 = "http://mktcollectionsvc.vip.qa.ebay.comi@transType=BIN-Store";
      assertNull(getParamValueFromQuery(testURL1, parameterNmae1));
      assertEquals(getParamValueFromQuery(testURL2, parameterNmae1), "BIN-Store");
      assertNull(getParamValueFromQuery(testURL2, parameterNmae2));
      assertNull(getParamValueFromQuery(testURL2, parameterNmae3));
      assertNull(getParamValueFromQuery(testURL2, parameterNmae4));
      assertNull(getParamValueFromQuery(testURL3, parameterNmae1));
      assertNull(getParamValueFromQuery(testURL4, parameterNmae1));
      assertNull(getParamValueFromQuery(testURL5, parameterNmae1));
    }

    @Test
    public void testIsBESRoiTransType() throws Exception {

      assertTrue(isBESRoiTransType("BIN-Store"));
      assertTrue(isBESRoiTransType("BIN-FP"));
      assertTrue(isBESRoiTransType("Bid"));
      assertTrue(isBESRoiTransType("BIN-ABIN"));
      assertTrue(isBESRoiTransType("BIN-ABIN"));
      assertFalse(isBESRoiTransType("BIN-aa"));
    }

    @Test
    public void testIsRoverBESRoi() throws Exception {
      FilterMessage outMessage = Mockito.mock(FilterMessage.class);;
      when(outMessage.getUri()).thenReturn("http://mktcollectionsvc.vip.qa.ebay.com/marketingtracking/v1/roi?transType=BIN-Store" +
          "&ff1=ss&ff2=TENSOR&transactionTimestamp=1587346164000&roisrc=1&siteId=77");
      when(outMessage.getChannelType()).thenReturn(ChannelType.ROI);
      assertTrue(isRoverBESRoi(outMessage));

      FilterMessage outMessage2 = Mockito.mock(FilterMessage.class);;
      when(outMessage2.getUri()).thenReturn("http://mktcollectionsvc.vip.qa.ebay.com/marketingtracking/v1/roi?transType=BIN-Store" +
          "&ff1=ss&transactionTimestamp=1587346164000&roisrc=1&siteId=77");
      when(outMessage2.getChannelType()).thenReturn(ChannelType.ROI);
      assertFalse(isRoverBESRoi(outMessage2));

      FilterMessage outMessage3 = Mockito.mock(FilterMessage.class);;
      when(outMessage3.getUri()).thenReturn("http://mktcollectionsvc.vip.qa.ebay.com/marketingtracking/v1/roi?transType=BIN-Store" +
          "&ff2=TENSOR&transactionTimestamp=1587346164000&roisrc=1&siteId=77");
      when(outMessage3.getChannelType()).thenReturn(ChannelType.ROI);
      assertFalse(isRoverBESRoi(outMessage3));
    }


}
