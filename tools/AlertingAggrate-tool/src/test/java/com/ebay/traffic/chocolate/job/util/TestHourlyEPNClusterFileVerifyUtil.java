package com.ebay.traffic.chocolate.job.util;

import com.ebay.traffic.chocolate.util.HourlyEPNClusterFileVerifyUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestHourlyEPNClusterFileVerifyUtil {

  @Test
  public void testGetHour() {
    String path = "viewfs://apollo-rno/apps/b_marketing_tracking/watch/20200116/ams_click_hourly.done.202001160200000000";

    int hour = HourlyEPNClusterFileVerifyUtil.getHour(path);

    Assert.assertEquals(2, hour);
  }

  @Test
  public void testGetSeqenceNum() {
    String path = "viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt/click/click_dt=2020-01-15/dw_ams.ams_clicks_cs_20200115_application_1578483518072_53120_00272.dat.gz";

    int seqence = HourlyEPNClusterFileVerifyUtil.getSeqenceNumFromApollo(path);

    Assert.assertEquals(272, seqence);
  }

//  @Test
//  public void test() {
//    String path = "viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt/click/click_dt=2020-01-15/dw_ams.ams_clicks_cs_20200115_application_1578483518072_53120_00272.dat.gz";
//
//    HourlyEPNClusterFileVerifyUtil.getIdentifiedFileMap("",1,"");
//  }

}
