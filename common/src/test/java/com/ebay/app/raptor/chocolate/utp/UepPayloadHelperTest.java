/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.utp;

import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class UepPayloadHelperTest {

  @Test
  public void getUtpPayload() {
    UepPayloadHelper helper = new UepPayloadHelper();
    String openUrl = "https://www.ebayadservices.com/marketingtracking/v1/impression?mkevt=4&mkpid=2&emsid=0&mkcid=8&bu=44076761443&osub=0&segname=seedlist&crd=20200611000000&ch=osgood&trkId=12345&sojTags=emid%3Dbu%2Cosub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2CtrkId%3DtrkId%2Cchnl%3Dmkcid";
    Map<String, String> result = helper.getUepPayload(openUrl, ActionTypeEnum.OPEN);
    System.out.println(result);
    assertEquals("1293411342", result.get("userid").toString());
  }
}