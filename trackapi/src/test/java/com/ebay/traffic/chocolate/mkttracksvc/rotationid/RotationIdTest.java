package com.ebay.traffic.chocolate.mkttracksvc.rotationid;

import com.ebay.globalenv.SiteEnum;
import com.ebay.traffic.chocolate.mkttracksvc.constant.TrackingChannelEnum;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.util.RotationId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;

public class RotationIdTest {

  @Test
  public void testGetNextCustomizedRotationId() {

    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannelId(TrackingChannelEnum.AFFILIATES.getId());
    rotationRequest.setSiteId(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaignId("000000001");
    rotationRequest.setCustomizedId("000000002");
    rotationRequest.setRotationName("CatherineTesting RotationName");
    // Customized rotationId
    String rotationId = RotationId.getNext(rotationRequest);
    Assert.assertEquals("0010077-000000001-000000002", rotationId.substring(0, rotationId.lastIndexOf("-")));
  }


  @Test
  public void testGetNextAutoGenRotationId() {

    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannelId(14);
    rotationRequest.setSiteId(11);
    rotationRequest.setRotationName("CatherineTesting RotationName");

    String rotationId = RotationId.getNext(rotationRequest);
    String[] rotationIdStr = rotationId.split("-");
    Assert.assertEquals(4, rotationIdStr.length);
    Assert.assertEquals("0140011", rotationIdStr[0]);
    Calendar current = Calendar.getInstance();
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(Long.valueOf(rotationIdStr[1]));
    Assert.assertEquals(current.get(Calendar.YEAR), c.get(Calendar.YEAR));
    Assert.assertEquals(current.get(Calendar.MONTH), c.get(Calendar.MONTH));
    Assert.assertEquals(current.get(Calendar.DATE), c.get(Calendar.DATE));
    c.setTimeInMillis(Long.valueOf(rotationIdStr[2]));
    Assert.assertEquals(current.get(Calendar.YEAR), c.get(Calendar.YEAR));
    Assert.assertEquals(current.get(Calendar.MONTH), c.get(Calendar.MONTH));
    Assert.assertEquals(current.get(Calendar.DATE), c.get(Calendar.DATE));
  }
}
