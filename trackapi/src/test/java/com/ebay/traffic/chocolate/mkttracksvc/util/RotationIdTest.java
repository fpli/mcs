package com.ebay.traffic.chocolate.mkttracksvc.util;

import com.ebay.globalenv.SiteEnum;
import com.ebay.app.raptor.chocolate.constant.TrackingChannelEnum;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;

public class RotationIdTest {

  @Test
  public void testGetNextCustomizedRotationId() {

    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(TrackingChannelEnum.AFFILIATES.getId());
    rotationRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaign_id("000000001");
    rotationRequest.setCustomized_id1("000000002");
    rotationRequest.setCustomized_id2("000000003");
    rotationRequest.setRotation_name("CatherineTesting RotationName");
    // Customized rotationId
    String rotationId = RotationId.getNext(rotationRequest);
    Assert.assertEquals("707-000000001-000000002-000000003", rotationId);
  }


  @Test
  public void testGetNextAutoGenRotationId() {

    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(14);
    rotationRequest.setSite_id(11);
    rotationRequest.setRotation_name("CatherineTesting RotationName");

    String rotationId = RotationId.getNext(rotationRequest);
    String[] rotationIdStr = rotationId.split("-");
    Assert.assertEquals(4, rotationIdStr.length);
    Assert.assertEquals("11", rotationIdStr[0]);
    Calendar current = Calendar.getInstance();
    RotationId18 r18 = new RotationId18(Long.valueOf(rotationIdStr[1]));
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(r18.getTimeMillis());
    Assert.assertEquals(current.get(Calendar.YEAR), c.get(Calendar.YEAR));
    Assert.assertEquals(current.get(Calendar.MONTH), c.get(Calendar.MONTH));
    Assert.assertEquals(current.get(Calendar.DATE), c.get(Calendar.DATE));
    r18 = new RotationId18(Long.valueOf(rotationIdStr[2]));
    c.setTimeInMillis(r18.getTimeMillis());
    Assert.assertEquals(current.get(Calendar.YEAR), c.get(Calendar.YEAR));
    Assert.assertEquals(current.get(Calendar.MONTH), c.get(Calendar.MONTH));
    Assert.assertEquals(current.get(Calendar.DATE), c.get(Calendar.DATE));
    r18 = new RotationId18(Long.valueOf(rotationIdStr[3]));
    c.setTimeInMillis(r18.getTimeMillis());
    Assert.assertEquals(current.get(Calendar.YEAR), c.get(Calendar.YEAR));
    Assert.assertEquals(current.get(Calendar.MONTH), c.get(Calendar.MONTH));
    Assert.assertEquals(current.get(Calendar.DATE), c.get(Calendar.DATE));
  }
}
