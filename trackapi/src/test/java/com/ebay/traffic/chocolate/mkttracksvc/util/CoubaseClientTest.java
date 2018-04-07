package com.ebay.traffic.chocolate.mkttracksvc.util;

import com.ebay.globalenv.SiteEnum;
import com.ebay.traffic.chocolate.mkttracksvc.MKTTrackSvcConfigBean;
import com.ebay.traffic.chocolate.mkttracksvc.constant.TrackingChannelEnum;
import com.ebay.traffic.chocolate.mkttracksvc.dao.RotationCbDao;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;
import com.google.gson.Gson;
import org.junit.*;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;


public class CoubaseClientTest {
  private MKTTrackSvcConfigBean mktTrackSvcConfigProperties = mock(MKTTrackSvcConfigBean.class);

  @AfterClass
  public static void tearDown() {
    RotationCbDao.close();
  }

  @BeforeClass
  public static void construct() throws Exception {
    CouchbaseClientMock.createClient();
    RotationCbDao.init(CouchbaseClientMock.getCluster(), CouchbaseClientMock.getBucket());
  }

  @Test
  public void testInsertAndGetCouchBaseRecord() throws CBException {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannelId(TrackingChannelEnum.AFFILIATES.getId());
    rotationRequest.setSiteId(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaignId("500000001");
    rotationRequest.setCustomizedId("700000002");
    rotationRequest.setRotationName("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationRequest.setRotationTag(rotationTag);
    // Customized rotationId
    String rotationId = RotationId.getNext(rotationRequest);
    Assert.assertEquals("0010077-500000001-700000002", rotationId.substring(0, rotationId.lastIndexOf("-")));
    rotationRequest.setRotationId(rotationId);

    RotationCbDao.getInstance(mktTrackSvcConfigProperties).addRotationMap(rotationId, rotationRequest);
    String addedInfoStr = RotationCbDao.getInstance(mktTrackSvcConfigProperties).getRotationInfo(rotationId);
    RotationInfo addedInfo = new Gson().fromJson(addedInfoStr, RotationInfo.class);
    Assert.assertEquals("0010077-500000001-700000002", addedInfo.getRotationId().substring(0, addedInfo.getRotationId().lastIndexOf("-")));
    Assert.assertEquals("1", String.valueOf(addedInfo.getChannelId()));
    Assert.assertEquals("77", String.valueOf(addedInfo.getSiteId()));
    Assert.assertEquals("500000001", addedInfo.getCampaignId());
    Assert.assertEquals("700000002", addedInfo.getCustomizedId());
    Assert.assertEquals("CatherineTesting RotationName", addedInfo.getRotationName());
    Assert.assertEquals("RotationTag-1", addedInfo.getRotationTag().get("TestTag-1"));

  }

  @Test
  public void testUpdate() throws CBException {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannelId(TrackingChannelEnum.AFFILIATES.getId());
    rotationRequest.setSiteId(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaignId("500000001");
    rotationRequest.setCustomizedId("700000002");
    rotationRequest.setRotationName("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationRequest.setRotationTag(rotationTag);
    String rotationId = RotationId.getNext(rotationRequest);
    Assert.assertEquals("0010077-500000001-700000002", rotationId.substring(0, rotationId.lastIndexOf("-")));
    rotationRequest.setRotationId(rotationId);
    RotationCbDao.getInstance(mktTrackSvcConfigProperties).addRotationMap(rotationId, rotationRequest);

    // Update
    rotationRequest.setCampaignId("5555");
    rotationRequest.setCustomizedId("6666");
    rotationRequest.setRotationName("Updated RotationName");
    rotationTag.put("TestTag-1", "UpdatedTagName");
    RotationInfo updatedInfo = RotationCbDao.getInstance(mktTrackSvcConfigProperties).updateRotationMap(rotationId, rotationRequest);


    Assert.assertEquals(rotationId, updatedInfo.getRotationId());
    Assert.assertEquals("1", String.valueOf(updatedInfo.getChannelId()));
    Assert.assertEquals("77", String.valueOf(updatedInfo.getSiteId()));
    Assert.assertEquals("500000001", updatedInfo.getCampaignId());
    Assert.assertEquals("700000002", updatedInfo.getCustomizedId());
    Assert.assertEquals("Updated RotationName", updatedInfo.getRotationName());
    Assert.assertEquals("UpdatedTagName", updatedInfo.getRotationTag().get("TestTag-1"));
  }

  @Test
  public void testChangeStatus() throws CBException {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannelId(TrackingChannelEnum.AFFILIATES.getId());
    rotationRequest.setSiteId(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaignId("500000001");
    rotationRequest.setCustomizedId("700000002");
    rotationRequest.setRotationName("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationRequest.setRotationTag(rotationTag);
    String rotationId = RotationId.getNext(rotationRequest);
    Assert.assertEquals("0010077-500000001-700000002", rotationId.substring(0, rotationId.lastIndexOf("-")));
    rotationRequest.setRotationId(rotationId);
    RotationCbDao.getInstance(mktTrackSvcConfigProperties).addRotationMap(rotationId, rotationRequest);

    // Decctivate
    RotationInfo updatedInfo = RotationCbDao.getInstance(mktTrackSvcConfigProperties).setActiveStatus(rotationId, false);
    Assert.assertFalse(updatedInfo.getActive());

    // Activate
    updatedInfo = RotationCbDao.getInstance(mktTrackSvcConfigProperties).setActiveStatus(rotationId, true);
    Assert.assertTrue(updatedInfo.getActive());
  }
}
