package com.ebay.traffic.chocolate.mkttracksvc.dao;

import com.ebay.globalenv.SiteEnum;
import com.ebay.traffic.chocolate.mkttracksvc.MKTTrackSvcConfigBean;
import com.ebay.app.raptor.chocolate.constant.TrackingChannelEnum;
import com.ebay.traffic.chocolate.mkttracksvc.dao.imp.RotationCbDaoImp;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;
import com.ebay.traffic.chocolate.mkttracksvc.util.RotationId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;


public class RotationCbDaoTest {
  private static MKTTrackSvcConfigBean mktTrackSvcConfigProperties = mock(MKTTrackSvcConfigBean.class);
  private static RotationCbDaoImp rotationCbDao = new RotationCbDaoImp(mktTrackSvcConfigProperties);


  @AfterClass
  public static void tearDown() {
    rotationCbDao.close();
  }

  @BeforeClass
  public static void construct() throws Exception {
    CouchbaseClientMock.createClient();
    rotationCbDao.init(CouchbaseClientMock.getCluster(), CouchbaseClientMock.getBucket());
  }

  @Test
  public void testInsertAndGetCouchBaseRecord() throws CBException {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(TrackingChannelEnum.AFFILIATES.getId());
    rotationRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaign_id(500000001L);
    rotationRequest.setCustomized_id1(200000001L);
    rotationRequest.setCustomized_id2(300000003L);
    rotationRequest.setRotation_name("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationRequest.setRotation_tag(rotationTag);
    // Customized rotationId
    String rotationId = RotationId.getNext(rotationRequest);
    Assert.assertEquals("707-500000001-200000001-300000003", rotationId);
    rotationRequest.setRotation_id(rotationId);

    rotationCbDao.addRotationMap(rotationId, rotationRequest);
    RotationInfo addedInfo = rotationCbDao.getRotationById(rotationId);
    Assert.assertEquals("707-500000001-200000001-300000003", addedInfo.getRotation_id());
    Assert.assertEquals("1", String.valueOf(addedInfo.getChannel_id()));
    Assert.assertEquals("77", String.valueOf(addedInfo.getSite_id()));
    Assert.assertEquals("500000001", String.valueOf(addedInfo.getCampaign_id()));
    Assert.assertEquals("200000001", String.valueOf(addedInfo.getCustomized_id1()));
    Assert.assertEquals("300000003", String.valueOf(addedInfo.getCustomized_id2()));
    Assert.assertEquals("CatherineTesting RotationName", addedInfo.getRotation_name());
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, addedInfo.getStatus());
    Assert.assertEquals("RotationTag-1", addedInfo.getRotation_tag().get("TestTag-1"));
  }

  @Test
  public void testInsertAndGetCouchBaseRecord2() throws CBException {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(TrackingChannelEnum.AFFILIATES.getId());
    rotationRequest.setSite_id(SiteEnum.EBAY_UK.getId());
    rotationRequest.setCampaign_id(500000001L);
    rotationRequest.setRotation_name("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationRequest.setRotation_tag(rotationTag);
    // Customized rotationId
    String rotationId = RotationId.getNext(rotationRequest);
    rotationRequest.setRotation_id(rotationId);

    rotationCbDao.addRotationMap(rotationId, rotationRequest);
    RotationInfo addedInfo = rotationCbDao.getRotationById(rotationId);
    Assert.assertEquals("1", String.valueOf(addedInfo.getChannel_id()));
    Assert.assertEquals("3", String.valueOf(addedInfo.getSite_id()));
    Assert.assertEquals("500000001", String.valueOf(addedInfo.getCampaign_id()));
    Assert.assertEquals("CatherineTesting RotationName", addedInfo.getRotation_name());
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, addedInfo.getStatus());
    Assert.assertEquals("RotationTag-1", addedInfo.getRotation_tag().get("TestTag-1"));
  }

  @Test
  public void testUpdate() throws CBException {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(TrackingChannelEnum.AFFILIATES.getId());
    rotationRequest.setSite_id(SiteEnum.EBAY_US.getId());
    rotationRequest.setCampaign_id(500000001L);
    rotationRequest.setCustomized_id1(200000001L);
    rotationRequest.setCustomized_id2(400000001L);
    rotationRequest.setRotation_name("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationRequest.setRotation_tag(rotationTag);
    String rotationId = RotationId.getNext(rotationRequest);
    Assert.assertEquals("711-500000001-200000001-400000001", rotationId);
    rotationRequest.setRotation_id(rotationId);
    rotationCbDao.addRotationMap(rotationId, rotationRequest);


    // Update
    rotationRequest.setCampaign_id(5555L);
    rotationRequest.setCustomized_id1(6666L);
    rotationRequest.setCustomized_id2(7777L);
    rotationRequest.setRotation_name("Updated RotationName");
    rotationTag.put("TestTag-1", "UpdatedTagName");
    RotationInfo updatedInfo = rotationCbDao.updateRotationMap(rotationId, rotationRequest);


    Assert.assertEquals(rotationId, updatedInfo.getRotation_id());
    Assert.assertEquals("1", String.valueOf(updatedInfo.getChannel_id()));
    Assert.assertEquals("1", String.valueOf(updatedInfo.getSite_id()));
    Assert.assertEquals("500000001", String.valueOf(updatedInfo.getCampaign_id()));
    Assert.assertEquals("200000001", String.valueOf(updatedInfo.getCustomized_id1()));
    Assert.assertEquals("400000001", String.valueOf(updatedInfo.getCustomized_id2()));
    Assert.assertEquals("Updated RotationName", updatedInfo.getRotation_name());
    Assert.assertEquals("UpdatedTagName", updatedInfo.getRotation_tag().get("TestTag-1"));
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, updatedInfo.getStatus());
  }

  @Test
  public void testChangeStatus() throws CBException {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(TrackingChannelEnum.AFFILIATES.getId());
    rotationRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaign_id(500000001L);
    rotationRequest.setCustomized_id1(200000001L);
    rotationRequest.setCustomized_id2(500000005L);
    rotationRequest.setRotation_name("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationRequest.setRotation_tag(rotationTag);
    String rotationId = RotationId.getNext(rotationRequest);
    Assert.assertEquals("707-500000001-200000001-500000005", rotationId);
    rotationRequest.setRotation_id(rotationId);
    rotationCbDao.addRotationMap(rotationId, rotationRequest);

    // Decctivate
    RotationInfo updatedInfo = rotationCbDao.setStatus(rotationId, RotationInfo.STATUS_INACTIVE);
    Assert.assertEquals(RotationInfo.STATUS_INACTIVE, updatedInfo.getStatus());

    // Activate
    updatedInfo = rotationCbDao.setStatus(rotationId, RotationInfo.STATUS_ACTIVE);
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, updatedInfo.getStatus());
  }

  @Test
  public void testGetRotationInfo() throws CBException {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(TrackingChannelEnum.AFFILIATES.getId());
    rotationRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaign_id(500000001L);
    rotationRequest.setCustomized_id1(200000001L);
    rotationRequest.setCustomized_id2(300000001L);
    rotationRequest.setRotation_name("rotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag", "RotationTag-1");
    rotationRequest.setRotation_tag(rotationTag);
    String rotationId = RotationId.getNext(rotationRequest);
    Assert.assertEquals("707-500000001-200000001-300000001", rotationId);
    rotationRequest.setRotation_id(rotationId);
    rotationCbDao.addRotationMap(rotationId, rotationRequest);

    // get by Id
    RotationInfo rInfo = rotationCbDao.getRotationById(rotationId);
    Assert.assertEquals("rotationName", rInfo.getRotation_name());
    Assert.assertEquals("RotationTag-1", rInfo.getRotation_tag().get("TestTag"));
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, rInfo.getStatus());

//    // get by Name
//    List<RotationInfo> rInfoList = rotationCbDao.getRotationByName("rotationName");
//    Assert.assertTrue(rInfoList != null && rInfoList.size() > 0);
//    Assert.assertEquals("TestName", rInfoList.get(0).getRotation_name());
//    Assert.assertEquals("CatherineTestingRotationName", rInfoList.get(0).getRotation_tag().get("TestTag"));
//    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, rInfoList.get(0).getStatus());
  }
}
