package com.ebay.traffic.chocolate.mkttracksvc.dao;

import com.ebay.app.raptor.chocolate.constant.MPLXChannelEnum;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.globalenv.SiteEnum;
import com.ebay.traffic.chocolate.mkttracksvc.MKTTrackSvcConfigBean;
import com.ebay.traffic.chocolate.mkttracksvc.dao.imp.RotationCbDaoImp;
import com.ebay.traffic.chocolate.mkttracksvc.entity.CampaignInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;
import com.ebay.traffic.chocolate.mkttracksvc.util.DriverId;
import com.ebay.traffic.chocolate.mkttracksvc.util.RotationId;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.booleanThat;
import static org.mockito.Mockito.mock;


public class RotationCbDaoTest {
  private static MKTTrackSvcConfigBean mktTrackSvcConfigProperties = mock(MKTTrackSvcConfigBean.class);
  private static RotationCbDaoImp rotationCbDao;


  @AfterClass
  public static void tearDown() {
    CouchbaseClientMock.getBucket().close();
    CouchbaseClientMock.tearDown();
  }

  @BeforeClass
  public static void construct() throws Exception {
    CouchbaseClientMock.createClient("rotation_info");
    rotationCbDao = new RotationCbDaoImp(CouchbaseClientMock.getBucket());
  }

  @Test
  public void testInsertAndGetCouchBaseRecord(){
    RotationInfo rotationRequest = getTestRotationInfo();
    rotationCbDao.addRotationMap(rotationRequest.getRotation_string(), rotationRequest);
    RotationInfo addedInfo = rotationCbDao.getRotationById(rotationRequest.getRotation_string());

//    RotationId rotationId = new RotationId(rotationRequest.getRotation_id());
//    Assert.assertEquals(String.valueOf(rotationId.getRepresentation()), String.valueOf(addedInfo.getRotation_id()));
//    Assert.assertEquals(rotationId.getRotationStr(707), addedInfo.getRotation_string());
    Assert.assertEquals("6", String.valueOf(addedInfo.getChannel_id()));
    Assert.assertEquals(MPLXChannelEnum.EPN.getMplxChannelName(), addedInfo.getRotation_tag().get(RotationConstant.FIELD_TAG_CHANNEL_NAME));
    Assert.assertEquals("77", String.valueOf(addedInfo.getSite_id()));
    Assert.assertEquals("DE", addedInfo.getRotation_tag().get(RotationConstant.FIELD_TAG_SITE_NAME));
    Assert.assertEquals("500000001", String.valueOf(addedInfo.getCampaign_id()));
    Assert.assertEquals("testing campaignName", String.valueOf(addedInfo.getCampaign_name()));
    Assert.assertEquals(String.valueOf(2003), String.valueOf(addedInfo.getVendor_id()));
    Assert.assertEquals("testing vendorName", String.valueOf(addedInfo.getVendor_name()));
    Assert.assertEquals("testing RotationName", addedInfo.getRotation_name());
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, addedInfo.getStatus());
    Assert.assertEquals("RotationTag-1", addedInfo.getRotation_tag().get("TestTag-1"));
    Assert.assertEquals("testing RotationDescription", addedInfo.getRotation_description());
    Assert.assertNull(addedInfo.getRotation_tag().get(RotationConstant.FIELD_TAG_PERFORMACE_STRATEGIC));
    Assert.assertNull(addedInfo.getRotation_tag().get(RotationConstant.FIELD_TAG_DEVICE));
  }


  @Test
  public void testUpdate() {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(MPLXChannelEnum.DISPLAY.getMplxChannelId());
    rotationRequest.setSite_id(SiteEnum.EBAY_US.getId());
    rotationRequest.setCampaign_id(500000002L);

    rotationRequest.setRotation_name("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationRequest.setRotation_tag(rotationTag);
    RotationId rotationId = RotationId.getNext(DriverId.getDriverIdFromIp());
    String rotationStr = rotationId.getRotationStr(711);
    rotationRequest.setRotation_id(rotationId.getRotationId(rotationStr));
    rotationRequest.setRotation_string(rotationStr);
    RotationInfo addedInfo = rotationCbDao.addRotationMap(rotationStr, rotationRequest);
    Assert.assertEquals(String.valueOf(rotationId.getRotationId(rotationStr)), String.valueOf(addedInfo.getRotation_id()));

    // Update
    RotationInfo updateRequest = new RotationInfo();
    updateRequest.setCampaign_id(5555L);
    updateRequest.setRotation_name("Updated RotationName");
    updateRequest.setChannel_id(MPLXChannelEnum.PAID_SEARCH.getMplxChannelId());
    updateRequest.setCampaign_id(4299L);
    updateRequest.setCampaign_name("Updated CampaignName");
    updateRequest.setVendor_id(123);
    updateRequest.setVendor_name("Updated VendorName");
    updateRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    updateRequest.setRotation_description("Test Strategy,Mobile,BR_XXX,Test_xxx");
    Map<String, String> updateRotationTag = new HashMap<String, String>();
    updateRotationTag.put(RotationConstant.FIELD_TAG_SITE_NAME, "DE");
    updateRotationTag.put(RotationConstant.FIELD_TAG_CHANNEL_NAME, MPLXChannelEnum.PAID_SEARCH.getMplxChannelName());
    updateRotationTag.put("TestTag-1", "UpdatedTagName");
    updateRequest.setRotation_tag(updateRotationTag);

    RotationInfo updatedInfo = rotationCbDao.updateRotationMap(rotationStr, updateRequest);


//    Assert.assertEquals(String.valueOf(rotationId.getRepresentation()), String.valueOf(updatedInfo.getRotation_id()));
    Assert.assertEquals(String.valueOf(MPLXChannelEnum.PAID_SEARCH.getMplxChannelId()), String.valueOf(updatedInfo.getChannel_id()));
    Assert.assertEquals("77", String.valueOf(updatedInfo.getSite_id()));
    Assert.assertEquals("4299", String.valueOf(updatedInfo.getCampaign_id()));
    Assert.assertEquals("Updated RotationName", updatedInfo.getRotation_name());
    Assert.assertEquals("Updated CampaignName", updatedInfo.getCampaign_name());
    Assert.assertEquals("Updated VendorName", updatedInfo.getVendor_name());
    Assert.assertEquals("Test Strategy,Mobile,BR_XXX,Test_xxx", updatedInfo.getRotation_description());
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, updatedInfo.getStatus());
  }

  @Test
  public void testChangeStatus() throws CBException {
    RotationInfo rotationRequest = getTestRotationInfo();
    RotationInfo addRotationInfo = rotationCbDao.addRotationMap(rotationRequest.getRotation_string(), rotationRequest);

    // Decctivate
    RotationInfo updatedInfo = rotationCbDao.setStatus(addRotationInfo.getRotation_string(), RotationInfo.STATUS_INACTIVE);
    Assert.assertEquals(RotationInfo.STATUS_INACTIVE, updatedInfo.getStatus());

    // Activate
    updatedInfo = rotationCbDao.setStatus(addRotationInfo.getRotation_string(), RotationInfo.STATUS_ACTIVE);
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, updatedInfo.getStatus());
  }

  @Test
  public void testGetRotationInfo() {
    RotationInfo rotationRequest = getTestRotationInfo();
    rotationRequest.setRotation_name("addedRotationName");
    RotationInfo addRotationInfo = rotationCbDao.addRotationMap(rotationRequest.getRotation_string(), rotationRequest);
    // get by Id
    RotationInfo rInfo = rotationCbDao.getRotationById(addRotationInfo.getRotation_string());
    Assert.assertEquals("addedRotationName", rInfo.getRotation_name());
    Assert.assertEquals("RotationTag-1", rInfo.getRotation_tag().get("TestTag-1"));
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, rInfo.getStatus());

//    // get by Name
//    List<RotationInfo> rInfoList = rotationCbDao.getRotationByName("addedRotationName");
//    Assert.assertTrue(rInfoList != null && rInfoList.size() > 0);
//    Assert.assertEquals("CatherineRotationName201806", rInfoList.get(0).getRotation_name());
//    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, rInfoList.get(0).getStatus());
  }

  private RotationInfo getTestRotationInfo(){
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(MPLXChannelEnum.EPN.getMplxChannelId());
    rotationRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaign_id(500000001L);
    rotationRequest.setVendor_id(2003);
    rotationRequest.setCampaign_name("testing campaignName");
    rotationRequest.setVendor_name("testing vendorName");
    rotationRequest.setRotation_name("testing RotationName");
    rotationRequest.setRotation_description("testing RotationDescription");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationTag.put(RotationConstant.FIELD_TAG_SITE_NAME, "DE");
    rotationTag.put(RotationConstant.FIELD_TAG_CHANNEL_NAME, MPLXChannelEnum.EPN.getMplxChannelName());
    rotationRequest.setRotation_tag(rotationTag);
    RotationId rotationId = RotationId.getNext(DriverId.getDriverIdFromIp());
    String rotationStr = rotationId.getRotationStr(707);
    rotationRequest.setRotation_id(rotationId.getRotationId(rotationStr));
    rotationRequest.setRotation_string(rotationStr);
    return rotationRequest;
  }

//  @Test
//  public void testIsExistedCampaign(){
//    RotationInfo rotationRequest = getTestRotationInfo();
//    rotationRequest.setCampaign_id(1234567890L);
//    rotationCbDao.addRotationMap(rotationRequest.getRotation_string(), rotationRequest);
//    RotationInfo rInfo = rotationCbDao.getRotationById(rotationRequest.getRotation_string());
//    Assert.assertNotNull(rInfo);
//
//    CampaignInfo campInfo = rotationCbDao.getExistedCampaignName(1234567890L);
////    Assert.assertTrue("campaignId not existed in CB.", isExisted);
////
////    isExisted = rotationCbDao.isExistedCampaign(9999999999L);
////    Assert.assertFalse("campaignId already existed in CB.", isExisted);
//  }
}
