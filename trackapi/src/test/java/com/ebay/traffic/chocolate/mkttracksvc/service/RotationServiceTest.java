package com.ebay.traffic.chocolate.mkttracksvc.service;

import com.ebay.app.raptor.chocolate.constant.MPLXChannelEnum;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.globalenv.SiteEnum;
import com.ebay.traffic.chocolate.mkttracksvc.constant.ErrorMsgConstant;
import com.ebay.traffic.chocolate.mkttracksvc.dao.RotationCbDao;
import com.ebay.traffic.chocolate.mkttracksvc.entity.CampaignInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.ServiceResponse;
import com.ebay.traffic.chocolate.mkttracksvc.util.DriverId;
import com.ebay.traffic.chocolate.mkttracksvc.util.RotationId;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RotationServiceTest {

  @InjectMocks
  private RotationService rotationService = new RotationServiceImpl();

  @Mock
  private RotationCbDao rotationCbDao;

  @Mock
  private ESMetrics esMetrics;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    rotationService.setEsMetrics(esMetrics);
    Mockito.doNothing().when(esMetrics).meter(Mockito.anyString());
  }

  @Test
  public void testAddRotationWithoutCampaignId() {
    RotationInfo rotationRequest = getTestRotationInfo();
    rotationRequest.setCampaign_id(null);
    Mockito.when(rotationCbDao.addRotationMap(Mockito.anyString(), Mockito.any(RotationInfo.class))).thenReturn(rotationRequest);
    // New campaign
    ServiceResponse response = rotationService.addRotationMap(rotationRequest);
    Assert.assertNotNull(response);
    RotationInfo rInfo = response.getRotation_info();
    Assert.assertNotNull(rInfo);
    Assert.assertNotNull(rInfo.getRotation_id());
    Assert.assertNotNull(rInfo.getLast_update_time());
    Assert.assertNotNull(rInfo.getCampaign_id());
    Assert.assertNull(response.getMessage());
  }

  @Test
  public void testAddRotationWithExistedCampaignId() {
    RotationInfo rotationRequest = getTestRotationInfo();
    CampaignInfo campaignInfo = new CampaignInfo();
    campaignInfo.setExisted(true);
    campaignInfo.setCampaign_id(rotationRequest.getCampaign_id());
    // Existed CampaignId, Different campaignName
    campaignInfo.setCampaign_name("DiffrentName");
    Mockito.when(rotationCbDao.getExistedCampaignName(Mockito.anyLong())).thenReturn(campaignInfo);
    Mockito.when(rotationCbDao.addRotationMap(Mockito.anyString(), Mockito.any(RotationInfo.class))).thenReturn(rotationRequest);
    ServiceResponse response = rotationService.addRotationMap(rotationRequest);
    RotationInfo rInfo = response.getRotation_info();
    Assert.assertNotNull(rInfo);
    Assert.assertNotNull(response);
    Assert.assertTrue(response.getMessage().contains("already existed"));
    Assert.assertEquals(response.getCampaign_info().getCampaign_id(), rotationRequest.getCampaign_id());
    Assert.assertEquals(response.getCampaign_info().getCampaign_name(), campaignInfo.getCampaign_name());


    // Existed CampaignId, Same campaignName
    campaignInfo.setCampaign_name(rotationRequest.getCampaign_name());
    Mockito.when(rotationCbDao.getExistedCampaignName(Mockito.anyLong())).thenReturn(campaignInfo);
    response = rotationService.addRotationMap(rotationRequest);
    rInfo = response.getRotation_info();
    Assert.assertNotNull(rInfo);
    Assert.assertNotNull(response);
    Assert.assertNull(response.getMessage());
    Assert.assertNull(response.getCampaign_info());
  }

  @Test
  public void testAddRotationWithNotExistedCampaignId() {
    RotationInfo rotationRequest = getTestRotationInfo();
    Mockito.when(rotationCbDao.getExistedCampaignName(Mockito.anyLong())).thenReturn(null);
    Mockito.when(rotationCbDao.addRotationMap(Mockito.anyString(), Mockito.any(RotationInfo.class))).thenReturn(rotationRequest);
    // Existed Campaign
    ServiceResponse response = rotationService.addRotationMap(rotationRequest);
    RotationInfo rInfo = response.getRotation_info();
    Assert.assertNotNull(rInfo);
    Assert.assertNotNull(response);
    Assert.assertNull(response.getMessage());
    Assert.assertNull(response.getCampaign_info());
  }

  @Test
  public void testGetCampaignInfo() {
    // CampaignID = null
    Mockito.when(rotationCbDao.getExistedCampaignName(Mockito.anyLong())).thenReturn(null);
    ServiceResponse response = rotationService.getCampaignInfo(null);
    Assert.assertNotNull(response);
    Assert.assertNull(response.getMessage());
    Assert.assertNotNull(response.getCampaign_info());
    Assert.assertNotNull(response.getCampaign_info().getCampaign_id());
    Assert.assertNull(response.getCampaign_info().getCampaign_name());

    //New Campaign
    CampaignInfo campaignInfo = new CampaignInfo();
    campaignInfo.setCampaign_id(12345L);
    Mockito.when(rotationCbDao.getExistedCampaignName(Mockito.anyLong())).thenReturn(null);
    response = rotationService.getCampaignInfo(12345L);
    Assert.assertNotNull(response);
    Assert.assertEquals(ErrorMsgConstant.CB_INSERT_CAMPAIGN_INFO, response.getMessage());
    Assert.assertNotNull(response.getCampaign_info());

    //Existed Campaign
    campaignInfo = new CampaignInfo();
    campaignInfo.setExisted(true);
    campaignInfo.setCampaign_id(7654321L);
    campaignInfo.setCampaign_name("Existed CampaignName");
    Mockito.when(rotationCbDao.getExistedCampaignName(Mockito.anyLong())).thenReturn(campaignInfo);
    response = rotationService.getCampaignInfo(7654321L);
    Assert.assertNotNull(response);
    Assert.assertEquals(response.getMessage(),
        String.format(ErrorMsgConstant.CB_INSERT_CAMPAIGN_ISSUE, campaignInfo.getCampaign_id(), campaignInfo.getCampaign_name()));
    Assert.assertNotNull(response.getCampaign_info());
    Assert.assertEquals(response.getCampaign_info().getCampaign_id(), campaignInfo.getCampaign_id());
    Assert.assertEquals(response.getCampaign_info().getCampaign_name(), campaignInfo.getCampaign_name());
  }

  @Test
  public void testUpdateRotationInfo() throws ParseException {
    RotationInfo rotationRequest = getTestRotationInfo();
    rotationRequest.setCampaign_id(null);
    rotationRequest.setRotation_name("Updated Rotation Name");
    rotationRequest.setRotation_description("Updated Rotation Desc Txt");
    Mockito.when(rotationCbDao.updateRotationMap(Mockito.anyString(), Mockito.any(RotationInfo.class))).thenReturn(rotationRequest);
    ServiceResponse response = rotationService.updateRotationMap(rotationRequest.getRotation_string(), rotationRequest);
    Assert.assertNotNull(response);
    RotationInfo rInfo = response.getRotation_info();
    Assert.assertNotNull(rInfo);
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat sdfOld = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Assert.assertEquals(sdf.format(new Date()), sdf.format(sdfOld.parse(rInfo.getUpdate_date())));
    Assert.assertEquals("Updated Rotation Name", rInfo.getRotation_name());
    Assert.assertEquals("Updated Rotation Desc Txt", rInfo.getRotation_description());
    Assert.assertNull(response.getMessage());
  }

  @Test
  public void testSetStatus() throws ParseException {
    // NOT Existed Rotation ID
    Mockito.when(rotationCbDao.setStatus(Mockito.anyString(), Mockito.anyString())).thenReturn(null);
    ServiceResponse response = rotationService.setStatus("123-456-2-1", "ACTIVE");
    Assert.assertNotNull(response);
    Assert.assertNull(response.getRotation_info());
    Assert.assertEquals(String.format(ErrorMsgConstant.CB_ACTIVATE_ROTATION_ISSUE, "123-456-2-1"), response.getMessage());

    // ACTIVATE
    RotationInfo rotationRequest = getTestRotationInfo();
    rotationRequest.setStatus("ACTIVE");
    Mockito.when(rotationCbDao.setStatus(Mockito.anyString(), Mockito.anyString())).thenReturn(rotationRequest);
    response = rotationService.setStatus(rotationRequest.getRotation_string(), "ACTIVE");
    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getRotation_info());
    Assert.assertEquals("ACTIVE", response.getRotation_info().getStatus());
    Assert.assertNull(response.getMessage());
  }

  @Test
  public void testGetRotationById() throws ParseException {
    // NOT Existed Rotation ID
    Mockito.when(rotationCbDao.getRotationById(Mockito.anyString())).thenReturn(null);
    ServiceResponse response = rotationService.getRotationById("123-456-2-1");
    Assert.assertNotNull(response);
    Assert.assertNull(response.getRotation_info());
    Assert.assertEquals(String.format(ErrorMsgConstant.CB_GET_ROTATION_ISSUE_BY_ID, "123-456-2-1"), response.getMessage());

    // Existed
    RotationInfo rotationRequest = getTestRotationInfo();
    Mockito.when(rotationCbDao.getRotationById(Mockito.anyString())).thenReturn(rotationRequest);
    response = rotationService.getRotationById("123-456");
    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getRotation_info());
    Assert.assertEquals(rotationRequest.getRotation_description(), response.getRotation_info().getRotation_description());
    Assert.assertNull(response.getMessage());
  }

  @Test
  public void testGetRotationByName() throws ParseException {
    // NOT Existed Rotation ID
    Mockito.when(rotationCbDao.getRotationByName(Mockito.anyString())).thenReturn(null);
    ServiceResponse response = rotationService.getRotationByName("GetRotationName");
    Assert.assertNotNull(response);
    Assert.assertNull(response.getRotation_info());
    Assert.assertEquals(String.format(ErrorMsgConstant.CB_GET_ROTATION_ISSUE_BY_NAME, "GetRotationName"), response.getMessage());

    // Existed
    RotationInfo rotationRequest = getTestRotationInfo();
    List<RotationInfo> rInfoList = new ArrayList<>();
    rInfoList.add(rotationRequest);
    Mockito.when(rotationCbDao.getRotationByName(Mockito.anyString())).thenReturn(rInfoList);
    response = rotationService.getRotationByName(rotationRequest.getRotation_string());
    Assert.assertNotNull(response);
    Assert.assertNull(response.getRotation_info());
    Assert.assertNotNull(response.getRotation_info_list());
    Assert.assertEquals(rotationRequest.getRotation_description(), response.getRotation_info_list().get(0).getRotation_description());
    Assert.assertNull(response.getMessage());
  }

  private RotationInfo getTestRotationInfo() {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(MPLXChannelEnum.EPN.getMplxChannelId());
    rotationRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    rotationRequest.setVendor_id(2003);
    rotationRequest.setCampaign_id(1234567L);
    rotationRequest.setCampaign_name("testing CampaignName");
    rotationRequest.setVendor_name("testing vendorName");
    rotationRequest.setRotation_name("CatherineTesting2018 RotationName");
    rotationRequest.setRotation_description("Strategic,Mobile,Direct,Android,1x1_Android_IP_Deal_Free_Listing_V1");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationTag.put(RotationConstant.FIELD_TAG_SITE_NAME, "DE");
    rotationTag.put(RotationConstant.FIELD_TAG_CHANNEL_NAME, MPLXChannelEnum.EPN.getMplxChannelName());
    rotationTag.put("vendor_url", "http://prodigy.msn.com/es-mx/");
    rotationTag.put(RotationConstant.FIELD_ROTATION_START_DATE, "20180410");
    rotationTag.put(RotationConstant.FIELD_ROTATION_END_DATE, "20180515");
    rotationTag.put(RotationConstant.FIELD_ROTATION_CLICK_THRU_URL, "http://www.ebay.co.uk");
    rotationRequest.setRotation_tag(rotationTag);
    RotationId rotationId = RotationId.getNext(DriverId.getDriverIdFromIp());
    String rotationStr = rotationId.getRotationStr(707);
    rotationRequest.setRotation_id(rotationId.getRotationId(rotationStr));
    rotationRequest.setRotation_string(rotationStr);
    return rotationRequest;
  }
}
