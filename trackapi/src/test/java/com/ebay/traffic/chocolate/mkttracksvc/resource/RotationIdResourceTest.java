package com.ebay.traffic.chocolate.mkttracksvc.resource;

import com.ebay.app.raptor.chocolate.constant.MPLXChannelEnum;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.globalenv.SiteEnum;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.traffic.chocolate.mkttracksvc.constant.ErrorMsgConstant;
import com.ebay.traffic.chocolate.mkttracksvc.dao.CouchbaseClient;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.ServiceResponse;
import com.ebay.traffic.chocolate.mkttracksvc.util.DriverId;
import com.ebay.traffic.chocolate.mkttracksvc.util.RotationId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.junit4.SpringRunner;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {"GingerClient.testService.testClient.endpointUri=http://localhost",
        "GingerClient.testService.testClient.readTimeout=10000"})
public class RotationIdResourceTest {
  @LocalServerPort
  int port;

  @Autowired
  private CouchbaseClient couchbaseClient;

  Configuration configuration;
  Client client;
  String endpoint;
  String svcEndPoint;

  private static final String BASE_PATH = "/tracksvc/v1/rid";
  private static final String CREATE_PATH = BASE_PATH + "/create";
  private static final String UPDATE_PATH = BASE_PATH + "/update";
  private static final String ACTIVATE_PATH = BASE_PATH + "/activate";
  private static final String DEACTIVATE_PATH = BASE_PATH + "/deactivate";


  @Before
  public void initBeforeTest() {
    configuration = ConfigurationBuilder.newConfig("testService.testClient");
    client = ClientBuilder.newClient(configuration);
    endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    svcEndPoint = endpoint + ":" + port;
  }

  private RotationInfo getTestRotationInfo() {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(MPLXChannelEnum.EPN.getMplxChannelId());
    rotationRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaign_id(500000001L);
    rotationRequest.setVendor_id(2003);
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

  @Test
  public void testCreate() {
    RotationInfo rotationRequest = getTestRotationInfo();
    Response result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    ServiceResponse serviceResponse = result.readEntity(ServiceResponse.class);
    RotationInfo rotationResponse = serviceResponse.getRotation_info();

    Assert.assertNotNull(rotationResponse.getRotation_id());
    Assert.assertEquals("16", String.valueOf(String.valueOf(rotationResponse.getRotation_id()).length()));
    Assert.assertNotNull(rotationResponse.getRotation_string());
    Assert.assertEquals("707", rotationResponse.getRotation_string().split("-")[0]);
    Assert.assertEquals("6", String.valueOf(rotationResponse.getChannel_id()));
    Assert.assertEquals(MPLXChannelEnum.EPN.getMplxChannelName(), rotationResponse.getRotation_tag().get(RotationConstant.FIELD_TAG_CHANNEL_NAME));
    Assert.assertEquals("77", String.valueOf(rotationResponse.getSite_id()));
    Assert.assertEquals("DE", rotationResponse.getRotation_tag().get(RotationConstant.FIELD_TAG_SITE_NAME));
    Assert.assertEquals("500000001", String.valueOf(rotationResponse.getCampaign_id()));
    Assert.assertEquals("testing CampaignName", String.valueOf(rotationResponse.getCampaign_name()));
    Assert.assertEquals(String.valueOf(2003), String.valueOf(rotationResponse.getVendor_id()));
    Assert.assertEquals("testing vendorName", String.valueOf(rotationResponse.getVendor_name()));
    Assert.assertEquals("CatherineTesting2018 RotationName", rotationResponse.getRotation_name());
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, rotationResponse.getStatus());
    Assert.assertEquals("RotationTag-1", rotationResponse.getRotation_tag().get("TestTag-1"));
    Assert.assertEquals("Strategic,Mobile,Direct,Android,1x1_Android_IP_Deal_Free_Listing_V1", rotationResponse.getRotation_description());
    Assert.assertEquals("Strategic", rotationResponse.getRotation_tag().get(RotationConstant.FIELD_TAG_PERFORMACE_STRATEGIC));
    Assert.assertEquals("Mobile", rotationResponse.getRotation_tag().get(RotationConstant.FIELD_TAG_DEVICE));
    Assert.assertEquals("20180410", rotationResponse.getRotation_tag().get(RotationConstant.FIELD_ROTATION_START_DATE));
    Assert.assertEquals("20180515", rotationResponse.getRotation_tag().get(RotationConstant.FIELD_ROTATION_END_DATE));
  }

  @Test
  public void testUpdate() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    //Create
    RotationInfo rotationRequest = getTestRotationInfo();
    Response createResult = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, createResult.getStatus());
    ServiceResponse serviceResponse = createResult.readEntity(ServiceResponse.class);
    RotationInfo createResponse = serviceResponse.getRotation_info();

    //Update
    String ridStr = createResponse.getRotation_string();
    rotationRequest.setRotation_name("UpdatedRotationName");
    rotationRequest.setRotation_description("UpdatedStrategic,UpdatedMobile,TestDirect,Android,1x1_Android_IP_Deal_Free_Listing_V1");
    rotationRequest.getRotation_tag().put("TestTag-2", "Updated-RotationTag-2");

    Response updateResult = client.target(svcEndPoint).path(UPDATE_PATH)
        .queryParam("rid", ridStr)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, updateResult.getStatus());
    serviceResponse = updateResult.readEntity(ServiceResponse.class);
    RotationInfo updateResponse = serviceResponse.getRotation_info();


    Assert.assertEquals(ridStr, updateResponse.getRotation_string());
    Assert.assertEquals("6", String.valueOf(updateResponse.getChannel_id()));
    Assert.assertEquals(MPLXChannelEnum.EPN.getMplxChannelName(), updateResponse.getRotation_tag().get(RotationConstant.FIELD_TAG_CHANNEL_NAME));
    Assert.assertEquals("77", String.valueOf(updateResponse.getSite_id()));
    Assert.assertEquals("DE", updateResponse.getRotation_tag().get(RotationConstant.FIELD_TAG_SITE_NAME));
    Assert.assertEquals("500000001", String.valueOf(updateResponse.getCampaign_id()));
    Assert.assertEquals("testing CampaignName", String.valueOf(updateResponse.getCampaign_name()));
    Assert.assertEquals(String.valueOf(2003), String.valueOf(updateResponse.getVendor_id()));
    Assert.assertEquals("testing vendorName", String.valueOf(updateResponse.getVendor_name()));
    Assert.assertEquals("UpdatedRotationName", updateResponse.getRotation_name());
    Assert.assertEquals("RotationTag-1", updateResponse.getRotation_tag().get("TestTag-1"));
    Assert.assertEquals("Updated-RotationTag-2", updateResponse.getRotation_tag().get("TestTag-2"));
    Assert.assertEquals("UpdatedStrategic,UpdatedMobile,TestDirect,Android,1x1_Android_IP_Deal_Free_Listing_V1", updateResponse.getRotation_description());
    Assert.assertEquals("UpdatedStrategic", updateResponse.getRotation_tag().get(RotationConstant.FIELD_TAG_PERFORMACE_STRATEGIC));
    Assert.assertEquals("UpdatedMobile", updateResponse.getRotation_tag().get(RotationConstant.FIELD_TAG_DEVICE));
    Assert.assertEquals("20180410", updateResponse.getRotation_tag().get(RotationConstant.FIELD_ROTATION_START_DATE));
    Assert.assertEquals("20180515", updateResponse.getRotation_tag().get(RotationConstant.FIELD_ROTATION_END_DATE));
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, updateResponse.getStatus());

    Map<String,String> addedRotationTags = new HashMap<String,String>();
    //add new fields in rotation tags
    addedRotationTags.put("TestTag-3", "Updated-RotationTag-3");
    addedRotationTags.put("TestTag-4", "Updated-RotationTag-4");
    RotationInfo addedRInfo = new RotationInfo();
    addedRInfo.setRotation_tag(addedRotationTags);

    Response updateResult2 = client.target(svcEndPoint).path(UPDATE_PATH)
        .queryParam("rid", ridStr)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(addedRInfo, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, updateResult.getStatus());
    serviceResponse = updateResult2.readEntity(ServiceResponse.class);
    updateResponse = serviceResponse.getRotation_info();
    Assert.assertEquals("77", String.valueOf(updateResponse.getSite_id()));
    Assert.assertEquals("testing CampaignName", String.valueOf(updateResponse.getCampaign_name()));
    Assert.assertEquals("Updated-RotationTag-2", updateResponse.getRotation_tag().get("TestTag-2"));
    Assert.assertEquals("Updated-RotationTag-3", updateResponse.getRotation_tag().get("TestTag-3"));
    Assert.assertEquals("Updated-RotationTag-4", updateResponse.getRotation_tag().get("TestTag-4"));
  }

  @Test
  public void testChangeStatus() {
    //Create
    RotationInfo rotationRequest = getTestRotationInfo();
    Response createResult = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, createResult.getStatus());
    ServiceResponse serviceResponse = createResult.readEntity(ServiceResponse.class);
    RotationInfo createResponse = serviceResponse.getRotation_info();

    //Deactivate
    String ridStr = createResponse.getRotation_string();
    Response updateResult = client.target(svcEndPoint).path(DEACTIVATE_PATH)
        .queryParam("rid", ridStr)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, updateResult.getStatus());
    serviceResponse = updateResult.readEntity(ServiceResponse.class);
    RotationInfo updateResponse = serviceResponse.getRotation_info();
    Assert.assertEquals(ridStr, updateResponse.getRotation_string());
    Assert.assertEquals(RotationInfo.STATUS_INACTIVE, updateResponse.getStatus());
    Assert.assertEquals("6", String.valueOf(updateResponse.getChannel_id()));
    Assert.assertEquals(MPLXChannelEnum.EPN.getMplxChannelName(), updateResponse.getRotation_tag().get(RotationConstant.FIELD_TAG_CHANNEL_NAME));
    Assert.assertEquals("77", String.valueOf(updateResponse.getSite_id()));
    Assert.assertEquals("DE", updateResponse.getRotation_tag().get(RotationConstant.FIELD_TAG_SITE_NAME));

    //activate
    updateResult = client.target(svcEndPoint).path(ACTIVATE_PATH)
        .queryParam("rid", ridStr)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, updateResult.getStatus());
    serviceResponse = updateResult.readEntity(ServiceResponse.class);
    updateResponse = serviceResponse.getRotation_info();
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, updateResponse.getStatus());
  }

  @Test
  public void testGetById() {
    //Create
    RotationInfo rotationRequest = getTestRotationInfo();
    Response createResult = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, createResult.getStatus());
    ServiceResponse serviceResponse = createResult.readEntity(ServiceResponse.class);
    RotationInfo createResponse = serviceResponse.getRotation_info();

    //Get RotationInfo by rotation id
    Response getResult = client.target(svcEndPoint)
        .path("/tracksvc/v1/rid/get")
        .queryParam("rid", createResponse.getRotation_string())
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    Assert.assertEquals(200, createResult.getStatus());
    RotationInfo response = getResult.readEntity(ServiceResponse.class).getRotation_info();
    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getRotation_id());
    Assert.assertTrue(String.valueOf(response.getRotation_id()).length() == 16);
    Assert.assertEquals("CatherineTesting2018 RotationName", response.getRotation_name());
    Assert.assertEquals("http://prodigy.msn.com/es-mx/", response.getRotation_tag().get("vendor_url"));
  }

  @Test
  public void testGetByName() {
    //Create
    RotationInfo rotationRequest = getTestRotationInfo();
    rotationRequest.setRotation_name("testGetRotationName");
    Response createResult = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, createResult.getStatus());
    ServiceResponse serviceResponse = createResult.readEntity(ServiceResponse.class);
    RotationInfo createResponse = serviceResponse.getRotation_info();

    //Get RotationInfo by rotation id
    Response getResult = client.target(svcEndPoint)
        .path("/tracksvc/v1/rid/get")
        .queryParam("rname", "testGetRotationName")
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    Assert.assertEquals(200, createResult.getStatus());
    List<RotationInfo> rInfoList = getResult.readEntity(ServiceResponse.class).getRotation_info_list();
    Assert.assertTrue(rInfoList != null && rInfoList.size() > 0);
    Assert.assertEquals("testGetRotationName", rInfoList.get(0).getRotation_name());
  }

  @Test
  public void testValidationForChannelId() {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(null);
    Response result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    ServiceResponse response = result.readEntity(ServiceResponse.class);
    List<String> errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertEquals("No rotation info was created. [channel_id] is required field", errorList.get(0));

    rotationRequest.setChannel_id(-1);
    result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    response = result.readEntity(ServiceResponse.class);
    errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertEquals("No rotation info was created. [channel_id] can't be less than 0 or greater than 999", errorList.get(0));

    rotationRequest.setChannel_id(1234);
    result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    response = result.readEntity(ServiceResponse.class);
    errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertEquals("No rotation info was created. [channel_id] can't be less than 0 or greater than 999", errorList.get(0));
  }

  @Test
  public void testValidationForSiteId() {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(MPLXChannelEnum.EPN.getMplxChannelId());
    rotationRequest.setSite_id(null);
    Response result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    ServiceResponse response = result.readEntity(ServiceResponse.class);
    List<String> errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertEquals("No rotation info was created. [site_id] is required field", errorList.get(0));

    rotationRequest.setSite_id(-1);
    result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    response = result.readEntity(ServiceResponse.class);
    errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertEquals("No rotation info was created. [site_id] can't be less than 0 or greater than 999", errorList.get(0));

    rotationRequest.setSite_id(1234);
    result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    response = result.readEntity(ServiceResponse.class);
    errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertEquals("No rotation info was created. [site_id] can't be less than 0 or greater than 999", errorList.get(0));
  }

  @Test
  public void testValidationForCampaignId() {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(MPLXChannelEnum.PAID_SEARCH.getMplxChannelId());
    rotationRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaign_id(null);
    Response result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    ServiceResponse response = result.readEntity(ServiceResponse.class);
    List<String> errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertEquals("No rotation info was created. [campaign_id] is required field", errorList.get(0));

    rotationRequest.setCampaign_id(-1L);
    result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    response = result.readEntity(ServiceResponse.class);
    errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertEquals("No rotation info was created. [campaign_id] can't be less than 0 or greater than " + Long.MAX_VALUE, errorList.get(0));
  }

  @Test
  public void testValidationForVendorId() {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(MPLXChannelEnum.PAID_SEARCH.getMplxChannelId());
    rotationRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaign_id(5231L);
    rotationRequest.setVendor_id(-1);
    Response result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    ServiceResponse response = result.readEntity(ServiceResponse.class);
    List<String> errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertEquals("No rotation info was created. [vendor_id] can't be less than 0 or greater than " + Integer.MAX_VALUE, errorList.get(0));
  }


  @Test
  public void testValidationForRotationStartDate() {
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(MPLXChannelEnum.PAID_SEARCH.getMplxChannelId());
    rotationRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaign_id(5231L);
    rotationRequest.setVendor_id(1234);
    rotationRequest.setRotation_tag(new HashMap());
    rotationRequest.getRotation_tag().put(RotationConstant.FIELD_ROTATION_START_DATE, "2018-05");
    rotationRequest.getRotation_tag().put(RotationConstant.FIELD_ROTATION_END_DATE, "20A05");
    Response result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    ServiceResponse response = result.readEntity(ServiceResponse.class);
    List<String> errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertEquals("No rotation info was created. Please set correct [rotation_start_date]. Like: 20180501", errorList.get(0));
    Assert.assertEquals("No rotation info was created. Please set correct [rotation_end_date]. Like: 20180501", errorList.get(1));
  }

  @Test
  public void testGetCampaignById(){
    //Create RotationInfo
    RotationInfo rotationRequest = getTestRotationInfo();
    rotationRequest.setCampaign_id(RotationId.getNext().getRepresentation());
    rotationRequest.setCampaign_name("existedCampaign");
    Response result = client.target(svcEndPoint).path(CREATE_PATH)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    ServiceResponse createResponse = result.readEntity(ServiceResponse.class);
    Assert.assertNotNull(createResponse);

    //Get Existed Campaign ID
    Response getResult = client.target(svcEndPoint)
        .path("/tracksvc/v1/rid/getcamp")
        .queryParam("cid", rotationRequest.getCampaign_id())
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    Assert.assertEquals(200, getResult.getStatus());
    ServiceResponse svcResponse = getResult.readEntity(ServiceResponse.class);
    Assert.assertEquals(String.format(ErrorMsgConstant.CB_INSERT_CAMPAIGN_ISSUE,rotationRequest.getCampaign_id(),rotationRequest.getCampaign_name()),
        svcResponse.getMessage());
    Assert.assertEquals(svcResponse.getCampaign_info().getCampaign_id(), rotationRequest.getCampaign_id());
    Assert.assertEquals(svcResponse.getCampaign_info().getCampaign_name(), rotationRequest.getCampaign_name());

    //Get NOT Existed Campaign ID
    getResult = client.target(svcEndPoint)
        .path("/tracksvc/v1/rid/getcamp")
        .queryParam("cid", 99699979999L)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    Assert.assertEquals(200, getResult.getStatus());
    svcResponse = getResult.readEntity(ServiceResponse.class);
    Assert.assertEquals(ErrorMsgConstant.CB_INSERT_CAMPAIGN_INFO, svcResponse.getMessage());
    Assert.assertNotNull(svcResponse.getCampaign_info());
    Assert.assertFalse(svcResponse.getCampaign_info().getIsExisted());
  }
}
