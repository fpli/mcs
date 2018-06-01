package com.ebay.traffic.chocolate.mkttracksvc.resource;

import com.ebay.globalenv.SiteEnum;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.ServiceResponse;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
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
  private int port;

  @Test
  public void testCreate() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(1);
    rotationRequest.setSite_id(SiteEnum.EBAY_DE.getId());
    rotationRequest.setCampaign_id(1000000001L);
    rotationRequest.setCustomized_id1(30000002L);
    rotationRequest.setRotation_name("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationRequest.setRotation_tag(rotationTag);
    Response result = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    ServiceResponse serviceResponse = result.readEntity(ServiceResponse.class);
    RotationInfo rotationResponse = serviceResponse.getRotation_info();
    Assert.assertEquals("707-1000000001-30000002",
        rotationResponse.getRotation_id().substring(0, rotationResponse.getRotation_id().lastIndexOf("-")));
    Assert.assertEquals("1", String.valueOf(rotationResponse.getChannel_id()));
    Assert.assertEquals("77", String.valueOf(rotationResponse.getSite_id()));
    Assert.assertEquals("1000000001", String.valueOf(rotationResponse.getCampaign_id()));
    Assert.assertEquals("30000002", String.valueOf(rotationResponse.getCustomized_id1()));
    Assert.assertEquals("CatherineTesting RotationName", rotationResponse.getRotation_name());
    Assert.assertEquals("RotationTag-1", rotationResponse.getRotation_tag().get("TestTag-1"));
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, rotationResponse.getStatus());

  }

  @Test
  public void testUpdate() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    //Create
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(1);
    rotationRequest.setSite_id(77);
    rotationRequest.setCampaign_id(1000000001L);
    rotationRequest.setCustomized_id1(30000001L);
    rotationRequest.setRotation_name("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-2", "RotationTag-2");
    rotationRequest.setRotation_tag(rotationTag);
    Response createResult = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, createResult.getStatus());
    ServiceResponse serviceResponse = createResult.readEntity(ServiceResponse.class);
    RotationInfo createResponse = serviceResponse.getRotation_info();

    //Update
    String rid = createResponse.getRotation_id();
    rotationRequest.setRotation_name("UpdatedRotationName");
    rotationTag.put("TestTag-2", "Updated-RotationTag-2");

    Response updateResult = client.target(svcEndPoint).path("/tracksvc/v1/rid/update")
        .queryParam("rid", rid)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, updateResult.getStatus());
    serviceResponse = updateResult.readEntity(ServiceResponse.class);
    RotationInfo updateResponse = serviceResponse.getRotation_info();


    Assert.assertEquals(rid, updateResponse.getRotation_id());
    Assert.assertEquals("UpdatedRotationName", updateResponse.getRotation_name());
    Assert.assertEquals("Updated-RotationTag-2", updateResponse.getRotation_tag().get("TestTag-2"));
    Assert.assertEquals("1", String.valueOf(updateResponse.getChannel_id()));
    Assert.assertEquals("77", String.valueOf(updateResponse.getSite_id()));
    Assert.assertEquals("1000000001", String.valueOf(updateResponse.getCampaign_id()));
    Assert.assertEquals("30000001", String.valueOf(updateResponse.getCustomized_id1()));
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, updateResponse.getStatus());

    Map addedRotationTags = new HashMap();
    //add new fields in rotation tags
    addedRotationTags.put("TestTag-3", "Updated-RotationTag-3");
    addedRotationTags.put("TestTag-4", "Updated-RotationTag-4");
    RotationInfo addedRInfo = new RotationInfo();
    addedRInfo.setRotation_tag(addedRotationTags);

    Response updateResult2 = client.target(svcEndPoint).path("/tracksvc/v1/rid/update")
        .queryParam("rid", rid)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(addedRInfo, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, updateResult.getStatus());
    serviceResponse = updateResult2.readEntity(ServiceResponse.class);
    updateResponse = serviceResponse.getRotation_info();
    Assert.assertEquals("Updated-RotationTag-2", updateResponse.getRotation_tag().get("TestTag-2"));
    Assert.assertEquals("Updated-RotationTag-3", updateResponse.getRotation_tag().get("TestTag-3"));
    Assert.assertEquals("Updated-RotationTag-4", updateResponse.getRotation_tag().get("TestTag-4"));
  }

  @Test
  public void testChangeStatus() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    //Create
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(1);
    rotationRequest.setSite_id(77);
    rotationRequest.setCampaign_id(1000000001L);
    rotationRequest.setCustomized_id1(30000001L);
    rotationRequest.setRotation_name("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-2", "RotationTag-2");
    rotationRequest.setRotation_tag(rotationTag);
    Response createResult = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, createResult.getStatus());
    ServiceResponse serviceResponse = createResult.readEntity(ServiceResponse.class);
    RotationInfo createResponse = serviceResponse.getRotation_info();

    //Deactivate
    String rid = createResponse.getRotation_id();
    Response updateResult = client.target(svcEndPoint).path("/tracksvc/v1/rid/deactivate/")
        .queryParam("rid", rid)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, updateResult.getStatus());
    serviceResponse = updateResult.readEntity(ServiceResponse.class);
    RotationInfo updateResponse = serviceResponse.getRotation_info();
    Assert.assertEquals(rid, updateResponse.getRotation_id());
    Assert.assertEquals(RotationInfo.STATUS_INACTIVE, updateResponse.getStatus());
    Assert.assertEquals("CatherineTesting RotationName", updateResponse.getRotation_name());
    Assert.assertEquals("1", String.valueOf(updateResponse.getChannel_id()));
    Assert.assertEquals("77", String.valueOf(updateResponse.getSite_id()));
    Assert.assertEquals("1000000001", String.valueOf(updateResponse.getCampaign_id()));
    Assert.assertEquals("30000001", String.valueOf(updateResponse.getCustomized_id1()));

    //activate
    updateResult = client.target(svcEndPoint).path("/tracksvc/v1/rid/activate")
        .queryParam("rid", rid)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, updateResult.getStatus());
    serviceResponse = updateResult.readEntity(ServiceResponse.class);
    updateResponse = serviceResponse.getRotation_info();
    Assert.assertEquals(RotationInfo.STATUS_ACTIVE, updateResponse.getStatus());
  }

  @Test
  public void testGetById() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    //Create
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(1);
    rotationRequest.setSite_id(77);
    rotationRequest.setCampaign_id(50000002L);
    rotationRequest.setCustomized_id1(500000011L);
    rotationRequest.setRotation_name("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("vendor_name", "catherine_testing");
    rotationRequest.setRotation_tag(rotationTag);
    Response createResult = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, createResult.getStatus());
    ServiceResponse serviceResponse = createResult.readEntity(ServiceResponse.class);
    RotationInfo createResponse = serviceResponse.getRotation_info();

    //Get RotationInfo by rotation id
    Response getResult = client.target(svcEndPoint)
        .path("/tracksvc/v1/rid/get")
        .queryParam("rid", createResponse.getRotation_id())
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    Assert.assertEquals(200, createResult.getStatus());
    RotationInfo response = getResult.readEntity(ServiceResponse.class).getRotation_info();
    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getRid());
    Assert.assertTrue(String.valueOf(response.getRid()).length() == 18);
    Assert.assertEquals("CatherineTesting RotationName", response.getRotation_name());
    Assert.assertEquals("catherine_testing", response.getRotation_tag().get("vendor_name"));
  }

  @Test
  public void testGetByName() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    //Create
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(1);
    rotationRequest.setSite_id(77);
    rotationRequest.setCampaign_id(50000002L);
    rotationRequest.setCustomized_id1(30000011L);
    rotationRequest.setRotation_name("TestName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("vendor_name", "catherine_testing");
    rotationRequest.setRotation_tag(rotationTag);
    Response createResult = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, createResult.getStatus());
    ServiceResponse serviceResponse = createResult.readEntity(ServiceResponse.class);
    RotationInfo createResponse = serviceResponse.getRotation_info();

    //Get RotationInfo by rotation id
    Response getResult = client.target(svcEndPoint)
        .path("/tracksvc/v1/rid/get")
        .queryParam("rname", "TestName")
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    Assert.assertEquals(200, createResult.getStatus());
    List<RotationInfo> rInfoList = getResult.readEntity(ServiceResponse.class).getRotation_info_list();
    Assert.assertTrue(rInfoList != null && rInfoList.size() > 0);
    Assert.assertEquals("TestName", rInfoList.get(0).getRotation_name());
//    Assert.assertEquals("catherine_testing", rInfoList.get(0).getRotation_tag().get("vendor_name"));
  }

  @Test
  public void testValidation() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    RotationInfo rotationRequest = new RotationInfo();
    Response result = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    ServiceResponse response = result.readEntity(ServiceResponse.class);
    List<String> errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertTrue(errorList.get(0).contains("channel_id"));
    Assert.assertTrue(errorList.get(1).contains("site_id"));
    Assert.assertTrue(errorList.get(2).contains("campaign_id"));

    //XC-816
    rotationRequest = new RotationInfo();
    rotationRequest.setChannel_id(12345);
    rotationRequest.setSite_id(12345);
    rotationRequest.setCampaign_id(-1L);
    rotationRequest.setCustomized_id1(-2L);
    result = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    response = result.readEntity(ServiceResponse.class);
    errorList = response.getErrors();
    Assert.assertNotNull(errorList);
    Assert.assertTrue(errorList.get(0).contains("campaign_id"));
  }

  @Test
  public void testResponseMessages() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setSite_id(77);
    rotationRequest.setChannel_id(4);
    rotationRequest.setCampaign_id(12345L);
    rotationRequest.setCustomized_id1(45523L);
    rotationRequest.setCustomized_id2(15523L);
    rotationRequest.setRotation_name("test messages rotation");
    Response result = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());

    result = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));

    ServiceResponse response = result.readEntity(ServiceResponse.class);
    Assert.assertNotNull(response);
    Assert.assertNotNull(response.getMessage());
    Assert.assertTrue( response.getMessage().contains("Can't create new rotation id, since rotationId already existed"));

  }
}
