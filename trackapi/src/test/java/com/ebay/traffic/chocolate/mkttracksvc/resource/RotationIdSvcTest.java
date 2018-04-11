package com.ebay.traffic.chocolate.mkttracksvc.resource;

import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.ServiceResponse;
import org.junit.Assert;
import org.junit.Ignore;
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
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {"GingerClient.testService.testClient.endpointUri=http://localhost",
        "GingerClient.testService.testClient.readTimeout=5000"})
@Ignore
public class RotationIdSvcTest {
  @LocalServerPort
  private int port;

  @Test
  public void testCreate() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannelId(1);
    rotationRequest.setSiteId(77);
    rotationRequest.setCampaignId("000000001");
    rotationRequest.setCustomizedId("000000002");
    rotationRequest.setRotationName("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-1", "RotationTag-1");
    rotationRequest.setRotationTag(rotationTag);
    Response result = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    ServiceResponse serviceResponse = result.readEntity(ServiceResponse.class);
    RotationInfo rotationResponse = serviceResponse.getRotationInfo();
    Assert.assertEquals("0010077-000000001-000000002", rotationResponse.getRotationId().substring(0, rotationResponse.getRotationId().lastIndexOf("-")));
    Assert.assertEquals("1", String.valueOf(rotationResponse.getChannelId()));
    Assert.assertEquals("77", String.valueOf(rotationResponse.getSiteId()));
    Assert.assertEquals("000000001", rotationResponse.getCampaignId());
    Assert.assertEquals("000000002", rotationResponse.getCustomizedId());
    Assert.assertEquals("CatherineTesting RotationName", rotationResponse.getRotationName());
    Assert.assertEquals("RotationTag-1", rotationResponse.getRotationTag().get("TestTag-1"));
  }

  @Test
  public void testUpdate() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    //Create
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannelId(1);
    rotationRequest.setSiteId(77);
    rotationRequest.setCampaignId("000000001");
    rotationRequest.setCustomizedId("000000002");
    rotationRequest.setRotationName("CatherineTesting RotationName");
    Map<String, String> rotationTag = new HashMap<String, String>();
    rotationTag.put("TestTag-2", "RotationTag-2");
    rotationRequest.setRotationTag(rotationTag);
    Response createResult = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, createResult.getStatus());
    ServiceResponse serviceResponse = createResult.readEntity(ServiceResponse.class);
    RotationInfo createResponse = serviceResponse.getRotationInfo();

    //Update
    String rid = createResponse.getRotationId();
    rotationRequest.setRotationName("UpdatedRotationName");
    rotationTag.put("TestTag-2", "Updated-RotationTag-2");

    Response updateResult = client.target(svcEndPoint).path("/tracksvc/v1/rid/update/" + rid)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, updateResult.getStatus());
    serviceResponse = updateResult.readEntity(ServiceResponse.class);
    RotationInfo updateResponse = serviceResponse.getRotationInfo();


    Assert.assertEquals(rid, updateResponse.getRotationId());
    Assert.assertEquals("UpdatedRotationName", updateResponse.getRotationName());
    Assert.assertEquals("Updated-RotationTag-2", updateResponse.getRotationTag().get("TestTag-2"));
    Assert.assertEquals("1", String.valueOf(updateResponse.getChannelId()));
    Assert.assertEquals("77", String.valueOf(updateResponse.getSiteId()));
    Assert.assertEquals("000000001", updateResponse.getCampaignId());
    Assert.assertEquals("000000002", updateResponse.getCustomizedId());
  }

  @Test
  public void testChangeStatus() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    //Create
    RotationInfo rotationRequest = new RotationInfo();
    rotationRequest.setChannelId(1);
    rotationRequest.setSiteId(77);
    rotationRequest.setCampaignId("000000001");
    rotationRequest.setCustomizedId("000000002");
    rotationRequest.setRotationName("CatherineTesting RotationName");
    Response result = client.target(svcEndPoint).path("/tracksvc/v1/rid/create")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, result.getStatus());
    ServiceResponse serviceResponse = result.readEntity(ServiceResponse.class);
    RotationInfo createResponse = serviceResponse.getRotationInfo();

    //Deactivate
    String rid = createResponse.getRotationId();
    Response updateResult = client.target(svcEndPoint).path("/tracksvc/v1/rid/deactivate/" + rid)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, updateResult.getStatus());
    serviceResponse = updateResult.readEntity(ServiceResponse.class);
    RotationInfo updateResponse = serviceResponse.getRotationInfo();
    Assert.assertEquals(rid, updateResponse.getRotationId());
    Assert.assertFalse(updateResponse.getActive());
    Assert.assertEquals("CatherineTesting RotationName", updateResponse.getRotationName());
    Assert.assertEquals("1", String.valueOf(updateResponse.getChannelId()));
    Assert.assertEquals("77", String.valueOf(updateResponse.getSiteId()));
    Assert.assertEquals("000000001", updateResponse.getCampaignId());
    Assert.assertEquals("000000002", updateResponse.getCustomizedId());

    //activate
    updateResult = client.target(svcEndPoint).path("/tracksvc/v1/rid/activate/" + rid)
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .put(Entity.entity(rotationRequest, MediaType.APPLICATION_JSON_TYPE));
    Assert.assertEquals(200, updateResult.getStatus());
    serviceResponse = updateResult.readEntity(ServiceResponse.class);
    updateResponse = serviceResponse.getRotationInfo();
    Assert.assertTrue(updateResponse.getActive());
  }
}
