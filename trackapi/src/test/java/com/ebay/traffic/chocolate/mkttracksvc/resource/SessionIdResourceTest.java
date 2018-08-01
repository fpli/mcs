package com.ebay.traffic.chocolate.mkttracksvc.resource;

import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
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
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {"GingerClient.testService.testClient.endpointUri=http://localhost",
        "GingerClient.testService.testClient.readTimeout=5000"})
@Ignore
public class SessionIdResourceTest {
  @LocalServerPort
  private int port;

  @Test
  public void testGetSnid() {
    Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
    Client client = ClientBuilder.newClient(configuration);
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    String svcEndPoint = endpoint + ":" + port;

    Response result = client.target(svcEndPoint).path("/tracksvc/v1/snid/getSnid")
        .request().accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    Assert.assertEquals(200, result.getStatus());
  }

}
