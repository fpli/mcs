/**
 * Created by xiangli4 on 02/03/20.
 * End to End test for Adservice. This class uses Spring test framework to
 * start the test web service, and uses Mini Kafka.
 */

package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.CollectionService;
import com.ebay.app.raptor.chocolate.gen.model.SyncEvent;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.platform.raptor.cosadaptor.token.ISecureTokenManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit4.SpringRunner;

import javax.inject.Inject;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "ginger-client.testService.testClient.endpointUri=http://localhost",
        "ginger-client.testService.testClient.connectTimeout=60000",
        "ginger-client.testService.testClient.readTimeout=60000"
    },
    classes = AdserviceApplication.class)

public class AdobeSolutionTest {

  @LocalServerPort
  private int port;

  @Inject
  private ISecureTokenManager tokenGenerator;

  private boolean initialized = false;

  private Client client;
  private String svcEndPoint;

  private final String arPath = "/marketingtracking/v1/ar";
  private final String impressionPath = "/marketingtracking/v1/impression?mkevt=4&mkcid=7&mkpid=14&id=h1d3e4dcb,2d1b8f79,1&segname=SOP708_SG49&country=US&pu=hrtHY5sgRPq&crd=20200110015225&sojTags=adcampid%id%adcamppu%pu%crd%crd%segname%segname&adobeParams=id,p1,p2,p3,p4";
  private final String redirectPath = "/marketingtracking/v1/redirect";
  private final String syncPath = "/marketingtracking/v1/sync";
  private final String guidPath = "/marketingtracking/v1/guid";

  @Autowired
  private CollectionService collectionService;

  @BeforeClass
  public static void initBeforeTest() throws Exception {
    ApplicationOptions options = ApplicationOptions.getInstance();
  }

  @Before
  public void setUp() {
    if (!initialized) {
      RuntimeContext.setConfigRoot(AdobeSolutionTest.class.getClassLoader().getResource
          ("META-INF/configuration/Dev/"));
      Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
      client = ClientBuilder.newClient(configuration);
      String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
      svcEndPoint = endpoint + ":" + port;

      initialized = true;
    }
  }


  @Test
  public void adobeOpen() {
    Response response = client.target(svcEndPoint).path(impressionPath)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(200, response.getStatus());
  }

  @Test
  public void redirect() {
    Response response = client.target(svcEndPoint).path(redirectPath)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(301, response.getStatus());
  }

}