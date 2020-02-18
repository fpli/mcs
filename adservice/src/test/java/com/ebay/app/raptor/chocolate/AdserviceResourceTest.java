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
import org.apache.http.client.utils.URIBuilder;
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
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.net.URISyntaxException;

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

public class AdserviceResourceTest {

  @LocalServerPort
  private int port;

  @Inject
  private ISecureTokenManager tokenGenerator;

  private boolean initialized = false;

  private Client client;
  private String svcEndPoint;

  private final String arPath = "/marketingtracking/v1/ar";
  private final String impressionPath = "/marketingtracking/v1/impression";
  private final String redirectPath = "/marketingtracking/v1/redirect";
  private final String syncPath = "/marketingtracking/v1/sync";
  private final String guidPath = "/marketingtracking/v1/guid";
  private final String useridPath = "/marketingtracking/v1/userid";
  private final String adobeOpenPath = "/marketingtracking/v1/impression?mkevt=4&mkcid=7&mkpid=14&id=h1d3e4dcb,2d1b8f79,1&segname=SOP708_SG49&country=US&pu=hrtHY5sgRPq&crd=20200110015225&sojTags=adcampid%id%adcamppu%pu%crd%crd%segname%segname&adobeParams=id,p1,p2,p3,p4";


  @Autowired
  private CollectionService collectionService;

  @BeforeClass
  public static void initBeforeTest() throws Exception {
    ApplicationOptions options = ApplicationOptions.getInstance();
  }

  @Before
  public void setUp() {
    if (!initialized) {
      RuntimeContext.setConfigRoot(AdserviceResourceTest.class.getClassLoader().getResource
          ("META-INF/configuration/Dev/"));
      Configuration configuration = ConfigurationBuilder.newConfig("testService.testClient");
      client = ClientBuilder.newClient(configuration);
      String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
      svcEndPoint = endpoint + ":" + port;

      initialized = true;
    }
  }

  @Test
  public void ar() {
//    Response response = client.target(svcEndPoint).path(arPath+"?siteId=0&ff8=2600242&ff9=max&adm=1&gbh=10022&adtype=2&size=300x600&pgroup=459125&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&rvr_id=3223821930815456&ZipCode=default&cguid=aeeee67816d0a4d0bb744efef26d8f0a&guid=a5283d6816c0a99b6de1b3aafcbad5af")
//        .request()
//        .accept(MediaType.APPLICATION_JSON_TYPE)
//        .get();
//    assertEquals(200, response.getStatus());
  }

  @Test
  public void impression() {
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

  @Test
  public void sync() {
    SyncEvent syncEvent = new SyncEvent();
    syncEvent.setGuid("abcd-efgh");
    syncEvent.setUserid("12345");
    Response syncResponse = client.target(svcEndPoint).path(syncPath)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .post(Entity.json(syncEvent));
    assertEquals(200, syncResponse.getStatus());

    Response guidResponse = client.target(svcEndPoint).path(guidPath)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(200, guidResponse.getStatus());

    Response useridResponse = client.target(svcEndPoint).path(useridPath)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(200, useridResponse.getStatus());
  }

  @Test
  public void guid() {
    Response response = client.target(svcEndPoint).path(guidPath)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(200, response.getStatus());

  }

  @Test
  public void userid() {
    Response response = client.target(svcEndPoint).path(useridPath)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(200, response.getStatus());

  }

  @Test
  public void adobeOpen() throws URISyntaxException {
    URIBuilder uriBuilder = new URIBuilder(svcEndPoint+impressionPath)
        .addParameter("mkevt","4")
        .addParameter("mkcid","7")
        .addParameter("mkpid","14")
        .addParameter("id","h1d3e4dcb,2d1b8f79,1")
        .addParameter("segname","SOP708_SG49")
        .addParameter("country","US")
        .addParameter("pu","hrtHY5sgRPq")
        .addParameter("sojTags","adcampid%id%adcamppu%pu%crd%crd%segname%segname")
        .addParameter("adobeParams","id,p1,p2,p3,p4");
    Response response = client.target(uriBuilder.build())
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(200, response.getStatus());
  }
}