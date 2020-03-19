/**
 * Created by xiangli4 on 02/03/20.
 * End to End test for Adservice. This class uses Spring test framework to
 * start the test web service, and uses Mini Kafka.
 */

package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.CollectionService;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.platform.raptor.cosadaptor.token.ISecureTokenManager;
import org.apache.commons.lang3.StringUtils;
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

  private static final String AR_PATH = "/marketingtracking/v1/ar";
  private static final String IMPRESSION_PATH = "/marketingtracking/v1/impression";
  private static final String REDIRECT_PATH = "/marketingtracking/v1/redirect";
  private static final String SYNC_PATH = "/marketingtracking/v1/sync";
  private static final String GUID_PATH = "/marketingtracking/v1/guid";
  private static final String USERID_PATH = "/marketingtracking/v1/uid";

  private static final String CLICK = "1";
  private static final String OPEN = "4";

  private static final String SITE_EMAIL = "7";
  private static final String MKT_EMAIL = "8";

  private static final String INTERNAL = "0";
  private static final String YESMAIL = "12";
  private static final String ADOBE = "14";

  @Autowired
  private CollectionService collectionService;

  @BeforeClass
  public static void initBeforeTest() {
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

  private Response buildUrlAndResponse(String mkevt, String mkcid, String mkpid, String landingPage)
      throws URISyntaxException {
    URIBuilder uriBuilder;

    // email click
    if (mkevt == CLICK) {
      uriBuilder = new URIBuilder(svcEndPoint + REDIRECT_PATH)
          .addParameter("mpre", landingPage);
    }
    // email open
    else {
      uriBuilder = new URIBuilder(svcEndPoint + IMPRESSION_PATH);
    }

    uriBuilder = uriBuilder.addParameter("mkevt", mkevt)
        .addParameter("mkcid", mkcid)
        .addParameter("mkpid", mkpid);

    // Add parameters
    // site email
    if (mkcid == SITE_EMAIL) {
      uriBuilder = uriBuilder.addParameter("emsid", "e11051.m44.l1139")
          .addParameter("sojTags", "bu%3Dbu")
          .addParameter("bu", "43551630917")
          .addParameter("euid", "c527526a795a414cb4ad11bfaba21b5d")
          .addParameter("ext", "56623");
    }
    // marketing email
    else if (mkcid == MKT_EMAIL){
      uriBuilder = uriBuilder.addParameter("emsid", "e11051.m44.l1139")
          .addParameter("sojTags", "bu%3Dbu")
          .addParameter("bu", "43551630917")
          .addParameter("crd", "20190801034425")
          .addParameter("segname", "AD379737195_GBH_BBDBENNEWROW_20180813_ZK")
          .addParameter("ymmmid", "1740915")
          .addParameter("yminstc", "7");
    }

    return client.target(uriBuilder.build())
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
  }

  private Response buildUrlAndResponse(String mkevt, String landingPage, String id, Boolean country,
                                       Boolean adobeParams) throws URISyntaxException {
    URIBuilder uriBuilder;

    // email click
    if (mkevt == CLICK) {
      uriBuilder = new URIBuilder(svcEndPoint + REDIRECT_PATH)
          .addParameter("mpre", landingPage);
    }
    // email open
    else {
      uriBuilder = new URIBuilder(svcEndPoint + IMPRESSION_PATH);
    }

    uriBuilder = uriBuilder.addParameter("mkevt", mkevt)
        .addParameter("mkcid", MKT_EMAIL)
        .addParameter("mkpid", ADOBE)
        .addParameter("id", id)
        .addParameter("segname", "SOP708_SG49")
        .addParameter("pu", "hrtHY5sgRPq")
        .addParameter("sojTags", "adcampid%id%adcamppu%pu%crd%crd%segname%segname");

    if (country) {
      uriBuilder = uriBuilder.addParameter("country", "US");
    }

    if (adobeParams) {
      uriBuilder = uriBuilder.addParameter("adobeParams", "id,p1,p2,p3,p4");
    }

    return client.target(uriBuilder.build())
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
  }

  @Test
  public void ar() {
//    Response response = client.target(svcEndPoint).path(AR_PATH+"?siteId=0&ff8=2600242&ff9=max&adm=1&gbh=10022&adtype=2&size=300x600&pgroup=459125&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&rvr_id=3223821930815456&ZipCode=default&cguid=aeeee67816d0a4d0bb744efef26d8f0a&guid=a5283d6816c0a99b6de1b3aafcbad5af")
//        .request()
//        .accept(MediaType.APPLICATION_JSON_TYPE)
//        .get();
//    assertEquals(200, response.getStatus());
  }

  @Test
  public void redirect() throws URISyntaxException {
    // Common site email redirect
    Response response = buildUrlAndResponse(CLICK, SITE_EMAIL, INTERNAL,
        "https://maps.google.com?q=51.8844429227,-0.1708975228");
    assertEquals(301, response.getStatus());

    // Common marketing email redirect
    response = buildUrlAndResponse(CLICK, MKT_EMAIL, YESMAIL, "http://www.yahoo.com");
    assertEquals(301, response.getStatus());

    // Partial hostname
    response = buildUrlAndResponse(CLICK, MKT_EMAIL, YESMAIL, "https://www.youtube.com/watch?v=T8_7VcGFFoA");
    assertEquals(301, response.getStatus());

    // Valid Protocol
    response = buildUrlAndResponse(CLICK, MKT_EMAIL, YESMAIL, "ebaydeals://");
    assertEquals(301, response.getStatus());

    // Adobe landing page from adobe server
    response = buildUrlAndResponse(CLICK, null, "h1d3e4e16,2d2cb515,2d03a0a1", true, true);
    assertEquals(301, response.getStatus());

    // Adobe landing page from parameter
    response = buildUrlAndResponse(CLICK, "http://www.yahoo.com", "h1d3e4e16,2d2cb515,2d03a0a1", true, true);
    assertEquals(301, response.getStatus());

    // Adobe home page
    response = buildUrlAndResponse(CLICK, null, "h1d3e4dcb,2d1b8f79,1", true, true);
    assertEquals(301, response.getStatus());

    // Adobe without country
    response = buildUrlAndResponse(CLICK, null, "h1d3e4e16,2d2cb515,2d03a0a1", false, true);
    assertEquals(301, response.getStatus());

    // Adobe without adobeParams
    response = buildUrlAndResponse(CLICK, null, "h1d3e4e16,2d2cb515,2d03a0a1", true, false);
    assertEquals(301, response.getStatus());
  }

  @Test
  public void sync() {
    Response syncResponse = client.target(svcEndPoint).path(SYNC_PATH)
        .queryParam("guid", "abcd")
        .queryParam("uid", "12345")
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(200, syncResponse.getStatus());

    Response guidResponse = client.target(svcEndPoint).path(GUID_PATH)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(200, guidResponse.getStatus());

    Response useridResponse = client.target(svcEndPoint).path(USERID_PATH)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(200, useridResponse.getStatus());
  }

  @Test
  public void guid() {
    Response response = client.target(svcEndPoint).path(GUID_PATH)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(200, response.getStatus());

  }

  @Test
  public void userid() {
    Response response = client.target(svcEndPoint).path(USERID_PATH)
        .request()
        .accept(MediaType.APPLICATION_JSON_TYPE)
        .get();
    assertEquals(200, response.getStatus());

  }

  @Test
  public void impression() throws URISyntaxException {
    // Common site email open
    Response response = buildUrlAndResponse(OPEN, SITE_EMAIL, INTERNAL, null);
    assertEquals(200, response.getStatus());

    // Common marketing email open
    response = buildUrlAndResponse(OPEN, MKT_EMAIL, YESMAIL, null);
    assertEquals(200, response.getStatus());

    // Adobe open
    response = buildUrlAndResponse(OPEN, null, "h1d3e4e16,2d2cb515,2d03a0a1", true, true);
    assertEquals(200, response.getStatus());

    // Adobe without adobeParams
    response = buildUrlAndResponse(OPEN, null, "h1d3e4e16,2d2cb515,2d03a0a1", true, false);
    assertEquals(200, response.getStatus());
  }
}