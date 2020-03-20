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
import org.apache.commons.collections.MapUtils;
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
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.HashMap;
import java.util.Map;

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

  private Response getAdserviceResponse(String path, Map<String, String> parameters) {
    WebTarget webTarget = client.target(svcEndPoint).path(path);

    // add parameters
    if (MapUtils.isNotEmpty(parameters)) {
      for (Map.Entry<String, String> entry : parameters.entrySet()) {
        webTarget = webTarget.queryParam(entry.getKey(), entry.getValue());
      }
    }

    return webTarget.request().accept(MediaType.APPLICATION_JSON_TYPE).get();
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
  public void redirect() {
    // Site email parameters
    Map<String, String> parameters = new HashMap<>();
    parameters.put("mkevt", "1");
    parameters.put("mkcid", "7");
    parameters.put("mkpid", "0");
    parameters.put("emsid", "e11051.m44.l1139");
    parameters.put("mpre", "https://maps.google.com?q=51.8844429227,-0.1708975228");
    parameters.put("sojTags", "bu%3Dbu");
    parameters.put("bu", "43551630917");
    parameters.put("euid", "c527526a795a414cb4ad11bfaba21b5d");
    parameters.put("ext", "56623");

    // Common site email redirect
    Response response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://maps.google.com?q=51.8844429227,-0.1708975228", response.getLocation().toString());

    // Marketing email parameters
    parameters.clear();
    parameters.put("mkevt", "1");
    parameters.put("mkcid", "8");
    parameters.put("mkpid", "12");
    parameters.put("emsid", "e11051.m44.l1139");
    parameters.put("mpre", "https://www.yahoo.com");
    parameters.put("sojTags", "bu%3Dbu");
    parameters.put("bu", "43551630917");
    parameters.put("crd", "20190801034425");
    parameters.put("segname", "AD379737195_GBH_BBDBENNEWROW_20180813_ZK");
    parameters.put("ymmmid", "1740915");
    parameters.put("yminstc", "7");

    // Common marketing email redirect, full hostname
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.yahoo.com", response.getLocation().toString());

    // Partial hostname
    parameters.replace("mpre", "https://www.youtube.com/watch?v=T8_7VcGFFoA");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.youtube.com/watch?v=T8_7VcGFFoA", response.getLocation().toString());

    // Valid Protocol
    parameters.replace("mpre", "ebaydeals://aaa");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("ebaydeals://aaa", response.getLocation().toString());

    // Infinite redirect, redirect to home page
    parameters.replace("mpre", "https://www.ebayadservices.com/marketingtracking/v1/redirect");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.com/", response.getLocation().toString());

    // Empty landing page, redirect to home page
    parameters.replace("mpre", "");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.com/", response.getLocation().toString());

    // No mkevt, redirect to home page
    parameters.remove("mkevt");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.com/", response.getLocation().toString());

    // Invalid mkevt, redirect to home page
    parameters.put("mkevt", "2");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.com/", response.getLocation().toString());
    parameters.replace("mkevt", "1");

    // No mkcid, redirect to home page
    parameters.remove("mkcid");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.com/", response.getLocation().toString());

    // Invalid mkcid, redirect to home page
    parameters.put("mkcid", "999");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.com/", response.getLocation().toString());
    parameters.replace("mkcid", "8");

    // No mkpid, redirect to home page
    parameters.remove("mkpid");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.com/", response.getLocation().toString());

    // Invalid mkpid, redirect successfully
    parameters.put("mkpid", "999");
    parameters.replace("mpre", "https://www.yahoo.com");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.yahoo.com", response.getLocation().toString());
    parameters.replace("mkpid", "12");

    // No query parameters, redirect to home page
    parameters.clear();
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.com/", response.getLocation().toString());

    // Adobe parameters
    parameters.clear();
    parameters.put("mkevt", "1");
    parameters.put("mkcid", "8");
    parameters.put("mkpid", "14");
    parameters.put("emsid", "0");
    parameters.put("sojTags", "adcampid%id%adcamppu%pu%crd%crd%segname%segname");
    parameters.put("id", "h1d3e4e16,2d2cb515,2d03a0a1");
    parameters.put("segname", "SOP708_SG49");
    parameters.put("pu", "hrtHY5sgRPq");
    parameters.put("country", "US");
    parameters.put("adobeParams", "id,p1,p2,p3,p4");

    // Adobe landing page from adobe server
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.de/deals", response.getLocation().toString());

    // Adobe landing page from parameter
    parameters.put("mpre", "https://www.yahoo.com");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.yahoo.com", response.getLocation().toString());
    parameters.remove("mpre");

    // Adobe server fail, redirect to home page by country
    parameters.replace("id", "h1d3e4dcb,2d1b8f79,1");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.com/", response.getLocation().toString());

    // Adobe without country, redirect to home page
    parameters.remove("country");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.com/", response.getLocation().toString());
    parameters.put("country", "US");
    parameters.put("id", "h1d3e4e16,2d2cb515,2d03a0a1");

    // Adobe without adobeParams, redirect successfully
    parameters.remove("adobeParams");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.de/deals", response.getLocation().toString());
    parameters.put("adobeParams", "id,p1,p2,p3,p4");

    // Adobe without id, redirect to home page by country
    parameters.remove("id");
    parameters.replace("country", "DE");
    response = getAdserviceResponse(REDIRECT_PATH, parameters);
    assertEquals(301, response.getStatus());
    assertEquals("https://www.ebay.de/", response.getLocation().toString());
    parameters.put("id", "h1d3e4e16,2d2cb515,2d03a0a1");
    parameters.replace("country", "US");
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
  public void impression() {
    // Site email parameters
    Map<String, String> parameters = new HashMap<>();
    parameters.put("mkevt", "4");
    parameters.put("mkcid", "7");
    parameters.put("mkpid", "0");
    parameters.put("emsid", "e11051.m44.l1139");
    parameters.put("sojTags", "bu%3Dbu");
    parameters.put("bu", "43551630917");
    parameters.put("euid", "c527526a795a414cb4ad11bfaba21b5d");
    parameters.put("ext", "56623");

    // Common site email open
    Response response = getAdserviceResponse(IMPRESSION_PATH, parameters);
    assertEquals(200, response.getStatus());

    // Marketing email parameters
    parameters.clear();
    parameters.put("mkevt", "4");
    parameters.put("mkcid", "8");
    parameters.put("mkpid", "12");
    parameters.put("emsid", "e11051.m44.l1139");
    parameters.put("sojTags", "bu%3Dbu");
    parameters.put("bu", "43551630917");
    parameters.put("crd", "20190801034425");
    parameters.put("segname", "AD379737195_GBH_BBDBENNEWROW_20180813_ZK");
    parameters.put("ymmmid", "1740915");
    parameters.put("yminstc", "7");

    // Common marketing email open
    response = getAdserviceResponse(IMPRESSION_PATH, parameters);
    assertEquals(200, response.getStatus());

    // No mkevt
    parameters.remove("mkevt");
    response = getAdserviceResponse(IMPRESSION_PATH, parameters);
    assertEquals(200, response.getStatus());

    // Adobe parameters
    parameters.clear();
    parameters.put("mkevt", "4");
    parameters.put("mkcid", "8");
    parameters.put("mkpid", "14");
    parameters.put("emsid", "0");
    parameters.put("sojTags", "adcampid%id%adcamppu%pu%crd%crd%segname%segname");
    parameters.put("id", "h1d3e4e16,2d2cb515,2d03a0a1");
    parameters.put("segname", "SOP708_SG49");
    parameters.put("pu", "hrtHY5sgRPq");
    parameters.put("country", "US");
    parameters.put("adobeParams", "id,p1,p2,p3,p4");

    // Adobe open
    response = getAdserviceResponse(IMPRESSION_PATH, parameters);
    assertEquals(200, response.getStatus());

    // Adobe without adobeParams
    parameters.remove("adobeParams");
    response = getAdserviceResponse(IMPRESSION_PATH, parameters);
    assertEquals(200, response.getStatus());
    parameters.put("adobeParams", "id,p1,p2,p3,p4");
  }
}