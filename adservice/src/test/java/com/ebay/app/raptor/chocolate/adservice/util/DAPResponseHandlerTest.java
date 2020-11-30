package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.AdserviceResourceTest;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.LBSConstants;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSClient;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSHttpResult;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSQueryResult;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSResults;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.kernel.constants.KernelConstants;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.kernel.util.FastURLEncoder;
import com.ebay.traffic.monitoring.ESMetrics;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.*;
import javax.ws.rs.core.*;
import java.io.BufferedReader;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;

/**
 * This class
 *
 * @author Zhiyuan Wang
 * @since 2020/3/25
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(GingerClientBuilder.class)
@PowerMockIgnore("javax.net.ssl.*")
public class DAPResponseHandlerTest {
  private LBSClient lbsClient;

  DAPResponseHandler dapResponseHandler;

  @BeforeClass
  public static void beforeClass() throws Exception {
    RuntimeContext.setConfigRoot(AdserviceResourceTest.class.getClassLoader().getResource("META-INF/configuration/Dev/"));
    ESMetrics.init("test", "localhost");
  }

  @Before
  public void setUp() throws Exception {
    dapResponseHandler = new DAPResponseHandler();
  }

  @Test
  public void getSiteId() throws Exception {
    Client client = Mockito.mock(Client.class);
    Configuration conf = Mockito.mock(Configuration.class);
    Response response = prepareResponse();
    WebTarget webTarget = Mockito.mock(WebTarget.class);
    Invocation.Builder builder = Mockito.mock(Invocation.Builder.class);

    when(conf.getProperty(EndpointUri.KEY)).thenReturn("localhost");
    when(builder.get()).thenReturn(response);
    when(webTarget.queryParam(anyString(), anyString())).thenReturn(webTarget);
    when(webTarget.request(MediaType.APPLICATION_JSON)).thenReturn(builder);
    when(client.target(anyString())).thenReturn(webTarget);
    when(client.getConfiguration()).thenReturn(conf);

    lbsClient = LBSClient.getInstance();
    Whitebox.setInternalState(lbsClient, "client", client);

    LBSQueryResult lbsQueryResult = lbsClient.getLBSInfo("97.77.104.22");
    assertEquals(Integer.valueOf(0), Whitebox.<Integer>invokeMethod(dapResponseHandler, "getSiteId", lbsQueryResult));
  }

  @Test
  public void setSiteId() throws Exception {
    URIBuilder dapUriBuilder = new URIBuilder();
    Whitebox.<String>invokeMethod(dapResponseHandler, "setSiteId", dapUriBuilder, 2);
    List<NameValuePair> collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.SITE_ID))
        .collect(Collectors.toList());
    assertEquals("2", collect.get(0).getValue());
  }

  @Test
  public void getUaPrime() throws Exception {
    Map<String, String[]> map = new HashMap<>();
    assertNull(Whitebox.<Integer>invokeMethod(dapResponseHandler, "getUaPrime", map));

    map.put(Constants.UA_PARAM, null);
    assertNull(Whitebox.<Integer>invokeMethod(dapResponseHandler, "getUaPrime", map));

    map.put(Constants.UA_PARAM, new String[0]);
    assertNull(Whitebox.<Integer>invokeMethod(dapResponseHandler, "getUaPrime", map));

    map.put(Constants.UA_PARAM, new String[]{"test"});
    assertEquals("test", Whitebox.<String>invokeMethod(dapResponseHandler, "getUaPrime", map));
  }

  @Test
  public void getRemoteIp() throws Exception {
    HttpServletRequest httpServletRequest = Mockito.mock(HttpServletRequest.class);
    when(httpServletRequest.getHeader("X-Forwarded-For")).thenReturn("1,2");
    assertEquals("1", Whitebox.<String>invokeMethod(dapResponseHandler, "getRemoteIp", httpServletRequest));

    when(httpServletRequest.getHeader("X-Forwarded-For")).thenReturn(null);
    when(httpServletRequest.getRemoteAddr()).thenReturn("10.10");
    assertEquals("10.10", Whitebox.<String>invokeMethod(dapResponseHandler, "getRemoteIp", httpServletRequest));
  }

  @Test
  public void setHLastLoggedInUserId() throws Exception {
    URIBuilder dapUriBuilder = new URIBuilder();
    Whitebox.<String>invokeMethod(dapResponseHandler, "setHLastLoggedInUserId", dapUriBuilder, "123");
    List<NameValuePair> collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.H_LAST_LOGGED_IN_USER_ID))
        .collect(Collectors.toList());
    assertEquals("123", collect.get(0).getValue());
  }

  @Test
  public void getHLastLoggedInUserId() throws Exception {
    assertNull(Whitebox.<String>invokeMethod(dapResponseHandler,
        "getHLastLoggedInUserId", ""));
    assertNull(Whitebox.<String>invokeMethod(dapResponseHandler,
        "getHLastLoggedInUserId", "xx"));
    assertNull(Whitebox.<String>invokeMethod(dapResponseHandler,
        "getHLastLoggedInUserId", "0"));
    assertNull(Whitebox.<String>invokeMethod(dapResponseHandler,
        "getHLastLoggedInUserId", String.valueOf(1L << 32) + (1L << 32)));
    assertEquals(IdMapUrlBuilder.hashData("44539737327", IdMapUrlBuilder.HASH_ALGO_SHA_256),
        Whitebox.<String>invokeMethod(dapResponseHandler,
            "getHLastLoggedInUserId", "44539737327"));
  }

  @Test
  public void setIsMobile() throws Exception {
    URIBuilder dapUriBuilder = new URIBuilder();
    Whitebox.<String>invokeMethod(dapResponseHandler, "setIsMobile", dapUriBuilder, false);
    List<NameValuePair> collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.IS_MOB))
        .collect(Collectors.toList());
    assertEquals(0, collect.size());

    Whitebox.<String>invokeMethod(dapResponseHandler, "setIsMobile", dapUriBuilder, true);
    collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.IS_MOB))
        .collect(Collectors.toList());
    assertEquals(Constants.IS_MOB_TRUE, collect.get(0).getValue());
  }

  @Test
  public void isMobileUserAgent() throws Exception {
    assertFalse(Whitebox.<Boolean>invokeMethod(dapResponseHandler, "isMobileUserAgent", ""));
    assertFalse(Whitebox.<Boolean>invokeMethod(dapResponseHandler, "isMobileUserAgent", "Symbian"));
    assertTrue(Whitebox.<Boolean>invokeMethod(dapResponseHandler, "isMobileUserAgent", "SSSymbianOSSS"));
  }

  @Test
  public void setGuid() throws Exception {
    URIBuilder dapUriBuilder = new URIBuilder();
    Whitebox.<String>invokeMethod(dapResponseHandler, "setGuid", dapUriBuilder, "");
    List<NameValuePair> collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.GUID))
        .collect(Collectors.toList());
    assertEquals(0, collect.size());

    Whitebox.<String>invokeMethod(dapResponseHandler, "setGuid", dapUriBuilder, "abcd");
    collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.GUID))
        .collect(Collectors.toList());
    assertEquals("abcd", collect.get(0).getValue());
  }

  @Test
  public void setRvrId() throws Exception {
    URIBuilder dapUriBuilder = new URIBuilder();
    Whitebox.<String>invokeMethod(dapResponseHandler, "setRvrId", dapUriBuilder, 123L);
    List<NameValuePair> collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.RVR_ID))
        .collect(Collectors.toList());
    assertEquals("123", collect.get(0).getValue());
  }

  @Test
  public void setRoverUserid() throws Exception {
    URIBuilder dapUriBuilder = new URIBuilder();
    Whitebox.<String>invokeMethod(dapResponseHandler, "setRoverUserid", dapUriBuilder, "");
    List<NameValuePair> collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.ROVER_USERID))
        .collect(Collectors.toList());
    assertEquals(0, collect.size());

    Whitebox.<String>invokeMethod(dapResponseHandler, "setRoverUserid", dapUriBuilder, "abcd");
    collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.ROVER_USERID))
        .collect(Collectors.toList());
    assertEquals("abcd", collect.get(0).getValue());
  }

  @Test
  public void addParameter() throws Exception {
    URIBuilder dapUriBuilder = new URIBuilder();
    Whitebox.<String>invokeMethod(dapResponseHandler, "addParameter", dapUriBuilder, "", "");
    List<NameValuePair> collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals("test_key"))
        .collect(Collectors.toList());
    assertEquals(0, collect.size());

    Whitebox.<String>invokeMethod(dapResponseHandler, "addParameter", dapUriBuilder, "test_key", "test_value");
    collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals("test_key"))
        .collect(Collectors.toList());

    Whitebox.<String>invokeMethod(dapResponseHandler, "addParameter", dapUriBuilder, "test_key", "test_value");
    collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals("test_key"))
        .collect(Collectors.toList());
    assertEquals("test_value", collect.get(0).getValue());
  }

  @Test
  public void setRequestParameters() throws Exception {
    URIBuilder dapUriBuilder = new URIBuilder();
    Map<String, String[]> map = new HashMap<>();
    Whitebox.<String>invokeMethod(dapResponseHandler, "setRequestParameters", dapUriBuilder, map);
    assertTrue(dapUriBuilder.getQueryParams().isEmpty());

    map.put("", new String[]{"test"});
    Whitebox.<String>invokeMethod(dapResponseHandler, "setRequestParameters", dapUriBuilder, map);
    assertTrue(dapUriBuilder.getQueryParams().isEmpty());

    map.put(Constants.IPN, new String[]{"test"});
    map.put("cguid", new String[]{"test"});
    map.put("guid", new String[]{"test"});
    map.put("rover_userid", new String[]{"test"});
    Whitebox.<String>invokeMethod(dapResponseHandler, "setRequestParameters", dapUriBuilder, map);
    assertTrue(dapUriBuilder.getQueryParams().isEmpty());

    map.put("test_key", new String[0]);
    Whitebox.<String>invokeMethod(dapResponseHandler, "setRequestParameters", dapUriBuilder, map);
    assertTrue(dapUriBuilder.getQueryParams().isEmpty());

    map.put("icep_siteid", new String[]{"2"});
    Whitebox.<String>invokeMethod(dapResponseHandler, "setRequestParameters", dapUriBuilder, map);
    List<NameValuePair> collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals("siteid") || nameValuePair.getName().equals("icep_siteid"))
        .collect(Collectors.toList());
    assertEquals(1, collect.size());
    assertEquals("2", collect.get(0).getValue());

    map.put("test_key", new String[]{"", "test_value"});
    Whitebox.<String>invokeMethod(dapResponseHandler, "setRequestParameters", dapUriBuilder, map);
    collect = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals("test_key"))
        .collect(Collectors.toList());
    assertEquals("test_value", collect.get(0).getValue());
  }

  @Test
  public void constructTrackingHeader() throws Exception {
    String head1 = Whitebox.invokeMethod(dapResponseHandler, "constructTrackingHeader", "", "");
    assertTrue(head1.contains("guid="));
    assertFalse(head1.contains("cguid="));

    String head2 = Whitebox.invokeMethod(dapResponseHandler, "constructTrackingHeader", "1234", "");
    assertTrue(head2.contains("cguid=1234"));
    assertTrue(head2.contains("guid="));

    String head3 = Whitebox.invokeMethod(dapResponseHandler, "constructTrackingHeader", "", "5678");
    assertFalse(head3.contains("cguid="));
    assertTrue(head3.contains("guid=5678"));

    String head4 = Whitebox.invokeMethod(dapResponseHandler, "constructTrackingHeader", "1234", "5678");
    assertTrue(head4.contains("cguid=1234"));
    assertTrue(head4.contains("guid=5678"));
  }

  @Test
  public void setReferrer() throws Exception {
    URIBuilder dapUriBuilder = new URIBuilder();

    Whitebox.invokeMethod(dapResponseHandler, "setReferrer", dapUriBuilder, null);
    assertTrue(dapUriBuilder.getQueryParams().isEmpty());

    Whitebox.invokeMethod(dapResponseHandler, "setReferrer", dapUriBuilder, "");
    assertTrue(dapUriBuilder.getQueryParams().isEmpty());

    Whitebox.invokeMethod(dapResponseHandler, "setReferrer", dapUriBuilder, "https://www.ebay.com/test");

    String refUrl = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.REF_URL))
        .collect(Collectors.toList()).get(0).getValue();
    assertEquals(FastURLEncoder.encode("https://www.ebay.com/test", KernelConstants.UTF8_ENCODING), refUrl);

    String refDomain = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.REF_DOMAIN))
        .collect(Collectors.toList()).get(0).getValue();
    assertEquals("www.ebay.com", refDomain);

    dapUriBuilder.clearParameters();
    Whitebox.invokeMethod(dapResponseHandler, "setReferrer", dapUriBuilder, "Hello world!");

    refUrl = dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.REF_URL))
        .collect(Collectors.toList()).get(0).getValue();
    assertEquals(FastURLEncoder.encode("Hello world!", KernelConstants.UTF8_ENCODING), refUrl);

    assertTrue(dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals(Constants.REF_DOMAIN))
        .collect(Collectors.toList()).isEmpty());
  }

  @Test
  public void getCountryFromBrowserLocale() throws Exception {
    HttpServletRequest httpServletRequest = Mockito.mock(HttpServletRequest.class);

    when(httpServletRequest.getHeader(Constants.HTTP_ACCEPT_LANGUAGE)).thenReturn(null);
    assertNull(Whitebox.invokeMethod(dapResponseHandler, "getCountryFromBrowserLocale", httpServletRequest));

    when(httpServletRequest.getHeader(Constants.HTTP_ACCEPT_LANGUAGE)).thenReturn(",,");
    assertNull(Whitebox.invokeMethod(dapResponseHandler, "getCountryFromBrowserLocale", httpServletRequest));

    when(httpServletRequest.getHeader(Constants.HTTP_ACCEPT_LANGUAGE)).thenReturn("test");
    assertNull(Whitebox.invokeMethod(dapResponseHandler, "getCountryFromBrowserLocale", httpServletRequest));

    when(httpServletRequest.getHeader(Constants.HTTP_ACCEPT_LANGUAGE)).thenReturn("--");
    assertNull(Whitebox.invokeMethod(dapResponseHandler, "getCountryFromBrowserLocale", httpServletRequest));

    when(httpServletRequest.getHeader(Constants.HTTP_ACCEPT_LANGUAGE)).thenReturn("en-US");
    assertEquals("US", Whitebox.invokeMethod(dapResponseHandler, "getCountryFromBrowserLocale", httpServletRequest));
  }

  @Test
  public void isValidCountryCode() throws Exception {
    assertFalse(Whitebox.invokeMethod(dapResponseHandler, "isValidCountryCode", null));
    assertFalse(Whitebox.invokeMethod(dapResponseHandler, "isValidCountryCode", "England"));
    assertTrue(Whitebox.invokeMethod(dapResponseHandler, "isValidCountryCode", "US"));
  }

  @Test
  public void convertLocaleNameToLocale() throws Exception {
    assertNull(Whitebox.invokeMethod(dapResponseHandler, "convertLocaleNameToLocale", null));
    assertNull(Whitebox.invokeMethod(dapResponseHandler, "convertLocaleNameToLocale", ""));
    assertNull(Whitebox.invokeMethod(dapResponseHandler, "convertLocaleNameToLocale", "--"));
    assertEquals(new Locale("en", ""), Whitebox.invokeMethod(dapResponseHandler, "convertLocaleNameToLocale", "en"));
    assertEquals(new Locale("en", "US"), Whitebox.invokeMethod(dapResponseHandler, "convertLocaleNameToLocale", "en-US"));
  }

  @Test
  public void setGeoInfo() throws Exception {
    URIBuilder dapUriBuilder = new URIBuilder();
    Map<String, String> lbsParameters = new HashMap<>();

    Whitebox.invokeMethod(dapResponseHandler, "setGeoInfo", dapUriBuilder, lbsParameters);
    assertTrue(dapUriBuilder.getQueryParams().isEmpty());

    lbsParameters.put("test_key", "test_value");
    Whitebox.invokeMethod(dapResponseHandler, "setGeoInfo", dapUriBuilder, lbsParameters);
    assertEquals("test_value", dapUriBuilder.getQueryParams().stream()
        .filter(nameValuePair -> nameValuePair.getName().equals("test_key"))
        .collect(Collectors.toList()).get(0).getValue());
  }

  @Test
  public void getLBSParameters() throws Exception {
    Client client = Mockito.mock(Client.class);
    Configuration conf = Mockito.mock(Configuration.class);
    Response response = prepareResponse();
    WebTarget webTarget = Mockito.mock(WebTarget.class);
    Invocation.Builder builder = Mockito.mock(Invocation.Builder.class);

    when(conf.getProperty(EndpointUri.KEY)).thenReturn("localhost");
    when(builder.get()).thenReturn(response);
    when(webTarget.queryParam(anyString(), anyString())).thenReturn(webTarget);
    when(webTarget.request(MediaType.APPLICATION_JSON)).thenReturn(builder);
    when(client.target(anyString())).thenReturn(webTarget);
    when(client.getConfiguration()).thenReturn(conf);

    lbsClient = LBSClient.getInstance();
    Whitebox.setInternalState(lbsClient, "client", client);

    LBSQueryResult lbsQueryResult = lbsClient.getLBSInfo("97.77.104.22");

    HttpServletRequest httpServletRequest = Mockito.mock(HttpServletRequest.class);

    Map<String, String> map = Whitebox.invokeMethod(dapResponseHandler, "getLBSParameters", httpServletRequest, lbsQueryResult);
    assertEquals("US", map.get((LBSConstants.GEO_COUNTRY_CODE)));
    assertEquals("TX", map.get((LBSConstants.GEO_DMA)));
    assertEquals("ft worth", map.get((LBSConstants.GEO_CITY)));
    assertEquals("76137", map.get((LBSConstants.GEO_ZIP_CODE)));
    assertEquals("32.86", map.get((LBSConstants.GEO_LATITUDE)));
    assertEquals("-97.2934", map.get((LBSConstants.GEO_LONGITUDE)));
    assertEquals("623", map.get((LBSConstants.GEO_METRO_CODE)));
    assertEquals("817", map.get((LBSConstants.GEO_AREA_CODE)));

    when(httpServletRequest.getHeader(Constants.HTTP_ACCEPT_LANGUAGE)).thenReturn(null);
    map = Whitebox.invokeMethod(dapResponseHandler, "getLBSParameters", httpServletRequest, lbsQueryResult);
    assertEquals("US", map.get((LBSConstants.GEO_COUNTRY_CODE)));

    when(httpServletRequest.getHeader(Constants.HTTP_ACCEPT_LANGUAGE)).thenReturn("de-CH");
    map = Whitebox.invokeMethod(dapResponseHandler, "getLBSParameters", httpServletRequest, lbsQueryResult);
    assertEquals("CH", map.get((LBSConstants.GEO_COUNTRY_CODE)));
  }

  private Response prepareResponse() {
    LBSQueryResult queryResult = new LBSQueryResult();
    queryResult.setRegionCode("44");
    queryResult.setPostalCode("76137");
    queryResult.setMetroCode("623");
    queryResult.setIsoCountryCode2("US");
    queryResult.setStateCode("TX");
    queryResult.setLongitude(-97.2934d);
    queryResult.setAreaCodes("817/682");
    queryResult.setLatitude(32.86d);
    queryResult.setQueryId("chocolate_geotargeting_ip_1");
    queryResult.setCity("ft worth");

    List<LBSQueryResult> queryResultList = new ArrayList<>();
    queryResultList.add(queryResult);

    LBSHttpResult httpResult = new LBSHttpResult();
    httpResult.setHttpStatus(200);
    httpResult.setQueryResult(queryResultList);

    List<LBSHttpResult> httpResultList = new ArrayList<>();
    httpResultList.add(httpResult);

    LBSResults lbsResults = new LBSResults();
    lbsResults.setAllResults(httpResultList);

    Response response = Mockito.mock(Response.class);
    when(response.getStatus()).thenReturn(200);
    when(response.readEntity(LBSResults.class)).thenReturn(lbsResults);
    return response;
  }

  @Test
  public void callDAPResponse() throws Exception {
    Client client = Mockito.mock(Client.class);
    PowerMockito.mockStatic(GingerClientBuilder.class);
    PowerMockito.when(GingerClientBuilder.newClient(any(Configuration.class))).thenReturn(client);

    Configuration conf = Mockito.mock(Configuration.class);

    BufferedReader bufferedReader = org.mockito.Mockito.mock(BufferedReader.class);
    when(bufferedReader.readLine())
        .thenReturn("first line")
        .thenReturn("second line");
    dapResponseHandler.couchbaseClient = Mockito.mock(CouchbaseClient.class);

    InputStream is = org.mockito.Mockito.mock(InputStream.class);
    when(is.read()).thenReturn(2);

    Response response = Mockito.mock(Response.class, Mockito.RETURNS_DEEP_STUBS);
    MultivaluedMap<String, Object> headers = null;
    when(response.getHeaders()).thenReturn(headers);
    when(response.getStatus()).thenReturn(Response.Status.OK.getStatusCode());
    HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);

    when(response.getEntity()).thenReturn(new Object());

    WebTarget webTarget = Mockito.mock(WebTarget.class);
    Invocation.Builder builder = Mockito.mock(Invocation.Builder.class);

    when(conf.getProperty(EndpointUri.KEY)).thenReturn("localhost");
    when(builder.get()).thenReturn(response);
    when(webTarget.queryParam(anyString(), anyString())).thenReturn(webTarget);
    when(webTarget.request()).thenReturn(builder);
    when(client.target(anyString())).thenReturn(webTarget);
    when(client.getConfiguration()).thenReturn(conf);

    HttpServletRequest httpServletRequest = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse httpServletResponse = Mockito.mock(HttpServletResponse.class);
    MultivaluedMap<String, Object> map = Whitebox.invokeMethod(dapResponseHandler, "callDAPResponse", "", httpServletRequest, httpServletResponse);
    assertNull(map);
  }

  @Test
  public void sendToMCS() throws Exception {
    Client client = Mockito.mock(Client.class);
    PowerMockito.mockStatic(GingerClientBuilder.class);
    PowerMockito.when(GingerClientBuilder.newClient(any(Configuration.class))).thenReturn(client);
    Configuration conf = Mockito.mock(Configuration.class);
    when(client.getConfiguration()).thenReturn(conf);

    WebTarget webTarget = Mockito.mock(WebTarget.class);
    Invocation.Builder builder = Mockito.mock(Invocation.Builder.class);
    when(builder.header(anyString(), anyString())).thenReturn(builder);
    when(conf.getProperty(EndpointUri.KEY)).thenReturn("localhost");
    when(webTarget.path(anyString())).thenReturn(webTarget);
    when(webTarget.request()).thenReturn(builder);

    when(client.target(anyString())).thenReturn(webTarget);

    HttpServletRequest httpServletRequest = Mockito.mock(HttpServletRequest.class);
    when(httpServletRequest.getHeaderNames()).thenReturn(new Enumeration<String>() {
      @Override
      public boolean hasMoreElements() {
        return false;
      }

      @Override
      public String nextElement() {
        return null;
      }
    });

    when(httpServletRequest.getRequestURL()).thenReturn(new StringBuffer("localhost"));
    AsyncInvoker asyncInvoker = Mockito.mock(AsyncInvoker.class);
    when(builder.async()).thenReturn(asyncInvoker);

    MultivaluedHashMap<String, Object> dapResponseHeaders = new MultivaluedHashMap<>();
    dapResponseHeaders.put("ff1", Collections.singletonList("ff1"));

    Whitebox.invokeMethod(dapResponseHandler, "sendToMCS", httpServletRequest, 1L, "", "", dapResponseHeaders);

    Mockito.verify(asyncInvoker).post(anyObject(), any(InvocationCallback.class));
  }

}
