package com.ebay.traffic.chocolate.listener.api;

import com.ebay.traffic.chocolate.listener.util.ChannelActionEnum;
import com.ebay.traffic.chocolate.listener.util.ChannelIdEnum;
import com.ebay.traffic.chocolate.listener.util.LogicalChannelEnum;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class AdsTrackingEventTest {
  /* valid data */
  private static String clickURL = "https://www.ebayadservices.com/v1?mkevt=1&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&testkey=testval";
  private static MockHttpServletRequest mockClientRequest;
  private String vimpURL = "https://www.ebayadservices.com/v1?mkevt=3&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&testkey=testval";
  private String impURL = "https://www.ebayadservices.com/v1?mkevt=2&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&testkey=testval";
  private String page = "http://www.ebay.com/itm/The-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-/380963112068";

  @BeforeClass
  public static void setUp() throws UnsupportedEncodingException {
    mockClientRequest = new MockHttpServletRequest();
    String[] splitClick = clickURL.split("\\?");
    mockClientRequest.setRequestURI(clickURL);
    mockClientRequest.setParameters(parsePayload(splitClick[1]));

  }

  private static Map<String, String[]> parsePayload(String query) throws UnsupportedEncodingException {
    Map<String, String[]> map = new HashMap<>();
    if (StringUtils.isEmpty(query)) return map;
    for (String pair : query.split("&")) {
      String kv[] = pair.split("=");
      Validate.isTrue(kv.length == 2);
      map.put(kv[0], new String[]{URLDecoder.decode(kv[1], "UTF-8")});
    }
    return map;
  }

  @Test
  public void testCreatingNewEventShouldReturnCorrectVersion() throws Exception {
    AdsTrackingEvent event = new AdsTrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
    assertEquals(1, event.getVersion());
  }

  @Test
  public void testCreatingNewEventShouldReturnClickEventType() throws Exception {
    MockHttpServletRequest mockClientRequest = new MockHttpServletRequest();
    mockClientRequest.setRequestURI(clickURL);
    mockClientRequest.setParameters(parsePayload(clickURL.split("\\?")[1]));
    AdsTrackingEvent event = new AdsTrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
    assertEquals(ChannelActionEnum.CLICK, event.getAction());
    assertEquals(LogicalChannelEnum.DISPLAY, ChannelIdEnum.parse(Integer.toString(event.getChannelID())).getLogicalChannel());
  }

  @Test
  public void testCreatingNewEventShouldReturnVimpEventType() throws Exception {
    MockHttpServletRequest mockClientRequest = new MockHttpServletRequest();
    mockClientRequest.setRequestURI(vimpURL);
    mockClientRequest.setParameters(parsePayload(vimpURL.split("\\?")[1]));
    AdsTrackingEvent event = new AdsTrackingEvent(new URL(vimpURL), mockClientRequest.getParameterMap());
    assertEquals(ChannelActionEnum.VIMP, event.getAction());
    assertEquals(LogicalChannelEnum.DISPLAY, ChannelIdEnum.parse(Integer.toString(event.getChannelID())).getLogicalChannel());
  }

  @Test
  public void testCreatingNewEventShouldReturnImpressionEventType() throws Exception {
    MockHttpServletRequest mockClientRequest = new MockHttpServletRequest();
    mockClientRequest.setRequestURI(impURL);
    mockClientRequest.setParameters(parsePayload(impURL.split("\\?")[1]));
    AdsTrackingEvent event = new AdsTrackingEvent(new URL(impURL), mockClientRequest.getParameterMap());

    assertEquals(ChannelActionEnum.IMPRESSION, event.getAction());
    assertEquals(LogicalChannelEnum.DISPLAY, ChannelIdEnum.parse(Integer.toString(event.getChannelID())).getLogicalChannel());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreatingNewEventShouldThrowExceptionWhenVersionIncorrectlySpecified() throws Exception {
    String invalidVersion = "https://www.ebayadservices.com/vv?mkevt=1&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068";
    new AdsTrackingEvent(new URL(invalidVersion), null);
  }

  @Test(expected = NumberFormatException.class)
  public void testCreatingNewEventShouldThrowExceptionWhenInvalidItem() throws Exception {
    String invalidItem = "https://www.ebayadservices.com/v1?mkevt=1&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=38096311abdew8";
    String[] splitClick = invalidItem.split("\\?");
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setRequestURI(invalidItem);
    request.setParameters(parsePayload(splitClick[1]));
    new AdsTrackingEvent(new URL(invalidItem), request.getParameterMap());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreatingNewEventShouldThrowExceptionWhenEventIncorrectlySpecified() throws Exception {
    String invalidEvent = "https://www.ebayadservices.com/v1?mkevt=99&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068";
    String[] splitClick = invalidEvent.split("\\?");
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setRequestURI(invalidEvent);
    request.setParameters(parsePayload(splitClick[1]));
    new AdsTrackingEvent(new URL(invalidEvent), request.getParameterMap());
  }

  @Test
  public void testCreatingNewEventShouldReturnCorrectChannelID() throws Exception {
    AdsTrackingEvent event = new AdsTrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());

    assertEquals(4, event.getChannelID());
  }

  @Test
  public void testCreatingNewEventShouldReturnCorrectCollectionID() throws Exception {
    AdsTrackingEvent event = new AdsTrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());

    assertEquals("711-1245-1245-235", event.getCollectionID());
  }

  @Test
  public void testCreatingNewEventShouldReturnCorrectMapOfPayload() throws Exception {
    AdsTrackingEvent event = new AdsTrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
    Map<String, Object> payload = event.getPayload();

    assertEquals("testval", payload.get("testkey"));
  }

  @Test
  public void testCreatingNewEventShouldReturnCorrectMapOfPayloadEvenIfSplit() throws Exception {
    AdsTrackingEvent event = new AdsTrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
    Map<String, Object> payload = event.getPayload();
    assertEquals("testval", payload.get("testkey"));
  }

  @Test
  public void testPayloadShouldParseLongValuesForItemIDAndProductID() throws Exception {
    AdsTrackingEvent event = new AdsTrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
    Map<String, Object> payload = event.getPayload();

    assertEquals(380963112068L, payload.get("item"));
  }

  @Test
  public void testPayloadShouldURLDecodeAnyStringValues() throws Exception {
    AdsTrackingEvent event = new AdsTrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
    Map<String, Object> payload = event.getPayload();

    assertEquals(page, payload.get("mklndp"));

  }

  @Test(expected = IllegalArgumentException.class)
  public void testPayloadShouldThrowExceptionWhenItemIDIsNotANumber() throws Exception {
    String invalidItem = "https://www.ebayadservices.com/v1?mkevt=99&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=3ABC80963112068";
    String[] splitClick = invalidItem.split("\\?");
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setRequestURI(invalidItem);
    request.setParameters(parsePayload(splitClick[1]));
    new AdsTrackingEvent(new URL(invalidItem), request.getParameterMap());
  }

  @Test
  public void testRespondToEventShouldRedirectToSpecifiedPageURL() throws Exception {
    String[] splitClick = clickURL.split("\\?");
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.encodeRedirectURL(anyString())).thenReturn(page);

    AdsTrackingEvent event = new AdsTrackingEvent(new URL(clickURL), parsePayload(splitClick[1]));
    event.respond(response);

    verify(response).sendRedirect(page);
  }

  @Test
  public void testRespondToEventShouldRedirectToEbayDotComIfNonEbayHostSpecified() throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.encodeRedirectURL("http://www.ebay.com")).thenReturn("http://www.ebay.com");

    String url = "https://www.ebayadservices.com/v1?mkevt=1&mkcid=4&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&mklndp=http%3A%2F%2Fwww.hackme.com";

    String[] splitClick = url.split("\\?");
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setRequestURI(url);
    request.setParameters(parsePayload(splitClick[1]));

    AdsTrackingEvent event = new AdsTrackingEvent(new URL(url), request.getParameterMap());
    event.respond(response);

    verify(response).sendRedirect("http://www.ebay.com");
  }

  @Test
  public void testRespondToEventShouldSendAPixelForImpressions() throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    ServletOutputStream output = mock(ServletOutputStream.class);
    when(response.getOutputStream()).thenReturn(output);

    String impressionURL = "https://www.ebayadservices.com/v1?mkevt=2&mkcid=4&mkrid=711-2";

    String[] split = impressionURL.split("\\?");
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setRequestURI(impressionURL);
    request.setParameters(parsePayload(split[1]));


    AdsTrackingEvent event = new AdsTrackingEvent(new URL(impressionURL), request.getParameterMap());
    event.respond(response);

    verify(output).write(EventConstant.pixel);
  }

  @Test
  public void testRespondToEventShouldSetCGUIDIfNotPresent() throws Exception {
    HttpServletResponse response = mock(HttpServletResponse.class);
    when(response.encodeRedirectURL(anyString())).thenReturn(page);

    AdsTrackingEvent event = new AdsTrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
    event.respond(response);

//        verify(response).addCookie(Mockito.notNull());
  }
}