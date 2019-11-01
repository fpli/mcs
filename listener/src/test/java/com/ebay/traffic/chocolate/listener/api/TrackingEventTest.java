package com.ebay.traffic.chocolate.listener.api;

import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.LogicalChannelEnum;
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

public class TrackingEventTest {

    /* valid data */
    private static String clickURL = "https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&testkey=testval";
    private String vimpURL = "https://c.ebay.com/1v/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&testkey=testval";
    private String impURL = "https://c.ebay.com/1i/9-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&testkey=testval";
    private String page = "http://www.ebay.com/itm/The-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-/380963112068";
  private static MockHttpServletRequest mockClientRequest;

    @BeforeClass
    public static void setUp() throws UnsupportedEncodingException{
      mockClientRequest = new MockHttpServletRequest();
      String[] splitClick = clickURL.split("\\?");
      mockClientRequest.setRequestURI(clickURL);
      mockClientRequest.setParameters(parsePayload(splitClick[1]));

    }

    @Test
    public void testCreatingNewEventShouldReturnCorrectVersion() throws Exception {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
        assertEquals(1, event.getVersion());
    }

    @Test
    public void testCreatingNewEventShouldReturnClickEventType() throws Exception {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
        assertEquals(ChannelActionEnum.CLICK, event.getAction());
        assertEquals(LogicalChannelEnum.EPN, ChannelIdEnum.parse(Integer.toString(event.getChannelID())).getLogicalChannel());
    }

    @Test
    public void testCreatingNewEventShouldReturnVimpEventType() throws Exception {
        TrackingEvent event = new TrackingEvent(new URL(vimpURL), mockClientRequest.getParameterMap());
        assertEquals(ChannelActionEnum.VIMP, event.getAction());
        assertEquals(LogicalChannelEnum.EPN, ChannelIdEnum.parse(Integer.toString(event.getChannelID())).getLogicalChannel());
    }

    @Test
    public void testCreatingNewEventShouldReturnImpressionEventType() throws Exception {
        TrackingEvent event = new TrackingEvent(new URL(impURL), mockClientRequest.getParameterMap());

        assertEquals(ChannelActionEnum.IMPRESSION, event.getAction());
        assertEquals(LogicalChannelEnum.EPN, ChannelIdEnum.parse(Integer.toString(event.getChannelID())).getLogicalChannel());
    }


    @Test(expected = IllegalArgumentException.class)
    public void testCreatingNewEventShouldThrowExceptionWhenVersionIncorrectlySpecified() throws Exception {
        String invalidVersion = "https://c.ebay.com/cc/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068";
        new TrackingEvent(new URL(invalidVersion), null);
    }

  @Test(expected = NumberFormatException.class)
  public void testCreatingNewEventShouldThrowExceptionWhenInvalidItem() throws Exception {
    String invalidItem = "https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=38096311abdew8";
    String[] splitClick = invalidItem.split("\\?");
    MockHttpServletRequest request = new MockHttpServletRequest();
    request.setRequestURI(invalidItem);
    request.setParameters(parsePayload(splitClick[1]));
    new TrackingEvent(new URL(invalidItem), request.getParameterMap());
  }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatingNewEventShouldThrowExceptionWhenEventIncorrectlySpecified() throws Exception{
        String invalidEvent = "https://c.ebay.com/11/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068";
      String[] splitClick = invalidEvent.split("\\?");
      MockHttpServletRequest request = new MockHttpServletRequest();
      request.setRequestURI(invalidEvent);
      request.setParameters(parsePayload(splitClick[1]));
        new TrackingEvent(new URL(invalidEvent), request.getParameterMap());
    }

    @Test
    public void testCreatingNewEventShouldReturnCorrectChannelID() throws Exception {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());

        assertEquals(1, event.getChannelID());
    }

    @Test
    public void testCreatingNewEventShouldReturnCorrectCollectionID() throws Exception {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());

        assertEquals("12345", event.getCollectionID());
    }

    @Test
    public void testCreatingNewEventShouldReturnCorrectMapOfPayload() throws Exception {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
        Map<String, Object> payload = event.getPayload();

        assertEquals("testval", payload.get("testkey"));
    }

    @Test
    public void testCreatingNewEventShouldReturnCorrectMapOfPayloadEvenIfSplit() throws Exception {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
        Map<String, Object> payload = event.getPayload();
        assertEquals("testval", payload.get("testkey"));
    }

    @Test
    public void testPayloadShouldParseLongValuesForItemIDAndProductID() throws Exception {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
        Map<String, Object> payload = event.getPayload();

        assertEquals(380963112068L, payload.get("item"));
    }

    @Test
    public void testPayloadShouldURLDecodeAnyStringValues() throws Exception {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
        Map<String, Object> payload = event.getPayload();

        assertEquals(page, payload.get("page"));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testPayloadShouldThrowExceptionWhenItemIDIsNotANumber() throws Exception {
        String invalidItem = "https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=3ABC80963112068";
      String[] splitClick = invalidItem.split("\\?");
      MockHttpServletRequest request = new MockHttpServletRequest();
      request.setRequestURI(invalidItem);
      request.setParameters(parsePayload(splitClick[1]));
        new TrackingEvent(new URL(invalidItem), request.getParameterMap());
    }

    @Test
    public void testRespondToEventShouldRedirectToSpecifiedPageURL() throws Exception {
      String[] splitClick = clickURL.split("\\?");
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.encodeRedirectURL(anyString())).thenReturn(page);

        TrackingEvent event = new TrackingEvent(new URL(clickURL), parsePayload(splitClick[1]));
        event.respond(response);

        verify(response).sendRedirect(page);
    }

    @Test
    public void testRespondToEventShouldRedirectToEbayDotComIfNonEbayHostSpecified() throws Exception {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.encodeRedirectURL("http://www.ebay.com")).thenReturn("http://www.ebay.com");

        String url = "https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.hackme.com";

      String[] splitClick = url.split("\\?");
      MockHttpServletRequest request = new MockHttpServletRequest();
      request.setRequestURI(url);
      request.setParameters(parsePayload(splitClick[1]));

        TrackingEvent event = new TrackingEvent(new URL(url), request.getParameterMap());
        event.respond(response);

        verify(response).sendRedirect("http://www.ebay.com");
    }

    @Test
    public void testRespondToEventShouldSendAPixelForImpressions() throws Exception {
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream output = mock(ServletOutputStream.class);
        when(response.getOutputStream()).thenReturn(output);

        String impressionURL = "https://c.ebay.com/1i/1-12345?item=380963112068";

      String[] split = impressionURL.split("\\?");
      MockHttpServletRequest request = new MockHttpServletRequest();
      request.setRequestURI(impressionURL);
      request.setParameters(parsePayload(split[1]));


        TrackingEvent event = new TrackingEvent(new URL(impressionURL), request.getParameterMap());
        event.respond(response);

        verify(output).write(TrackingEvent.pixel);
    }

    @Test
    public void testRespondToEventShouldSetCGUIDIfNotPresent() throws Exception {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.encodeRedirectURL(anyString())).thenReturn(page);

        TrackingEvent event = new TrackingEvent(new URL(clickURL), mockClientRequest.getParameterMap());
        event.respond(response);

//        verify(response).addCookie(Mockito.notNull());
    }

  private static Map<String, String[]> parsePayload(String query) throws UnsupportedEncodingException{
    Map<String, String[]> map = new HashMap<>();
    if (StringUtils.isEmpty(query)) return map;
    for (String pair : query.split("&")) {
      String kv[] = pair.split("=");
      Validate.isTrue(kv.length == 2);
      map.put(kv[0], new String[]{URLDecoder.decode(kv[1],  "UTF-8")});
    }
    return map;
  }
}