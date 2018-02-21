package com.ebay.traffic.chocolate.listener.api;

import com.ebay.traffic.chocolate.listener.util.ChannelActionEnum;
import com.ebay.traffic.chocolate.listener.util.ChannelIdEnum;
import com.ebay.traffic.chocolate.listener.util.LogicalChannelEnum;
import org.junit.Test;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class TrackingEventTest {

    /* valid data */
    private String clickURL = "https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&testkey=testval";
    private String vimpURL = "https://c.ebay.com/1v/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&testkey=testval";
    private String impURL = "https://c.ebay.com/1i/9-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068&testkey=testval";
    private String page = "http://www.ebay.com/itm/The-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-/380963112068";

    @Test
    public void testCreatingNewEventShouldReturnCorrectVersion() throws MalformedURLException {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), null);

        assertEquals(1, event.getVersion());
    }

    @Test
    public void testCreatingNewEventShouldReturnClickEventType() throws MalformedURLException {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), null);

        assertEquals(ChannelActionEnum.CLICK, event.getAction());
        assertEquals(LogicalChannelEnum.EPN, ChannelIdEnum.parse(Integer.toString(event.getChannelID())).getLogicalChannel());
    }

    @Test
    public void testCreatingNewEventShouldReturnVimpEventType() throws MalformedURLException {
        TrackingEvent event = new TrackingEvent(new URL(vimpURL), null);

        assertEquals(ChannelActionEnum.VIMP, event.getAction());
        assertEquals(LogicalChannelEnum.EPN, ChannelIdEnum.parse(Integer.toString(event.getChannelID())).getLogicalChannel());
    }

    @Test
    public void testCreatingNewEventShouldReturnImpressionEventType() throws MalformedURLException {
        TrackingEvent event = new TrackingEvent(new URL(impURL), null);

        assertEquals(ChannelActionEnum.IMPRESSION, event.getAction());
        assertEquals(LogicalChannelEnum.EPN, ChannelIdEnum.parse(Integer.toString(event.getChannelID())).getLogicalChannel());
    }


    @Test(expected = IllegalArgumentException.class)
    public void testCreatingNewEventShouldThrowExceptionWhenVersionIncorrectlySpecified() throws MalformedURLException {
        String invalidVersion = "https://c.ebay.com/cc/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068";
        new TrackingEvent(new URL(invalidVersion), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatingNewEventShouldThrowExceptionWhenEventIncorrectlySpecified() throws MalformedURLException {
        String invalidEvent = "https://c.ebay.com/11/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=380963112068";
        new TrackingEvent(new URL(invalidEvent), null);
    }

    @Test
    public void testCreatingNewEventShouldReturnCorrectChannelID() throws MalformedURLException {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), null);

        assertEquals(1, event.getChannelID());
    }

    @Test
    public void testCreatingNewEventShouldReturnCorrectCollectionID() throws MalformedURLException {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), null);

        assertEquals("12345", event.getCollectionID());
    }

    @Test
    public void testCreatingNewEventShouldReturnCorrectMapOfPayload() throws MalformedURLException {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), null);
        HashMap<String, Object> payload = event.getPayload();

        assertEquals("testval", payload.get("testkey"));
    }

    @Test
    public void testCreatingNewEventShouldReturnCorrectMapOfPayloadEvenIfSplit() throws MalformedURLException {
        String[] splitClick = clickURL.split("\\?");
        assertEquals(2, splitClick.length);
        TrackingEvent event = new TrackingEvent(new URL(splitClick[0]), splitClick[1]);
        HashMap<String, Object> payload = event.getPayload();

        assertEquals("testval", payload.get("testkey"));
    }

    @Test
    public void testPayloadShouldParseLongValuesForItemIDAndProductID() throws MalformedURLException {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), null);
        HashMap<String, Object> payload = event.getPayload();

        assertEquals(380963112068L, payload.get("item"));
    }

    @Test
    public void testPayloadShouldURLDecodeAnyStringValues() throws MalformedURLException {
        TrackingEvent event = new TrackingEvent(new URL(clickURL), null);
        HashMap<String, Object> payload = event.getPayload();

        assertEquals(page, payload.get("page"));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testPayloadShouldThrowExceptionWhenItemIDIsNotANumber() throws MalformedURLException {
        String invalidItem = "https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.ebay.com%2Fitm%2FThe-Way-of-Kings-by-Brandon-Sanderson-Hardcover-Book-English-%2F380963112068&item=3ABC80963112068";
        new TrackingEvent(new URL(invalidItem), null);
    }

    @Test
    public void testRespondToEventShouldRedirectToSpecifiedPageURL() throws IOException {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.encodeRedirectURL(anyString())).thenReturn(page);

        TrackingEvent event = new TrackingEvent(new URL(clickURL), null);
        event.respond(response);

        verify(response).sendRedirect(page);
    }

    @Test
    public void testRespondToEventShouldRedirectToEbayDotComIfNonEbayHostSpecified() throws IOException {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.encodeRedirectURL("http://www.ebay.com")).thenReturn("http://www.ebay.com");

        TrackingEvent event = new TrackingEvent(new URL("https://c.ebay.com/1c/1-12345?page=http%3A%2F%2Fwww.hackme.com"), null);
        event.respond(response);

        verify(response).sendRedirect("http://www.ebay.com");
    }

    @Test
    public void testRespondToEventShouldSendAPixelForImpressions() throws IOException {
        HttpServletResponse response = mock(HttpServletResponse.class);
        ServletOutputStream output = mock(ServletOutputStream.class);
        when(response.getOutputStream()).thenReturn(output);

        String impressionURL = "https://c.ebay.com/1i/1-12345?item=380963112068";
        TrackingEvent event = new TrackingEvent(new URL(impressionURL), null);
        event.respond(response);

        verify(output).write(TrackingEvent.pixel);
    }

    @Test
    public void testRespondToEventShouldSetCGUIDIfNotPresent() throws IOException {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.encodeRedirectURL(anyString())).thenReturn(page);

        TrackingEvent event = new TrackingEvent(new URL(clickURL), null);
        event.respond(response);

//        verify(response).addCookie(Mockito.notNull());
    }
}