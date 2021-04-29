package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.traffic.chocolate.utp.common.model.UnifiedTrackingMessage;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.platform.raptor.raptordds.parsers.UserAgentParser;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.raptor.domain.request.api.DomainRequestData;
import com.ebay.raptor.geo.context.GeoCtx;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import com.ebay.raptorio.request.tracing.RequestTracingContext;
import com.ebay.traffic.monitoring.ESMetrics;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;

public class UnifiedTrackingMessageParserTest {

  @BeforeClass
  public static void setUp() throws IOException {
    RuntimeContext.setConfigRoot(UnifiedTrackingMessageParserTest.class.getClassLoader().getResource
        ("META-INF/configuration/Dev/"));
    ApplicationOptions.init();
    ESMetrics.init("test", "localhost");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testParse() throws Exception {
    UnifiedTrackingMessageParser utpParser = new UnifiedTrackingMessageParser();
    long currentTimeMillis = System.currentTimeMillis();
    ContainerRequestContext requestContext = Mockito.mock(ContainerRequestContext.class);
    UserPrefsCtx userPrefsCtx = Mockito.mock(UserPrefsCtx.class);
    Locale locale = new Locale("xx", "YY");
    GeoCtx geoCtx = new GeoCtx(101);
    Mockito.when(userPrefsCtx.getLangLocale()).thenReturn(locale);
    Mockito.when(userPrefsCtx.getGeoContext()).thenReturn(geoCtx);
    Mockito.when(requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY)).thenReturn(userPrefsCtx);
    RequestTracingContext requestTracingContext = Mockito.mock(RequestTracingContext.class);
    Mockito.when(requestTracingContext.getRlogId()).thenReturn("123456");
    Mockito.when(requestContext.getProperty(RequestTracingContext.NAME)).thenReturn(requestTracingContext);
    DomainRequestData domainRequestData = Mockito.mock(DomainRequestData.class);
    Mockito.when(domainRequestData.getSiteId()).thenReturn(101);
    Mockito.when(domainRequestData.getHost()).thenReturn("localhost");
    Mockito.when(requestContext.getProperty(DomainRequestData.NAME)).thenReturn(domainRequestData);

    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    IEndUserContext endUserContext = Mockito.mock(IEndUserContext.class);
    RaptorSecureContext raptorSecureContext = Mockito.mock(RaptorSecureContext.class);
    UserAgentInfo agentInfo = new UserAgentParser().parse("ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;" +
            "no-carrier;414x736;3.0");

    MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
    parameters.put("mkrid", Arrays.asList(new Object[]{"123-123-123"}));
    parameters.put("sojTags", Arrays.asList(new Object[]{"chnl=mkcid"}));
    parameters.put("mkcid", Arrays.asList(new Object[]{"4"}));
    String url = "https://www.ebay.com/itm/1234123132?mkevt=1&mkcid=4&mkrid=123-123-123";
    String referer = "www.google.com";
    ChannelType channelType = ChannelType.DISPLAY;
    ChannelAction channelAction = ChannelAction.CLICK;
    boolean isROIFromCheckoutAPI = false;
    long snapshotId = 0L;
    long shortSnapshotId = 0L;
    Map<String, String> requestHeaders = new TreeMap<>();
    requestHeaders.put(Constants.IS_FROM_UFES_HEADER, "true");
    requestHeaders.put(Constants.NODE_REDIRECTION_HEADER_NAME, "301");

    UnifiedTrackingMessage message = utpParser.parse(requestContext, request, endUserContext, raptorSecureContext,
        requestHeaders, agentInfo, parameters, url, referer, channelType, channelAction, null,
        snapshotId, shortSnapshotId, currentTimeMillis);

    assertNotNull(message.getEventId());
    assertEquals("", message.getProducerEventId());
    assertEquals(message.getEventTs(), Long.valueOf(currentTimeMillis));
    assertEquals("123456", message.getRlogId());
    assertNull(message.getTrackingId());
    assertEquals(Long.valueOf(0), message.getUserId());
    assertNull( message.getPublicUserId());
    assertNull( message.getGuid());
    assertNull( message.getDeviceId());
    assertNull( message.getUserAgent());
    assertEquals("DISPLAY",  message.getChannelType());
    assertEquals("CLICK",  message.getActionType());
    assertEquals("",  message.getPartner());
    assertEquals("",  message.getCampaignId());
    assertEquals("123123123",  message.getRotationId());
    assertEquals(Integer.valueOf(101), message.getSiteId());
    assertEquals("https://www.ebay.com/itm/1234123132?mkevt=1&mkcid=4&mkrid=123-123-123", message.getUrl());
    assertEquals("www.google.com", message.getReferer());
    assertNull(message.getUserAgent());
    assertEquals("Other", message.getDeviceFamily());
    assertEquals("iOS", message.getDeviceType());
    assertNull(message.getBrowserFamily());
    assertNull(message.getBrowserVersion());
    assertEquals("iOS", message.getOsFamily());
    assertEquals("11.2", message.getOsVersion());
    assertEquals("1462", message.getAppId());
    assertEquals("5.19.0", message.getAppVersion());
    assertEquals("CHOCOLATE", message.getService());
    assertEquals("localhost", message.getServer());
    assertEquals("", message.getRemoteIp());
    assertEquals(Integer.valueOf(2547208), message.getPageId());
    assertEquals(Integer.valueOf(101), message.getGeoId());
    assertFalse(message.getIsBot());
    assertEquals("xx-YY", message.getPayload().get("lang_cd"));
    assertEquals("true", message.getPayload().get("isUfes"));
    assertEquals("301", message.getPayload().get("statusCode"));

  }

  @Test
  public void testBot() {
    assertTrue(UnifiedTrackingMessageParser.isBot("googleBot"));
    assertTrue(UnifiedTrackingMessageParser.isBot("yahooProxy"));
    assertTrue(UnifiedTrackingMessageParser.isBot("Spiderman"));
    assertFalse(UnifiedTrackingMessageParser.isBot("eBayAndroid/6.7.2"));
    assertFalse(UnifiedTrackingMessageParser.isBot(""));
  }
}