package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;

import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.gen.model.UnifiedTrackingEvent;
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
import com.google.common.collect.Maps;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;

import java.io.IOException;
import java.util.*;

import static com.ebay.app.raptor.chocolate.constant.Constants.*;
import static org.junit.Assert.*;

public class UnifiedTrackingMessageParserTest {

  @BeforeClass
  public static void setUp() throws IOException {
    RuntimeContext.setConfigRoot(UnifiedTrackingMessageParserTest.class.getClassLoader().getResource
        ("META-INF/configuration/Dev/"));
    ApplicationOptions.init();
  }

  @Test
  public void testParseUEPEvents() {
    UnifiedTrackingEvent unifiedTrackingEvent = new UnifiedTrackingEvent();
    unifiedTrackingEvent.setPayload(new HashMap<>());
    UnifiedTrackingMessage parse = UnifiedTrackingMessageParser.parse(unifiedTrackingEvent);
    assertEquals(parse.getProducerEventTs(), parse.getEventTs());
    unifiedTrackingEvent.setProducerEventTs(1L);
    parse = UnifiedTrackingMessageParser.parse(unifiedTrackingEvent);
    assertEquals(Long.valueOf(1L), parse.getProducerEventTs());
    assertNotEquals(parse.getProducerEventTs(), parse.getEventTs());
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
    Mockito.when(endUserContext.getIPAddress()).thenReturn("127.0.0.1");
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
    Map<String, String> requestHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    requestHeaders.put(Constants.IS_FROM_UFES_HEADER, "true");
    requestHeaders.put(Constants.NODE_REDIRECTION_HEADER_NAME, "301");

    BaseEvent baseEvent = new BaseEvent();
    baseEvent.setTimestamp(currentTimeMillis);
    baseEvent.setUrl(url);
    baseEvent.setReferer(referer);
    baseEvent.setChannelType(ChannelIdEnum.DAP);
    baseEvent.setActionType(ChannelActionEnum.CLICK);
    baseEvent.setUserAgentInfo(agentInfo);
    baseEvent.setEndUserContext(endUserContext);
    baseEvent.setUrlParameters(parameters);
    requestHeaders.put("X-EBAY-C-TRACKING", "cguid=8b34ef1d1740a4d724970d78eec8ee4c644dc2df");
    baseEvent.setRequestHeaders(requestHeaders);
    baseEvent.setUserPrefsCtx(userPrefsCtx);
    baseEvent.setUuid(UUID.randomUUID().toString());

    UnifiedTrackingMessage message = utpParser.parse(baseEvent, requestContext, snapshotId, shortSnapshotId);

    assertNotNull(message.getEventId());
    assertEquals("", message.getProducerEventId());
    assertEquals(message.getEventTs(), Long.valueOf(currentTimeMillis));
    assertEquals(message.getProducerEventTs(), Long.valueOf(currentTimeMillis));
    assertEquals("123456", message.getRlogId());
    assertNull(message.getTrackingId());
    assertEquals(Long.valueOf(0), message.getUserId());
    assertNull(message.getPublicUserId());
    assertNull(message.getGuid());
    assertNull(message.getDeviceId());
    assertNull(message.getUserAgent());
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
    assertNull(message.getRemoteIp());
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
    assertTrue(UnifiedTrackingMessageParser.isBot("Mediapartners-Google"));
    assertFalse(UnifiedTrackingMessageParser.isBot("eBayAndroid/6.7.2"));
    assertFalse(UnifiedTrackingMessageParser.isBot(""));
  }

  @Test
  public void testLowercaseParams() throws Exception {
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

    IEndUserContext endUserContext = Mockito.mock(IEndUserContext.class);
    Mockito.when(endUserContext.getIPAddress()).thenReturn("127.0.0.1");
    UserAgentInfo agentInfo = new UserAgentParser().parse("ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;" +
        "no-carrier;414x736;3.0");

    String url = "https://www.ebay.com/i/313369033082?mkevt=1&mkpid=2&emsid=e90001.m43.l1123&mkcid=8&bu=45101843235&osub=0d6476007af14726a1eaca4dbd59f3fd%257ETE80101_T_AGM&segname=TE80101_T_AGM&crd=20220321090000&ch=osgood&trkid=0A7B08EE-0B951BFA9CE-017F96B73B0D-0000000000BDDC2E&mesgid=3024&plmtid=700001&recoid=313369033082&recopos=1";
    String referer = "https://www.google.com";
    MultiValueMap<String, String> parameters = UriComponentsBuilder.fromUriString(url).build().getQueryParams();
    long snapshotId = 0L;
    long shortSnapshotId = 0L;
    Map<String, String> requestHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    requestHeaders.put(Constants.IS_FROM_UFES_HEADER, "true");
    requestHeaders.put(Constants.NODE_REDIRECTION_HEADER_NAME, "301");

    BaseEvent baseEvent = new BaseEvent();
    baseEvent.setTimestamp(currentTimeMillis);
    baseEvent.setUrl(url);
    baseEvent.setReferer(referer);
    baseEvent.setChannelType(ChannelIdEnum.MRKT_EMAIL);
    baseEvent.setActionType(ChannelActionEnum.CLICK);
    baseEvent.setUserAgentInfo(agentInfo);
    baseEvent.setEndUserContext(endUserContext);
    baseEvent.setUrlParameters(parameters);
    requestHeaders.put("X-EBAY-C-TRACKING", "cguid=8b34ef1d1740a4d724970d78eec8ee4c644dc2df");
    baseEvent.setRequestHeaders(requestHeaders);
    baseEvent.setUserPrefsCtx(userPrefsCtx);
    baseEvent.setUuid(UUID.randomUUID().toString());

    UnifiedTrackingMessage message = utpParser.parse(baseEvent, requestContext, snapshotId, shortSnapshotId);

    assertNotNull(message.getEventId());
    assertEquals("", message.getProducerEventId());
    assertEquals(message.getEventTs(), Long.valueOf(currentTimeMillis));
    assertEquals(message.getProducerEventTs(), Long.valueOf(currentTimeMillis));
    assertEquals("123456", message.getRlogId());
    assertEquals("0A7B08EE-0B951BFA9CE-017F96B73B0D-0000000000BDDC2E", message.getTrackingId());
    assertEquals(Long.valueOf(2390648398L), message.getUserId());
    assertNull(message.getPublicUserId());
    assertNull(message.getGuid());
    assertNull(message.getDeviceId());
    assertNull(message.getUserAgent());
    assertEquals("MRKT_EMAIL",  message.getChannelType());
    assertEquals("CLICK",  message.getActionType());
    assertEquals("kana",  message.getPartner());
    assertEquals("TE80101_T_AGM",  message.getCampaignId());
    assertEquals("",  message.getRotationId());
    assertEquals(Integer.valueOf(101), message.getSiteId());
    assertEquals(url, message.getUrl());
    assertEquals(referer, message.getReferer());
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
    assertNull(message.getRemoteIp());
    assertEquals(Integer.valueOf(2547208), message.getPageId());
    assertEquals(Integer.valueOf(101), message.getGeoId());
    assertFalse(message.getIsBot());
    assertEquals("xx-YY", message.getPayload().get("lang_cd"));
    assertEquals("true", message.getPayload().get("isUfes"));
    assertEquals("301", message.getPayload().get("statusCode"));
    assertEquals("8", message.getPayload().get("chnl"));
    assertEquals("true", message.getPayload().get("isUEP"));
    assertEquals("0A7B08EE-0B951BFA9CE-017F96B73B0D-0000000000BDDC2E", message.getPayload().get("trkId"));
    assertEquals("0A7B08EE-0B951BFA9CE-017F96B73B0D-0000000000BDDC2E", message.getPayload().get("tracking.id"));
    assertEquals("3024", message.getPayload().get("mesgId"));
    assertEquals("700001", message.getPayload().get("plmtId"));
    assertEquals("313369033082", message.getPayload().get("recoId"));
    assertEquals("1", message.getPayload().get("recoPos"));

  }
}
