/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = EventListenerApplication.class)
public class CollectionServiceTest {

  private String endUserCtxiPhone = "ip=10.148.184.210," +
      "userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage" +
      "%2Fapng%2C*%2F*%3Bq%3D0.8,userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0," +
      "deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF," +
      "contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134,referer=https%3A%2F%2Fwiki.vip.corp.ebay" +
      ".com%2Fdisplay%2FTRACKING%2FTest%2BMarketing%2Btracking,uri=%2Fsampleappweb%2Fsctest," +
      "applicationURL=http%3A%2F%2Ftrackapp-3.stratus.qa.ebay.com%2Fsampleappweb%2Fsctest%3Fmkevt%3D1," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT," +
      "origUserId=origUserName%3Dqamenaka1%2CorigAcctId%3D1026324923,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true&X-EBAY-C-CULTURAL-PREF=currency=USD,locale=en-US,timezone=America%2FLos_Angeles";

  private String endUserCtxNoReferer = "ip=10.148.184.210," +
      "userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage" +
      "%2Fapng%2C*%2F*%3Bq%3D0.8,userAgentAcceptEncoding=gzip%2C+deflate%2C+br,userAgentAcceptCharset=null," +
      "userAgent=ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0," +
      "deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF," +
      "contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134," +
      "applicationURL=http%3A%2F%2Ftrackapp-3.stratus.qa.ebay.com%2Fsampleappweb%2Fsctest%3Fmkevt%3D1," +
      "physicalLocation=country%3DUS,contextualLocation=country%3DIT," +
      "origUserId=origUserName%3Dqamenaka1%2CorigAcctId%3D1026324923,isPiggybacked=false,fullSiteExperience=true," +
      "expectSecureURL=true&X-EBAY-C-CULTURAL-PREF=currency=USD,locale=en-US,timezone=America%2FLos_Angeles";

  @Autowired
  CollectionService collectionService;

  @Test(expected = Exception.class)
  public void collectROIEventNoTracking() throws Exception {
    HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
    IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
    RaptorSecureContext mockRaptorSecureContext = Mockito.mock(RaptorSecureContext.class);
    ContainerRequestContext mockContainerRequestContext = Mockito.mock(ContainerRequestContext.class);
    ROIEvent roiEvent = new ROIEvent();

    // no tracking header
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-TRACKING")).thenReturn(null);
    collectionService.collectROIEvent(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, roiEvent);
  }

  @Test(expected = Exception.class)
  public void collectROIEventNoEndUserctx() throws Exception {

    HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
    IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
    RaptorSecureContext mockRaptorSecureContext = Mockito.mock(RaptorSecureContext.class);
    ContainerRequestContext mockContainerRequestContext = Mockito.mock(ContainerRequestContext.class);
    ROIEvent roiEvent = new ROIEvent();

    // no enduserctx
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-TRACKING")).thenReturn("guid=8b34ef1d1740a4d724970d78eec8ee4c");
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-ENDUSERCTX")).thenReturn(null);
    collectionService.collectROIEvent(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, roiEvent);
  }

  @Test
  public void collectROIEvent() throws Exception {

    HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
    IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
    RaptorSecureContext mockRaptorSecureContext = Mockito.mock(RaptorSecureContext.class);
    ContainerRequestContext mockContainerRequestContext = Mockito.mock(ContainerRequestContext.class);
    ROIEvent roiEvent = new ROIEvent();


    IRequestScopeTracker requestTracker = Mockito.mock(IRequestScopeTracker.class);
    Mockito.when(mockContainerRequestContext.getProperty(IRequestScopeTracker.NAME)).thenReturn(requestTracker);

    //EBAYUSER
    roiEvent.setItemId("192658398245");
    Map<String, String> aaa = new HashMap<String, String>();
    aaa.put("ff1", "ss");
    roiEvent.setPayload(aaa);
    roiEvent.setTransType("BO-MobileApp@");
    roiEvent.setUniqueTransactionId("1677235978009");

    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-TRACKING")).thenReturn("guid=8b34ef1d1740a4d724970d78eec8ee4c");
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-ENDUSERCTX")).thenReturn(endUserCtxiPhone);
    Mockito.when(mockRaptorSecureContext.getSubjectDomain()).thenReturn("EBAYUSER");
    Mockito.when(mockRaptorSecureContext.getSubjectImmutableId()).thenReturn("12345");
    UserAgentInfo agentInfo = new UserAgentInfo();
    agentInfo.setDesktop(true);
    Mockito.when(mockContainerRequestContext.getProperty(UserAgentInfo.NAME)).thenReturn(agentInfo);

    Mockito.when(mockIEndUserContext.getIPAddress()).thenReturn("127.0.0.1");
    Mockito.when(mockIEndUserContext.getUserAgent()).thenReturn("ebayAndroid/6.2");

//    Producer<Long, ListenerMessage> producer = Mockito.mock(Producer<Long, ListenerMessage.class);
//    Mockito.when(KafkaSink.get()).thenReturn(producer);

    assertTrue(collectionService.collectROIEvent(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, roiEvent));

    //invalid item id
    Mockito.when(mockRaptorSecureContext.getSubjectDomain()).thenReturn("EBAY");
    roiEvent.setItemId("abc");
    assertTrue(collectionService.collectROIEvent(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, roiEvent));
    assertEquals("", roiEvent.getItemId());

    //invalid timestamp
    roiEvent.setTransactionTimestamp("-12423232");
    assertTrue(collectionService.collectROIEvent(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, roiEvent));
    assertTrue(Long.valueOf(roiEvent.getTransactionTimestamp()) > 0);

    // invalid transid
    roiEvent.setUniqueTransactionId("-12312312");
    assertTrue(collectionService.collectROIEvent(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, roiEvent));
    assertEquals("", roiEvent.getUniqueTransactionId());

    //no referrer in header, header in payload encoded
    Map<String, String> payload = new HashedMap();
    payload.put("referrer", "https%3A%2F%2Fwww.google.com");
    roiEvent.setPayload(payload);
    assertTrue(collectionService.collectROIEvent(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, roiEvent));

    // null referer in payload
    payload.put("referrer", "");
    assertTrue(collectionService.collectROIEvent(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, roiEvent));

  }

  @Test(expected = Exception.class)
  public void collectImpressionNoTracking() throws Exception {
    HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
    IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
    RaptorSecureContext mockRaptorSecureContext = Mockito.mock(RaptorSecureContext.class);
    ContainerRequestContext mockContainerRequestContext = Mockito.mock(ContainerRequestContext.class);

    Event event = new Event();
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=2&mkcid=1");

    // no tracking header, exception
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-TRACKING")).thenReturn(null);
    collectionService.collectImpression(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, event);

  }

  @Test
  public void collectImpression() throws Exception {
    HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
    IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
    RaptorSecureContext mockRaptorSecureContext = Mockito.mock(RaptorSecureContext.class);
    ContainerRequestContext mockContainerRequestContext = Mockito.mock(ContainerRequestContext.class);

    UserAgentInfo agentInfo = new UserAgentInfo();
    agentInfo.setDesktop(true);
    Mockito.when(mockContainerRequestContext.getProperty(UserAgentInfo.NAME)).thenReturn(agentInfo);
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-TRACKING")).thenReturn("guid=8b34ef1d1740a4d724970d78eec8ee4c");
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-ENDUSERCTX")).thenReturn(endUserCtxiPhone);

    IRequestScopeTracker requestTracker = Mockito.mock(IRequestScopeTracker.class);
    Mockito.when(mockContainerRequestContext.getProperty(IRequestScopeTracker.NAME)).thenReturn(requestTracker);

    Mockito.when(mockIEndUserContext.getIPAddress()).thenReturn("127.0.0.1");
    Mockito.when(mockIEndUserContext.getUserAgent()).thenReturn("ebayAndroid/6.2");

    Event event = new Event();
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=2&mkcid=1");
    // no referrer
    event.setReferrer("");
    assertTrue(collectionService.collectImpression(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, event));

    // encoded referrer
    event.setReferrer("https%3A%2F%2Fwww.google.com");
    assertTrue(collectionService.collectImpression(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, event));

    // no user agent
    Mockito.when(mockHttpServletRequest.getHeader("User-Agent")).thenReturn(null);
    assertTrue(collectionService.collectImpression(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, event));

    //non display channel
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=2&mkcid=1&gdpr=1");
    assertTrue(collectionService.collectImpression(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, event));

    //display channel no consent
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=2&mkcid=4&gdpr=1");
    assertTrue(collectionService.collectImpression(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, event));

     //display channel and valid consent
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=2&mkcid=4&gdpr=1&gdpr_consent=CO9hy3tO9hy3tKpAAAENAwCAAPJAAAAAAAAAALAAABAAAAAA.IGLtV_T9fb2vj-_Z99_tkeYwf95y3p-wzhheMs-8NyZeH_B4Wv2MyvBX4JiQKGRgksjLBAQdtHGlcTQgBwIlViTLMYk2MjzNKJrJEilsbO2dYGD9Pn8HT3ZCY70-vv__7v3ff_3g");
    assertTrue(collectionService.collectImpression(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, event));
  }

  @Test(expected = Exception.class)
  public void collectImpressionInvalidMkevt() throws Exception {
    HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
    IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
    RaptorSecureContext mockRaptorSecureContext = Mockito.mock(RaptorSecureContext.class);
    ContainerRequestContext mockContainerRequestContext = Mockito.mock(ContainerRequestContext.class);

    Event event = new Event();
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=2&mkcid=1");
    // invalid mkevt, exception
    event.setTargetUrl("http://mktcollectionsvc.vip.ebay.com/marketingtracking/v1/impression?mkevt=1&mkcid=1");
    collectionService.collectImpression(mockHttpServletRequest, mockIEndUserContext, mockRaptorSecureContext, mockContainerRequestContext, event);

  }

  @Test (expected = Exception.class)
  public void collectNotificationNoTracking() throws Exception {
    HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
    IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
    ContainerRequestContext mockContainerRequestContext = Mockito.mock(ContainerRequestContext.class);

    Event event = new Event();

    //no tracking, exception
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-TRACKING")).thenReturn(null);
    collectionService.collectNotification(mockHttpServletRequest, mockIEndUserContext, mockContainerRequestContext, event, 123);
  }

  @Test (expected = Exception.class)
  public void collectNotificationNoEndUser() throws Exception {
    HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
    IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
    ContainerRequestContext mockContainerRequestContext = Mockito.mock(ContainerRequestContext.class);

    Event event = new Event();

    // no enduserctx, exception
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-TRACKING")).thenReturn("guid=8b34ef1d1740a4d724970d78eec8ee4c");
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-ENDUSERCTX")).thenReturn(null);
    collectionService.collectNotification(mockHttpServletRequest, mockIEndUserContext, mockContainerRequestContext, event, 123);
  }

  @Test (expected = Exception.class)
  public void collectNotificationNoUA() throws Exception {
    HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
    IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
    ContainerRequestContext mockContainerRequestContext = Mockito.mock(ContainerRequestContext.class);

    Event event = new Event();

    // no user agent, exception
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-ENDUSERCTX")).thenReturn(endUserCtxNoReferer);
    Mockito.when(mockIEndUserContext.getUserAgent()).thenReturn(null);
    collectionService.collectNotification(mockHttpServletRequest, mockIEndUserContext, mockContainerRequestContext, event, 123);
  }

  @Test(expected = Exception.class)
  public void collectSyncNoTracking() throws Exception {
    HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
    IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
    RaptorSecureContext mockRaptorSecureContext = Mockito.mock(RaptorSecureContext.class);
    ContainerRequestContext mockContainerRequestContext = Mockito.mock(ContainerRequestContext.class);

    Event event = new Event();

    //no tracking, exception
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-TRACKING")).thenReturn(null);
    collectionService.collectSync(mockHttpServletRequest, mockRaptorSecureContext, mockContainerRequestContext, event);
  }

  @Test()
  public void collectSync() throws Exception {
    HttpServletRequest mockHttpServletRequest = Mockito.mock(HttpServletRequest.class);
    IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
    RaptorSecureContext mockRaptorSecureContext = Mockito.mock(RaptorSecureContext.class);
    ContainerRequestContext mockContainerRequestContext = Mockito.mock(ContainerRequestContext.class);

    UserAgentInfo agentInfo = new UserAgentInfo();
    agentInfo.setDesktop(true);
    Mockito.when(mockContainerRequestContext.getProperty(UserAgentInfo.NAME)).thenReturn(agentInfo);
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-TRACKING")).thenReturn("guid=8b34ef1d1740a4d724970d78eec8ee4c");
    Mockito.when(mockHttpServletRequest.getHeader("X-EBAY-C-ENDUSERCTX")).thenReturn(endUserCtxiPhone);

    IRequestScopeTracker requestTracker = Mockito.mock(IRequestScopeTracker.class);
    Mockito.when(mockContainerRequestContext.getProperty(IRequestScopeTracker.NAME)).thenReturn(requestTracker);

    Mockito.when(mockIEndUserContext.getIPAddress()).thenReturn("127.0.0.1");
    Mockito.when(mockIEndUserContext.getUserAgent()).thenReturn("ebayAndroid/6.2");

    Event event = new Event();
    // no parameters
    event.setTargetUrl("https://www.ebayadservices.com/marketingtracing/v1/sync");
    assertTrue(collectionService.collectSync(mockHttpServletRequest, mockRaptorSecureContext, mockContainerRequestContext, event));

    // valid
    event.setTargetUrl("https://www.ebayadservices.com/marketingtracing/v1/sync?guid=abc&u=123e12");
    assertTrue(collectionService.collectSync(mockHttpServletRequest, mockRaptorSecureContext, mockContainerRequestContext, event));
  }
}