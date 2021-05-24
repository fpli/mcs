/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.gen.model;

import org.apache.avro.generic.GenericData;
import org.apache.commons.lang.SerializationUtils;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class GenObjTest {

  @Test
  public void testUTPEvent() {
    UnifiedTrackingEvent event1 = new UnifiedTrackingEvent();
    event1.setActionType("CLICK");
    event1.setChannelType("SITE_EMAIL");
    event1.setReferer("");
    event1.setUrl("https://www.ebay.com/?mkevt=1");
    event1.setPartner("1");
    event1.setCampaignId("123");
    event1.setDeviceId("abc");
    event1.setEncryptedUserId(12344L);
    event1.setGadid("abc");
    event1.setGeoId(1);
    event1.setGuid("abc");
    event1.setIdfa("def");
    event1.setPageId(24555);
    Map<String, String> payload = new HashMap<>();
    payload.put("g","abbb");
    event1.setPayload(payload);
    event1.setProducerEventId("22222");
    event1.setProducerEventTs(12123123L);
    event1.setPublicUserId("asdfa");
    event1.setRemoteIp("127.0.0.1");
    event1.setRlogId("xxxx");
    event1.setServer("localhost");
    event1.setService("chocolate");
    event1.setSiteId(1);
    event1.setTrackingId("uuid");
    event1.setUserAgent("iphone");
    event1.setUserId(1212L);

    UnifiedTrackingEvent event2 = (UnifiedTrackingEvent)SerializationUtils.clone(event1);
    assertEquals(event1, event2);
    assertEquals(event1.hashCode(), event2.hashCode());
  }

  @Test
  public void testErrorData() {
    ErrorData errorData = new ErrorData();
    errorData.setErrorId(new BigDecimal(123));
    errorData.setDomain(ErrorData.DomainEnum.MARKETING);
    errorData.setSubdomain(ErrorData.SubdomainEnum.TRACKING);
    errorData.setCategory(ErrorData.CategoryEnum.APPLICATION);
    errorData.setMessage("error");
    errorData.setLongMessage("big error");
    errorData.setInputRefIds(new ArrayList<>());
    errorData.setOutputRefIds(new ArrayList<>());
    errorData.setParameters(new ArrayList<>());

    System.out.println(errorData.hashCode());
    ErrorData errorData2 = (ErrorData) SerializationUtils.clone(errorData);
    assertEquals(errorData, errorData2);
    assertEquals(errorData, errorData);
    assertNotEquals(errorData, null);
    System.out.println(errorData.toString());
  }

  @Test
  public void testEvent() {
    Event event1 = new Event();
    event1.setReferrer("https://www.google.com");
    event1.setTargetUrl("https://www.ebay.com/?mkevt=1");
    Event event2 = (Event) SerializationUtils.clone(event1);
    assertEquals(event1, event2);
    assertEquals(event1, event1);
    assertNotEquals(null, event1);
    System.out.println(event1);
  }

  @Test
  public void testROIEvent() {
    ROIEvent roiEvent1 = new ROIEvent();
    roiEvent1.setTransactionTimestamp("1620920179178");
    roiEvent1.setItemId("21312121");
    roiEvent1.setUniqueTransactionId("4232664");
    roiEvent1.setTransType("Bin");
    Map<String, String> payload = new HashMap<>();
    payload.put("mmpid", "91");
    roiEvent1.setPayload(payload);
    ROIEvent roiEvent2 = (ROIEvent) SerializationUtils.clone(roiEvent1);
    assertEquals(roiEvent1, roiEvent2);
    assertEquals(roiEvent1, roiEvent1);
    assertNotEquals(roiEvent1, null);
    System.out.println(roiEvent1.toString());

  }

}