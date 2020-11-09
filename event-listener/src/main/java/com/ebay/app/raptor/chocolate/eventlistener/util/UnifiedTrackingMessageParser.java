package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingMessage;
import com.ebay.app.raptor.chocolate.gen.model.UnifiedTrackingEvent;
import com.ebay.platform.raptor.ddsmodels.DDSResponse;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.platform.raptor.raptordds.parsers.UserAgentParser;
import com.ebay.userlookup.UserLookup;
import com.ebay.userlookup.common.ClientException;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by jialili1 on 11/5/20
 */
public class UnifiedTrackingMessageParser {
  private static final Logger logger = LoggerFactory.getLogger(UnifiedTrackingMessageParser.class);
  private Metrics metrics = ESMetrics.getInstance();

  private UnifiedTrackingMessageParser() {}

  /**
   * Parse message to unified tracking message
   * For UEP cases
   */
  public static UnifiedTrackingMessage parse(UnifiedTrackingEvent event) {
    Map<String, String> payload = new HashMap<>();

    // set default value
    UnifiedTrackingMessage record = new UnifiedTrackingMessage("", "", 0L, 0L,
        "", "", 0L, "", 0L, "", "", "", "",
        "", "", "", "", "", 0, "", "", "",
        "", "", "", "", "", "", "", "",
        "", "", "", 0, 0, false, payload);

    // event id
    record.setEventId(UUID.randomUUID().toString());
    record.setProducerEventId(event.getProducerEventId());

    // event timestamp
    record.setEventTs(System.currentTimeMillis());
    record.setProducerEventTs(event.getProducerEventTs());

    record.setRlogId(event.getRlogId());
    record.setTrackingId(event.getTrackingId());

    // user id
    record.setUserId(Long.parseLong(event.getUserId()));
    record.setPublicUserId(event.getUserId());
    record.setEncryptedUserId(Long.parseLong(event.getEncryptedUserId()));

    // guid
    record.setGuid(event.getGuid());

    // device and app info
    record.setIdfa(event.getIdfa());
    record.setGadid(event.getGadid());
    record.setUserAgent(event.getUserAgent());
    UserAgentInfo agentInfo = new UserAgentParser().parse(event.getUserAgent());
    DDSResponse deviceInfo = agentInfo.getDeviceInfo();
    if (deviceInfo != null) {
      record.setDeviceFamily(getDeviceFamily(deviceInfo));
      record.setDeviceType(deviceInfo.getOsName());
      record.setBrowserFamily(deviceInfo.getBrowser());
      record.setBrowserVersion(deviceInfo.getBrowserVersion());
      record.setOsFamily(deviceInfo.getDeviceOS());
      record.setOsVersion(deviceInfo.getDeviceOSVersion());
      String appId = CollectionServiceUtil.getAppIdFromUserAgent(agentInfo);
      record.setAppId(appId);
    }
    record.setDeviceId(event.getDeviceId());
    if (agentInfo.getAppInfo() != null) {
      record.setAppVersion(agentInfo.getAppInfo().getAppVersion());
    }

    // channel type
    record.setChannelType(event.getChannelType());

    // action type
    record.setActionType(event.getActionType());

    // partner id
    record.setPartnerId(event.getPartnerId());

    // campaign id
    record.setCampaignId(event.getCampaignId());

    // site id
    record.setSiteId(event.getSiteId());

    // url
    record.setUrl(event.getUrl());

    // referer
    record.setReferer(event.getReferer());

    // service
    record.setService(event.getService());

    // server
    record.setServer(event.getServer());

    // remote ip
    record.setRemoteIp(event.getRemoteIp());

    // page id
    record.setPageId(event.getPageId());

    // user geo id
    record.setGeoId(event.getGeoId());

    // payload
    record.setPayload(event.getPayload());

    return record;
  }



  /**
   * Get device family
   */
  private static String getDeviceFamily(DDSResponse deviceInfo) {
    String deviceFamily;

    if (deviceInfo.isTablet()) {
      deviceFamily = "Tablet";
    } else if (deviceInfo.isTouchScreen()) {
      deviceFamily = "TouchScreen";
    } else if (deviceInfo.isDesktop()) {
      deviceFamily = "Desktop";
    } else if (deviceInfo.isMobile()) {
      deviceFamily = "Mobile";
    } else {
      deviceFamily = "Other";
    }

    return deviceFamily;
  }

  /**
   * Get public user id
   */
  private static String getPublicUserId(String userId) {
    String publicUserId = "";

    try {
      new UserLookup().getPublicUserId(Long.parseLong(userId));
    } catch (ClientException e) {
      logger.warn("Get public user id error.", e);
    }

    return publicUserId;
  }
}
