package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingMessage;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Constants;
import com.ebay.app.raptor.chocolate.gen.model.UnifiedTrackingEvent;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.DDSResponse;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.platform.raptor.raptordds.parsers.UserAgentParser;
import com.ebay.raptor.domain.request.api.DomainRequestData;
import com.ebay.raptorio.request.tracing.RequestTracingContext;
import com.ebay.userlookup.UserLookup;
import com.ebay.userlookup.common.ClientException;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
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
    record = setDeviceInfo(record, agentInfo);
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
   * Parse message to unified tracking message
   * For user behavior events directly coming to MCS
   */
  public static UnifiedTrackingMessage parse(ContainerRequestContext requestContext, HttpServletRequest request,
                                             IEndUserContext endUserContext, UserAgentInfo agentInfo,
                                             MultiValueMap<String, String> parameters, String url, String referer,
                                             ChannelType channelType, ChannelAction channelAction) {
    Map<String, String> payload = new HashMap<>();

    // set default value
    UnifiedTrackingMessage record = new UnifiedTrackingMessage("", "", 0L, 0L,
        "", "", 0L, "", 0L, "", "", "", "",
        "", "", "", "", "", 0, "", "", "",
        "", "", "", "", "", "", "", "",
        "", "", "", 0, 0, false, payload);

    DomainRequestData domainRequest = (DomainRequestData) requestContext.getProperty(DomainRequestData.NAME);
    RequestTracingContext tracingContext = (RequestTracingContext) requestContext.getProperty(RequestTracingContext.NAME);

    // event id
    record.setEventId(UUID.randomUUID().toString());
    record.setProducerEventId(getProducerEventId());

    // event timestamp
    record.setEventTs(System.currentTimeMillis());
    record.setProducerEventTs(request.getSession().getCreationTime());

    // rlog id
    record.setRlogId(tracingContext.getRlogId());

    // tracking id
    record.setTrackingId(parameters.getFirst(Constants.SEND_TRACKING_ID));

    // user id
    String bu = parameters.getFirst(Constants.BEST_GUESS_USER);
    if (!StringUtils.isEmpty(bu)) {
      record.setEncryptedUserId(Long.parseLong(bu));
      Long uerId = EncryptUtil.decryptUserId(Long.parseLong(bu));
      record.setUserId(uerId);
//      record.setPublicUserId(getPublicUserId(uerId));
    }

    // guid
    String trackingHeader = request.getHeader("X-EBAY-C-TRACKING");
    String guid = UrlUtil.getHeaderValue(trackingHeader, Constants.GUID);
    if (guid != null) {
      record.setGuid(guid);
    }

    // device and app info
//    record.setIdfa(event.getIdfa());
//    record.setGadid(event.getGadid());
    record.setUserAgent(endUserContext.getUserAgent());
    record = setDeviceInfo(record, agentInfo);
//    record.setDeviceId(event.getDeviceId());
//    if (agentInfo.getAppInfo() != null) {
//      record.setAppVersion(agentInfo.getAppInfo().getAppVersion());
//    }

    // channel type
    record.setChannelType(channelType.toString());

    // action type
    record.setActionType(channelAction.toString());

    // partner id
    record.setPartnerId(getPartnerId());

    // campaign id
    record.setCampaignId(getCampaignId());

    // site id
    record.setSiteId(domainRequest.getSiteId());

    // url
    record.setUrl(url);

    // referer
    record.setReferer(referer);

    // service
    record.setService("CHOCOLATE");

    // server
    record.setServer(domainRequest.getHost());

    // remote ip
    record.setRemoteIp(UrlUtil.getRemoteIp(request));

    // page id
    record.setPageId(PageIdEnum.CLICK.getId());

    // user geo id
    record.setGeoId(Integer.parseInt(UrlUtil.parseTagFromParams(parameters, Constants.CHOCO_BUYER_ACCESS_SITE_ID)));

    // payload
    record.setPayload(payload);

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
   * Get producer event id
   */
  private static String getProducerEventId() {
    return "";
  }

  /**
   * Get public user id
   */
  private static String getPublicUserId(Long userId) {
    String publicUserId = "";

    try {
      new UserLookup().getPublicUserId(userId);
    } catch (ClientException e) {
      logger.warn("Get public user id error.", e);
    }

    return publicUserId;
  }

  /**
   * Get partner id
   * EPN - publisher id, mapping from campaign id
   * Display - partner id
   * Customer Marketing - partner id is in parameter 'mkpid'
   */
  private static String getPartnerId() {
    return "";
  }

  /**
   * Get campaign id
   */
  private static String getCampaignId() {
    return "";
  }

  /**
   * Set device info
   */
  private static UnifiedTrackingMessage setDeviceInfo(UnifiedTrackingMessage record, UserAgentInfo agentInfo) {
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

    return record;
  }

  /**
   * Get application payload
   */
  private Map<String, String> getPayload(Map<String, String> payload) {
    return payload;
  }
}
