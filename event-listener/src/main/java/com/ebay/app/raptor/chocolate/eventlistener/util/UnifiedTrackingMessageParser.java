package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingMessage;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.gen.model.UnifiedTrackingEvent;
import com.ebay.kernel.presentation.constants.PresentationConstants;
import com.ebay.kernel.util.FastURLEncoder;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.platform.raptor.raptordds.parsers.UserAgentParser;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.raptor.domain.request.api.DomainRequestData;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import com.ebay.raptorio.request.tracing.RequestTracingContext;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ServiceEnum;
import com.ebay.traffic.monitoring.Field;
import com.ebay.userlookup.UserLookup;
import com.ebay.userlookup.common.ClientException;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.apache.commons.lang.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.*;

/**
 * Created by jialili1 on 11/5/20
 */
public class UnifiedTrackingMessageParser {
  private static final Logger logger = LoggerFactory.getLogger(UnifiedTrackingMessageParser.class);
  private static Metrics metrics = ESMetrics.getInstance();
  private static CobrandParser cobrandParser = new CobrandParser();

  private UnifiedTrackingMessageParser() {}

  /**
   * Parse message to unified tracking message
   * For UEP cases
   */
  public static UnifiedTrackingMessage parse(UnifiedTrackingEvent event, UserLookup userLookup) {
    Map<String, String> payload = new HashMap<>();

    // set default value
    UnifiedTrackingMessage record = setDefaultAndCommonValues(payload, new UserAgentParser().parse(event.getUserAgent()));

    // event id
    record.setProducerEventId(event.getProducerEventId());

    // event timestamp
    record.setProducerEventTs(event.getProducerEventTs());

    record.setRlogId(event.getRlogId());
    record.setTrackingId(event.getTrackingId());

    // user id
    record.setUserId(event.getUserId());
    record.setPublicUserId(event.getPublicUserId());
    record.setEncryptedUserId(event.getEncryptedUserId());

    // guid
    record.setGuid(event.getGuid());

    // device info
    record.setIdfa(event.getIdfa());
    record.setGadid(event.getGadid());
    record.setDeviceId(event.getDeviceId());
    record.setUserAgent(event.getUserAgent());

    // channel type
    record.setChannelType(event.getChannelType());

    // action type
    record.setActionType(event.getActionType());

    // partner id
    record.setPartner(event.getPartner());

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
                                             IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext,
                                             UserAgentInfo agentInfo, UserLookup userLookup,
                                             MultiValueMap<String, String> parameters, String url, String referer,
                                             ChannelType channelType, ChannelAction channelAction) {
    Map<String, String> payload = new HashMap<>();

    // set default value
    UnifiedTrackingMessage record = setDefaultAndCommonValues(payload, agentInfo);

    DomainRequestData domainRequest = (DomainRequestData) requestContext.getProperty(DomainRequestData.NAME);
    RequestTracingContext tracingContext = (RequestTracingContext) requestContext.getProperty(RequestTracingContext.NAME);

    // event id
    record.setProducerEventId(getProducerEventId(parameters, channelType));

    // event timestamp
    record.setProducerEventTs(request.getSession().getCreationTime());

    // rlog id
    record.setRlogId(tracingContext.getRlogId());

    // tracking id
    record.setTrackingId(parameters.getFirst(Constants.TRACKING_ID));

    // user id
    String bu = parameters.getFirst(Constants.BEST_GUESS_USER);
    if (!StringUtils.isEmpty(bu)) {
      record.setEncryptedUserId(Long.parseLong(bu));
    }
    long userId = getUserId(raptorSecureContext, endUserContext, bu, channelType);
    record.setUserId(userId);
//      record.setPublicUserId(getPublicUserId(userLookup, uerId));

    // guid
    String trackingHeader = request.getHeader("X-EBAY-C-TRACKING");
    String guid = HttpRequestUtil.getHeaderValue(trackingHeader, Constants.GUID);
    if (guid != null) {
      record.setGuid(guid);
    }

    // device info
//    record.setIdfa(event.getIdfa());
//    record.setGadid(event.getGadid());
    record.setDeviceId(endUserContext.getDeviceId());
    String userAgent = endUserContext.getUserAgent();
    record.setUserAgent(userAgent);

    // channel type
    record.setChannelType(channelType.toString());

    // action type
    record.setActionType(getActionType(channelAction));

    // partner id
    record.setPartner(getPartner(parameters, channelType));

    // campaign id
    record.setCampaignId(getCampaignId(parameters, channelType));

    // rotation id
    record.setRotationId(getRotationId(parameters));

    // site id
    record.setSiteId(domainRequest.getSiteId());

    // url
    record.setUrl(removeBsParam(parameters, url));

    // referer
    record.setReferer(referer);

    // service
    record.setService(ServiceEnum.CHOCOLATE.getValue());

    // server
    record.setServer(domainRequest.getHost());

    // remote ip
    record.setRemoteIp(HttpRequestUtil.getRemoteIp(request));

    // page id
    record.setPageId(PageIdEnum.getPageIdByAction(channelAction));

    // user geo id
    record.setGeoId(getGeoID(requestContext, parameters, channelType, channelAction));

    // payload
    String appId = CollectionServiceUtil.getAppIdFromUserAgent(agentInfo);
    record.setPayload(getPayload(payload, parameters, requestContext, url, userAgent, appId, channelType, channelAction));

    return record;
  }

  /**
   * Parse common logic
   */
  private static UnifiedTrackingMessage setDefaultAndCommonValues( Map<String, String> payload, UserAgentInfo agentInfo) {
    // set default value
    UnifiedTrackingMessage record = new UnifiedTrackingMessage("", "", 0L, 0L,
        "", "", 0L, "", 0L, "", "", "", "",
        "", "", "", "", "", 0, "", "", "",
        "", "", "", "", "", "", "", "",
        "", "", "", 0, 0, false, payload);

    // event id
    record.setEventId(UUID.randomUUID().toString());

    // event timestamp
    record.setEventTs(System.currentTimeMillis());

    // device info
    DeviceInfoParser deviceInfoParser = new DeviceInfoParser().parse(agentInfo);
    record.setDeviceFamily(deviceInfoParser.getDeviceFamily());
    record.setDeviceType(deviceInfoParser.getDeviceType());
    record.setBrowserFamily(deviceInfoParser.getBrowserFamily());
    record.setBrowserVersion(deviceInfoParser.getBrowserVersion());
    record.setOsFamily(deviceInfoParser.getOsFamily());
    record.setOsVersion(deviceInfoParser.getOsVersion());

    // app info
    record.setAppId(CollectionServiceUtil.getAppIdFromUserAgent(agentInfo));
    if (agentInfo.getAppInfo() != null) {
      record.setAppVersion(agentInfo.getAppInfo().getAppVersion());
    }

    return record;
  }

  /**
   * Get action type
   */
  private static String getActionType(ChannelAction channelAction) {
    if (ChannelAction.EMAIL_OPEN.equals(channelAction)) {
      return ActionTypeEnum.OPEN.getValue();
    }

    return channelAction.toString();
  }

  /**
   * Get producer event id
   */
  private static String getProducerEventId(MultiValueMap<String, String> parameters, ChannelType channelType) {
    if (ChannelType.SITE_EMAIL.equals(channelType)) {
      return parameters.getFirst(Constants.EMAIL_UNIQUE_ID);
    }

    return "";
  }

  /**
   * Get public user id
   */
  private static String getPublicUserId(UserLookup userLookup, Long userId) {
    String publicUserId = "";

    try {
      publicUserId = userLookup.getPublicUserId(userId);
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
  private static String getPartner(MultiValueMap<String, String> parameters, ChannelType channelType) {
    String partner = "";
    if (ChannelType.EPN.equals(channelType)) {
      if (StringUtils.isNumeric(parameters.getFirst(Constants.CAMPID))) {
        // Do we really need to get publisher id here?
      }
    } else if (ChannelType.PAID_SEARCH.equals(channelType)) {
      // partner definition unknown
    } else if (ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.MRKT_EMAIL.equals(channelType)) {
      partner = parameters.getFirst(Constants.MKPID);
    }

    return partner;
  }

  /**
   * Get campaign id
   */
  private static String getCampaignId(MultiValueMap<String, String> parameters, ChannelType channelType) {
    String campaignId = "";
    if (ChannelType.EPN.equals(channelType)) {
      if (StringUtils.isNumeric(parameters.getFirst(Constants.CAMPID))) {
        campaignId = parameters.getFirst(Constants.CAMPID);
      }
    } else if (ChannelType.PAID_SEARCH.equals(channelType)) {
      campaignId = parameters.getFirst(Constants.CAMPAIGN_ID);
    } else if (ChannelType.SITE_EMAIL.equals(channelType)) {
      campaignId = StringUtils.substringBetween(parameters.getFirst(Constants.SOURCE_ID), "e", ".");
    } else if (ChannelType.MRKT_EMAIL.equals(channelType)) {
      if (!StringUtils.isEmpty(parameters.getFirst(Constants.SEGMENT_NAME))) {
        campaignId = parameters.getFirst(Constants.SEGMENT_NAME).trim();
      }
    }

    return campaignId;
  }

  /**
   * Get rotation id
   */
  private static String getRotationId(MultiValueMap<String, String> parameters) {
    String rotationId = "";
    if (parameters.containsKey(Constants.MKRID) && parameters.get(Constants.MKRID).get(0) != null) {
      try {
        String rawRotationId = parameters.get(Constants.MKRID).get(0);
        // decode rotationId if rotation is encoded
        // add decodeCnt to avoid looping infinitely
        int decodeCnt = 0;
        while (rawRotationId.contains("%") && decodeCnt<5) {
          rawRotationId = URLDecoder.decode(rawRotationId, "UTF-8");
          decodeCnt = decodeCnt + 1;
        }
        rotationId = rawRotationId.replaceAll("-", "");
      } catch (Exception e) {
        logger.warn(Errors.ERROR_INVALID_MKRID);
      }
    } else {
      logger.warn(Errors.ERROR_NO_MKRID);
    }

    return rotationId;
  }

  /**
   * Get geo id
   */
  private static int getGeoID(ContainerRequestContext requestContext, MultiValueMap<String, String> parameters,
                              ChannelType channelType, ChannelAction channelAction) {
    int geoId;

    if (ChannelAction.EMAIL_OPEN.equals(channelAction)) {
      geoId = Integer.parseInt(HttpRequestUtil.parseTagFromParams(parameters, Constants.CHOCO_BUYER_ACCESS_SITE_ID));
    } else {
      UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);
      geoId = userPrefsCtx.getGeoContext().getCountryId();
    }

    return geoId;
  }

  /**
   * Get user id
   */
  private static long getUserId(RaptorSecureContext raptorSecureContext, IEndUserContext endUserContext,
                                String bu, ChannelType channelType) {
    if (ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.MRKT_EMAIL.equals(channelType)) {
      if (!StringUtils.isEmpty(bu)) {
        return EncryptUtil.decryptUserId(Long.parseLong(bu));
      }
    } else {
      if ("EBAYUSER".equals(raptorSecureContext.getSubjectDomain())) {
        return Long.parseLong(raptorSecureContext.getSubjectImmutableId());
      } else {
        return endUserContext.getOrigUserOracleId();
      }
    }

    return 0;
  }

  /**
   * Get payload
   */
  private static Map<String, String> getPayload(Map<String, String> payload, MultiValueMap<String, String> parameters,
                                                ContainerRequestContext requestContext, String url, String userAgent,
                                                String appId, ChannelType channelType, ChannelAction channelAction) {
    // add tags from parameters
    for (Map.Entry<String, String> entry : Constants.emailTagParamMap.entrySet()) {
      if (parameters.containsKey(entry.getValue()) && parameters.getFirst(entry.getValue()) != null) {
        payload.put(entry.getKey(), HttpRequestUtil.parseTagFromParams(parameters, entry.getValue()));
      }
    }

    // add tags in url param "sojTags" into applicationPayload
    payload = addSojTags(payload, parameters, channelType, channelAction);

    // add other tags
    // buyer access site id
    if (ChannelAction.EMAIL_OPEN.equals(channelAction)) {
      payload.put("bs", HttpRequestUtil.parseTagFromParams(parameters, Constants.CHOCO_BUYER_ACCESS_SITE_ID));
    } else {
      UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);
      payload.put("bs", String.valueOf(userPrefsCtx.getGeoContext().getSiteId()));
    }

    // cobrand
    payload.put("cobrand", cobrandParser.parse(appId, userAgent));

    // facebook prefetch
    if (isFacebookPrefetchEnabled(requestContext)) {
      payload.put("fbprefetch", "true");
    }

    // landing page and tracking url
    payload.put("url_mpre", url);

    return encodeTags(deleteNullOrEmptyValue(payload));
  }

  /**
   * Add tags in param sojTags
   */
  private static Map<String, String> addSojTags(Map<String, String> applicationPayload, MultiValueMap<String, String> parameters,
                                         ChannelType channelType, ChannelAction channelAction) {
    if(parameters.containsKey(Constants.SOJ_TAGS) && parameters.get(Constants.SOJ_TAGS).get(0) != null) {
      String sojTags = parameters.get(Constants.SOJ_TAGS).get(0);
      try {
        sojTags = URLDecoder.decode(sojTags, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        logger.warn("Param sojTags is wrongly encoded", e);
        metrics.meter("ErrorEncodedSojTags", 1, Field.of(Constants.CHANNEL_ACTION, channelAction.toString()),
            Field.of(Constants.CHANNEL_TYPE, channelType.toString()));
      }
      if (!org.springframework.util.StringUtils.isEmpty(sojTags)) {
        StringTokenizer stToken = new StringTokenizer(sojTags, PresentationConstants.COMMA);
        while (stToken.hasMoreTokens()) {
          StringTokenizer sojNvp = new StringTokenizer(stToken.nextToken(), PresentationConstants.EQUALS);
          if (sojNvp.countTokens() == 2) {
            String sojTag = sojNvp.nextToken().trim();
            String urlParam = sojNvp.nextToken().trim();
            if (!org.springframework.util.StringUtils.isEmpty(urlParam) && !org.springframework.util.StringUtils.isEmpty(sojTag)) {
              applicationPayload.put(sojTag, HttpRequestUtil.parseTagFromParams(parameters, urlParam));
            }
          }
        }
      }
    }

    return applicationPayload;
  }

  /**
   * Soj tag fbprefetch
   */
  private static boolean isFacebookPrefetchEnabled(ContainerRequestContext requestContext) {
    String facebookprefetch = requestContext.getHeaderString("X-Purpose");
    if (facebookprefetch != null && facebookprefetch.trim().equals("preview")) {
      return true;
    }
    return false;
  }

  /**
   * Encode tags
   */
  private static Map<String, String> encodeTags(Map<String, String> inputMap) {
    Map<String, String> outputMap = new HashMap<>();
    for (Map.Entry<String, String> entry : inputMap.entrySet()) {
      outputMap.put(entry.getKey(), FastURLEncoder.encode(entry.getValue(), "UTF-8"));
    }

    return outputMap;
  }

  /**
   * Delete map entry with null or empty value
   */
  private static Map<String, String> deleteNullOrEmptyValue(Map<String, String> map) {
    Set<Map.Entry<String, String>> entrySet = map.entrySet();
    Iterator<Map.Entry<String, String>> iterator = entrySet.iterator();

    while(iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      if (org.springframework.util.StringUtils.isEmpty(entry.getValue())) {
        iterator.remove();
      }
    }

    return map;
  }

  /**
   * Remove choco_bs param if it exists
   */
  private static String removeBsParam(MultiValueMap<String, String> parameters, String uri) {
    if (parameters.containsKey(Constants.CHOCO_BUYER_ACCESS_SITE_ID)) {
      try {
        uri = HttpRequestUtil.removeParam(uri, Constants.CHOCO_BUYER_ACCESS_SITE_ID);
      } catch (URISyntaxException e) {
        logger.warn("Error when deleting choco_bs", e);
      }
    }

    return uri;
  }
}
