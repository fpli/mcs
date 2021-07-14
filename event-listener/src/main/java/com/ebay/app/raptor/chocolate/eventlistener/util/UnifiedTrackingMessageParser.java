package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.traffic.chocolate.utp.common.model.UnifiedTrackingMessage;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.app.raptor.chocolate.gen.model.UnifiedTrackingEvent;
import com.ebay.app.raptor.chocolate.util.EncryptUtil;
import com.ebay.app.raptor.chocolate.utp.UepPayloadHelper;
import com.ebay.kernel.presentation.constants.PresentationConstants;
import com.ebay.kernel.util.FastURLEncoder;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.platform.raptor.raptordds.parsers.UserAgentParser;
import com.ebay.raptor.domain.request.api.DomainRequestData;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import com.ebay.raptorio.request.tracing.RequestTracingContext;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ServiceEnum;
import com.ebay.traffic.chocolate.utp.common.EmailPartnerIdEnum;
import com.ebay.traffic.chocolate.utp.lib.UnifiedTrackerFactory;
import com.ebay.traffic.chocolate.utp.lib.cache.TrackingGovernanceTagCache;
import com.ebay.traffic.chocolate.utp.lib.constants.EnvironmentEnum;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.apache.commons.lang.StringUtils;

import javax.ws.rs.container.ContainerRequestContext;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.*;

import static com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil.isLongNumeric;

/**
 * Created by jialili1 on 11/5/20
 */
public class UnifiedTrackingMessageParser {
  private static final Logger logger = LoggerFactory.getLogger(UnifiedTrackingMessageParser.class);
  private static Metrics metrics = ESMetrics.getInstance();
  private static CobrandParser cobrandParser = new CobrandParser();
  private static UepPayloadHelper uepPayloadHelper = new UepPayloadHelper();

  public UnifiedTrackingMessageParser() throws Exception {
    UnifiedTrackerFactory.getUnifiedTracker(getEnv());
  }

  /**
   * Parse message to unified tracking message
   * For UEP cases
   */
  public static UnifiedTrackingMessage parse(UnifiedTrackingEvent event) {
    logger.debug(event.toString());

    Map<String, String> payload = new HashMap<>();

    // set default value
    long eventTs = System.currentTimeMillis();
    UnifiedTrackingMessage record = setDefaultAndCommonValues(payload,
        new UserAgentParser().parse(event.getUserAgent()), eventTs);

    // event id
    record.setProducerEventId(coalesce(event.getProducerEventId(), ""));

    // event timestamp
    record.setProducerEventTs(coalesce(event.getProducerEventTs(), eventTs));

    // rlogid
    record.setRlogId(event.getRlogId());

    // tracking id
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
    record.setPayload(deleteNullOrEmptyValue(event.getPayload()));

    return record;
  }

  /**
   * Bot detection by device type
   *
   * @param userAgent user agent
   */
  public static boolean isBot(String userAgent) {
    // isBot. Basic bot detection by user agent.
    if (StringUtils.isNotEmpty(userAgent) && (
        userAgent.toLowerCase().contains("bot") ||
            userAgent.toLowerCase().contains("proxy") ||
            userAgent.toLowerCase().contains("spider")
    )) {
      return true;
    }
    return false;
  }

  /**
   * Parse chocolate tracked user behavior into utp message
   * @param baseEvent             base event
   * @param requestContext        request context
   * @param snapshotId            PM snapshot id
   * @param shortSnapshotId       PM short snapshot id
   * @return                      utp event
   */

  public static UnifiedTrackingMessage parse(BaseEvent baseEvent, ContainerRequestContext requestContext,
                                             long snapshotId, long shortSnapshotId) {
    Map<String, String> payload = new HashMap<>();

    // set default value
    UnifiedTrackingMessage record = setDefaultAndCommonValues(payload, baseEvent.getUserAgentInfo(),
        baseEvent.getTimestamp());

    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);
    payload.put("lang_cd", userPrefsCtx.getLangLocale().toLanguageTag());

    DomainRequestData domainRequest = (DomainRequestData) requestContext.getProperty(DomainRequestData.NAME);
    RequestTracingContext tracingContext =
        (RequestTracingContext) requestContext.getProperty(RequestTracingContext.NAME);

    // event id
    record.setProducerEventId(getProducerEventId(baseEvent.getUrlParameters(),
        baseEvent.getChannelType().getLogicalChannel().getAvro()));

    // event timestamp
    record.setProducerEventTs(getProducerEventTs(baseEvent.getActionType().getAvro(), baseEvent.getRoiEvent(),
        baseEvent.getTimestamp()));

    // rlog id
    record.setRlogId(tracingContext.getRlogId());

    MultiValueMap<String, String> parameters = baseEvent.getUrlParameters();

    // tracking id
    record.setTrackingId(HttpRequestUtil.parseFromTwoParams(parameters, UepPayloadHelper.TRACKING_ID,
        UepPayloadHelper.TRACKING_ID_S));

    // user id
    String bu = baseEvent.getUrlParameters().getFirst(Constants.BEST_GUESS_USER);
    if (!StringUtils.isEmpty(bu)) {
      record.setEncryptedUserId(Long.parseLong(bu));
    }

    long userId = getUserId(baseEvent.getEndUserContext(), bu,
        baseEvent.getChannelType().getLogicalChannel().getAvro());
    record.setUserId(userId);

    // guid
    String trackingHeader = baseEvent.getRequestHeaders().get("X-EBAY-C-TRACKING");
    String guid = HttpRequestUtil.getHeaderValue(trackingHeader, Constants.GUID);
    if (guid != null) {
      record.setGuid(guid);
    }

    // device info
//    record.setIdfa(event.getIdfa());
//    record.setGadid(event.getGadid());
    record.setDeviceId(baseEvent.getEndUserContext().getDeviceId());
    String userAgent = baseEvent.getEndUserContext().getUserAgent();
    record.setUserAgent(userAgent);

    // channel type
    ChannelTypeEnum channelTypeEnum = getChannelType(baseEvent.getChannelType().getLogicalChannel().getAvro());
    record.setChannelType(channelTypeEnum.getValue());

    // action type
    String actionType = getActionType(baseEvent.getActionType().getAvro());
    record.setActionType(actionType);

    // partner id
    record.setPartner(getPartner(parameters, baseEvent.getChannelType().getLogicalChannel().getAvro()));

    // campaign id
    record.setCampaignId(getCampaignId(parameters, baseEvent.getChannelType().getLogicalChannel().getAvro()));

    // rotation id
    String rotationId = getRotationId(parameters);
    record.setRotationId(rotationId);

    // site id
    record.setSiteId(domainRequest.getSiteId());

    // url
    record.setUrl(removeBsParam(parameters, baseEvent.getUrl()));

    // referer
    record.setReferer(baseEvent.getReferer());

    // service. Rover send all clicks covered in IMK TFS to chocolate.
    if(baseEvent.getUrl().startsWith("https://rover.ebay.com")
        || baseEvent.getUrl().startsWith("http://rover.ebay.com")) {
      record.setService(ServiceEnum.ROVER.getValue());
    } else {
      record.setService(ServiceEnum.CHOCOLATE.getValue());
    }

    // server
    record.setServer(domainRequest.getHost());

    // remote ip
    record.setRemoteIp(baseEvent.getEndUserContext().getIPAddress());

    // page id
    int pageId = PageIdEnum.getPageIdByAction(baseEvent.getActionType().getAvro());
    record.setPageId(pageId);

    // user geo id
    record.setGeoId(getGeoID(requestContext));

    // isBot. Basic bot detection by user agent.
    record.setIsBot(isBot(userAgent));

    // payload
    String appId = CollectionServiceUtil.getAppIdFromUserAgent(baseEvent.getUserAgentInfo());
    // format UEP payload
    Map<String, String> uepPayload =
        uepPayloadHelper.getUepPayload(baseEvent.getUrl(), ActionTypeEnum.valueOf(actionType), channelTypeEnum);
    Map<String, String> fullPayload =
        getPayload(payload, baseEvent, parameters, appId,
            baseEvent.getChannelType().getLogicalChannel().getAvro(), baseEvent.getActionType().getAvro(), snapshotId,
            shortSnapshotId, baseEvent.getRoiEvent(), userId, trackingHeader);

    // append UEP payload
    if (uepPayload != null && uepPayload.size() > 0) {
      fullPayload.putAll(uepPayload);
    }
    record.setPayload(deleteNullOrEmptyValue(fullPayload));

    // data governance
    long startTs = System.currentTimeMillis();
    TrackingGovernanceTagCache.getInstance().govern(record);
    metrics.mean("DataGovernanceLatency", System.currentTimeMillis() - startTs);

    return record;
  }

  private static ChannelTypeEnum getChannelType(ChannelType channelType) {
    switch (channelType) {
      case PAID_SEARCH:
        return ChannelTypeEnum.PLA;
      case DISPLAY:
        return ChannelTypeEnum.DISPLAY;
      case SOCIAL_MEDIA:
        return ChannelTypeEnum.SOCIAL;
      case PAID_SOCIAL:
        return ChannelTypeEnum.SOCIAL;
      case SEARCH_ENGINE_FREE_LISTINGS:
        return ChannelTypeEnum.SEARCH_ENGINE_FREE_LISTINGS;
      case MRKT_EMAIL:
        return ChannelTypeEnum.MRKT_EMAIL;
      case SITE_EMAIL:
        return ChannelTypeEnum.SITE_EMAIL;
      case EPN:
        return ChannelTypeEnum.EPN;
      default:
        return ChannelTypeEnum.GENERIC;
    }
  }

  /**
   * Parse common logic
   */
  private static UnifiedTrackingMessage setDefaultAndCommonValues(Map<String, String> payload, UserAgentInfo agentInfo, long eventTs) {
    // set default value
    UnifiedTrackingMessage record = new UnifiedTrackingMessage("", "", 0L, 0L,
        null, null, 0L, null, 0L, null, null, null,
        null, null, null, null, null, null, 0, null,
        null, null, null, null, null, null, null,
        null, null, null, null, null, null, 0, 0,
        false, payload);

    // event id
    record.setEventId(UUID.randomUUID().toString());

    // event timestamp
    record.setEventTs(eventTs);

    // device info
    DeviceInfoParser deviceInfoParser = new DeviceInfoParser().parse(agentInfo);
    record.setDeviceFamily(deviceInfoParser.getDeviceFamily());
    record.setDeviceType(deviceInfoParser.getDeviceType());
    record.setBrowserFamily(deviceInfoParser.getBrowserFamily());
    record.setBrowserVersion(deviceInfoParser.getBrowserVersion());
    record.setOsFamily(deviceInfoParser.getOsFamily());
    record.setOsVersion(deviceInfoParser.getOsVersion());

    // app info
    String appId = CollectionServiceUtil.getAppIdFromUserAgent(agentInfo);
    if (!StringUtils.isEmpty(appId)) {
      record.setAppId(appId);
    }
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
      return HttpRequestUtil.parseTagFromParams(parameters, Constants.EMAIL_UNIQUE_ID);
    }

    return "";
  }

  private static long getProducerEventTs(ChannelAction channelAction, ROIEvent roiEvent, long startTime) {
    if (ChannelAction.ROI.equals(channelAction) && isLongNumeric(roiEvent.getTransactionTimestamp())) {
      return Long.parseLong(roiEvent.getTransactionTimestamp());
    } else {
      return startTime;
    }
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
        // TODO: Do we really need to get publisher id here?
      }
    } else if (ChannelType.PAID_SEARCH.equals(channelType)) {
      // partner definition unknown
    } else if (ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.MRKT_EMAIL.equals(channelType)) {
      partner = EmailPartnerIdEnum.parse(parameters.getFirst(Constants.MKPID));
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
      campaignId = CollectionServiceUtil.substring(parameters.getFirst(Constants.SOURCE_ID), "e", ".mle");
    } else if (ChannelType.MRKT_EMAIL.equals(channelType)) {
      if (StringUtils.isNotEmpty(HttpRequestUtil.parseFromTwoParams(parameters, Constants.SEGMENT_NAME,
          Constants.SEGMENT_NAME_S))) {
        campaignId = Objects.requireNonNull(HttpRequestUtil.parseFromTwoParams(parameters, Constants.SEGMENT_NAME,
            Constants.SEGMENT_NAME_S)).trim();
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
        while (rawRotationId.contains("%") && decodeCnt < 5) {
          rawRotationId = URLDecoder.decode(rawRotationId, "UTF-8");
          decodeCnt = decodeCnt + 1;
        }
        rotationId = rawRotationId.replaceAll("-", "");
      } catch (Exception e) {
        logger.warn(Errors.ERROR_INVALID_MKRID);
      }
    }

    return rotationId;
  }

  /**
   * Get geo id
   */
  private static int getGeoID(ContainerRequestContext requestContext) {
    int geoId;

    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);
    geoId = userPrefsCtx.getGeoContext().getCountryId();

    return geoId;
  }

  /**
   * Get user id
   */
  private static long getUserId(IEndUserContext endUserContext,
                                String bu, ChannelType channelType) {
    if (ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.MRKT_EMAIL.equals(channelType)) {
      if (!StringUtils.isEmpty(bu)) {
        return EncryptUtil.decryptUserId(Long.parseLong(bu));
      }
    } else {
        return endUserContext.getOrigUserOracleId();
    }

    return 0;
  }

  /**
   * Get payload
   */
  private static Map<String, String> getPayload(Map<String, String> payload, BaseEvent baseEvent,
                                                MultiValueMap<String, String> parameters,
                                                String appId, ChannelType channelType, ChannelAction channelAction,
                                                long snapshotId, long shortSnapshotId, ROIEvent roiEvent, long userId,
                                                String trackingHeader) {
    // add tags from parameters
    for (Map.Entry<String, String> entry : Constants.emailTagParamMap.entries()) {
      if (parameters.containsKey(entry.getValue()) && parameters.getFirst(entry.getValue()) != null) {
        payload.put(entry.getKey(), HttpRequestUtil.parseTagFromParams(parameters, entry.getValue()));
      }
    }
    if (channelAction != ChannelAction.ROI) {
      // add tags in url param "sojTags" into applicationPayload
      addSojTags(payload, parameters, channelType, channelAction);
      addTags(payload, parameters, snapshotId, shortSnapshotId);
    }

    if (channelAction == ChannelAction.ROI) {
      addRoiSojTags(payload, roiEvent, String.valueOf(userId), snapshotId, shortSnapshotId);
    }

    if (channelType == ChannelType.EPN) {
      String toolId = HttpRequestUtil.parseTagFromParams(parameters, Constants.TOOL_ID);
      if (StringUtils.isNotEmpty(toolId)) {
        payload.put(Constants.TOOL_ID, HttpRequestUtil.parseTagFromParams(parameters, Constants.TOOL_ID));
      }
    }

    // add other tags
    // buyer access site id
    if (ChannelAction.EMAIL_OPEN.equals(channelAction)) {
      payload.put("bs", HttpRequestUtil.parseTagFromParams(parameters, Constants.CHOCO_BUYER_ACCESS_SITE_ID));
    } else {
      UserPrefsCtx userPrefsCtx = baseEvent.getUserPrefsCtx();
      payload.put("bs", String.valueOf(userPrefsCtx.getGeoContext().getSiteId()));
    }

    // cobrand
    payload.put("cobrand", cobrandParser.parse(appId, baseEvent.getUserAgentInfo().getUserAgentRawData()));

    // facebook prefetch
    if (CollectionServiceUtil.isFacebookPrefetchEnabled(baseEvent.getRequestHeaders())) {
      payload.put("fbprefetch", "true");
    }

    // landing page and tracking url
    if (ChannelAction.CLICK.equals(channelAction)) {
      payload.put("url_mpre", baseEvent.getUrl());
    }

    // adguid
    String adguid = HttpRequestUtil.getHeaderValue(trackingHeader, Constants.ADGUID);
    if (!StringUtils.isEmpty(adguid)) {
      payload.put(Constants.ADGUID, adguid);
    }

    // is from ufes
    String isUfes = baseEvent.getRequestHeaders().get(Constants.IS_FROM_UFES_HEADER);
    if (StringUtils.isEmpty(isUfes)) {
      payload.put(Constants.TAG_IS_UFES, "false");
    } else {
      payload.put(Constants.TAG_IS_UFES, "true");
    }

    // status code
    String statusCode = baseEvent.getRequestHeaders().get(Constants.NODE_REDIRECTION_HEADER_NAME);
    if (!StringUtils.isEmpty(statusCode)) {
      payload.put(Constants.TAG_STATUS_CODE, statusCode);
    }
    //parse itm from url and put itm to payload
      parseItmTag(payload, url);

      return encodeTags(payload);
  }

  private static void parseItmTag(Map<String, String> payload, String url) {
    if (!payload.containsKey("itm")) {
      try {
        URI uri = new URI(url);
        String path = uri.getPath();
        if (StringUtils.isNotEmpty(path) && (path.startsWith("/itm/") || path.startsWith("/i/"))) {
          String itemId = path.substring(path.lastIndexOf("/") + 1);
          if (StringUtils.isNumeric(itemId)) {
            payload.put("itm", itemId);
          }
        }
      } catch (Exception e) {
        metrics.meter("putItmToPldError");
        logger.warn("put itm tag to payload error, url is {}", url);
      }
    }
  }

  private static void addTags(Map<String, String> payload, MultiValueMap<String, String> parameters,
                              long snapshotId, long shortSnapshotId) {
    String searchKeyword = "";
    if (parameters.containsKey(Constants.SEARCH_KEYWORD) && parameters.get(Constants.SEARCH_KEYWORD).get(0) != null) {
      searchKeyword = parameters.get(Constants.SEARCH_KEYWORD).get(0);
    }
    payload.put("keyword", searchKeyword);
    payload.put("rvrid", String.valueOf(shortSnapshotId));
    payload.put("snapshotid", String.valueOf(snapshotId));

    String gclid = "";
    if (parameters.containsKey(Constants.GCLID) && parameters.get(Constants.GCLID).get(0) != null) {
      gclid = parameters.get(Constants.GCLID).get(0);
    }
    payload.put("gclid", gclid);
  }

  /**
   * Add tags in param sojTags
   */
  private static Map<String, String> addSojTags(Map<String, String> applicationPayload, MultiValueMap<String, String> parameters,
                                                ChannelType channelType, ChannelAction channelAction) {
    if (parameters.containsKey(Constants.SOJ_TAGS) && parameters.get(Constants.SOJ_TAGS).get(0) != null) {
      String sojTags = parameters.get(Constants.SOJ_TAGS).get(0);
      try {
        sojTags = URLDecoder.decode(sojTags, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        logger.warn("Param sojTags is wrongly encoded", e);
        metrics.meter("ErrorEncodedSojTags", 1, Field.of(Constants.CHANNEL_ACTION, channelAction.toString()),
            Field.of(Constants.CHANNEL_TYPE, channelType.toString()));
      }
      if (!StringUtils.isEmpty(sojTags)) {
        StringTokenizer stToken = new StringTokenizer(sojTags, PresentationConstants.COMMA);
        while (stToken.hasMoreTokens()) {
          StringTokenizer sojNvp = new StringTokenizer(stToken.nextToken(), PresentationConstants.EQUALS);
          if (sojNvp.countTokens() == 2) {
            String sojTag = sojNvp.nextToken().trim();
            String urlParam = sojNvp.nextToken().trim();
            if (!StringUtils.isEmpty(urlParam) && !StringUtils.isEmpty(sojTag)) {
              applicationPayload.put(sojTag, HttpRequestUtil.parseTagFromParams(parameters, urlParam));
            }
          }
        }
      }
    }

    return applicationPayload;
  }

  private static void addRoiSojTags(Map<String, String> payloadMap, ROIEvent roiEvent, String userId, long snapshotId, long shortSnapshotId) {
    payloadMap.put("rvrid", String.valueOf(shortSnapshotId));
    payloadMap.put("snapshotid", String.valueOf(snapshotId));

    if (isLongNumeric(roiEvent.getItemId())) {
      payloadMap.put("itm", roiEvent.getItemId());
    }
    if (!StringUtils.isEmpty(roiEvent.getTransType())) {
      payloadMap.put("tt", roiEvent.getTransType());
    }
    if (isLongNumeric(roiEvent.getUniqueTransactionId())) {
      payloadMap.put("roi_bti", roiEvent.getUniqueTransactionId());
    }
    if (isLongNumeric(userId)) {
      payloadMap.put("userid", userId);
    }
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
   * in case null value cause incompatibility
   */
  private static Map<String, String> deleteNullOrEmptyValue(Map<String, String> map) {
    Set<Map.Entry<String, String>> entrySet = map.entrySet();
    Iterator<Map.Entry<String, String>> iterator = entrySet.iterator();

    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      if (StringUtils.isEmpty(entry.getValue())) {
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

  /**
   * Coalesce to set default value in case of null value
   */
  private static <T> T coalesce(T a, T b) {
    return a == null ? b : a;
  }

  /**
   * Get environment for event emitter
   */
  private static EnvironmentEnum getEnv() throws Exception {
    String env = ApplicationOptions.getInstance().getEnvironment();
    logger.info("Platform Environment: {}", env);

    EnvironmentEnum environment;
    switch (env) {
      case "dev":
        environment = EnvironmentEnum.DEV;
        break;
      case "qa":
        environment = EnvironmentEnum.STAGING;
        break;
      case "pre-production":
        environment = EnvironmentEnum.PRE_PROD;
        break;
      case "production":
        environment = EnvironmentEnum.PROD;
        break;
      default:
        throw new Exception("No matched environment");
    }

    return environment;
  }
}
