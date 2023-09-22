package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.constant.ClientDataEnum;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.constant.StringConstants;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.app.raptor.chocolate.gen.model.UnifiedTrackingEvent;
import com.ebay.app.raptor.chocolate.util.EncryptUtil;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.app.raptor.chocolate.utp.UepPayloadHelper;
import com.ebay.kernel.presentation.constants.PresentationConstants;
import com.ebay.kernel.util.FastURLEncoder;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.DDSResponse;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.platform.raptor.raptordds.parsers.UserAgentParser;
import com.ebay.raptor.domain.request.api.DomainRequestData;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import com.ebay.raptor.user.experience.classfication.UserExperienceClass;
import com.ebay.raptorio.request.tracing.RequestTracingContext;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum;
import com.ebay.traffic.chocolate.utp.common.EmailPartnerIdEnum;
import com.ebay.traffic.chocolate.utp.common.ServiceEnum;
import com.ebay.traffic.chocolate.utp.common.model.UnifiedTrackingMessage;
import com.ebay.traffic.chocolate.utp.lib.UnifiedTrackerFactory;
import com.ebay.traffic.chocolate.utp.lib.cache.TrackingGovernanceTagCache;
import com.ebay.traffic.chocolate.utp.lib.constants.EnvironmentEnum;
import com.ebay.traffic.monitoring.Field;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;

import javax.ws.rs.container.ContainerRequestContext;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;

import static com.ebay.app.raptor.chocolate.constant.Constants.*;
import static com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil.isLongNumeric;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.deeplinksites;

/**
 * Created by jialili1 on 11/5/20
 */
public class UnifiedTrackingMessageParser {
  private static final Logger logger = LoggerFactory.getLogger(UnifiedTrackingMessageParser.class);
  private static CobrandParser cobrandParser = new CobrandParser();

  private static final List<String> BOT_LIST = Arrays.asList("bot", "proxy", "Mediapartners-Google",
      "facebookexternalhit", "aiohttp", "python-requests", "axios", "Go-http-client", "spider", "curl", "Tumblr");
  private static final List<String> OPEN_BOT_LIST = Arrays.asList("bot", "Mediapartners-Google",
      "facebookexternalhit", "aiohttp", "python-requests", "axios", "Go-http-client", "spider", "curl", "Tumblr");

  private static final String ITEM = "itm";
  private static final String ITEM_VIEW = "item.view";
  private static final String ITEM_PRODUCT = "item.product";

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
    record.setEventId(UUID.randomUUID().toString());
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
    // Notification Send will send appVersion and deviceType
    if (StringUtils.isNotEmpty(event.getAppVersion())) {
      record.setAppVersion(event.getAppVersion());
    }
    if (StringUtils.isNotEmpty(event.getDeviceType())) {
      record.setDeviceType(event.getDeviceType());
    }

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
   * Bot detection by user agent
   *
   * @param userAgent user agent
   */
  public static boolean isBot(String userAgent, String actionType) {
    if (StringUtils.isNotEmpty(userAgent)) {
      String userAgentLower = userAgent.toLowerCase();
      if (ActionTypeEnum.OPEN.getValue().equals(actionType)) {
        for (String botKeyword : OPEN_BOT_LIST) {
          if (userAgentLower.contains(botKeyword.toLowerCase())) {
            return true;
          }
        }
      } else {
        for (String botKeyword : BOT_LIST) {
          if (userAgentLower.contains(botKeyword.toLowerCase())) {
            return true;
          }
        }
      }
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
    ChannelType channelType = baseEvent.getChannelType().getLogicalChannel().getAvro();
    ChannelAction channelAction = baseEvent.getActionType().getAvro();

    // event id
    record.setEventId(baseEvent.getUuid());
    record.setProducerEventId(getProducerEventId(baseEvent.getUrlParameters(), channelType));

    // event timestamp
    record.setProducerEventTs(getProducerEventTs(channelAction, baseEvent.getRoiEvent(),
        baseEvent.getTimestamp()));

    // rlog id
    record.setRlogId(tracingContext.getRlogId());

    MultiValueMap<String, String> parameters = baseEvent.getUrlParameters();

    // tracking id
    record.setTrackingId(HttpRequestUtil.parseTagFromTwoParams(parameters, UepPayloadHelper.TRACKING_ID,
        UepPayloadHelper.TRACKING_ID.toLowerCase()));

    // user id
    String bu = baseEvent.getUrlParameters().getFirst(Constants.BEST_GUESS_USER);
    if (!StringUtils.isEmpty(bu)) {
      record.setEncryptedUserId(Long.parseLong(bu));
    }

    long userId = getUserId(baseEvent.getEndUserContext(), bu, channelType);
    record.setUserId(userId);

    // guid
    String trackingHeader = baseEvent.getRequestHeaders().get("X-EBAY-C-TRACKING");
    String guid = HttpRequestUtil.getHeaderValue(trackingHeader, Constants.GUID);
    if (!StringUtils.isEmpty(guid)) {
      record.setGuid(guid);
    }
    // Email open and 3rd party clicks need eventId to be the fake guid
    if (needFakeGuid(channelType, channelAction, baseEvent.isThirdParty())) {
      record.setGuid(baseEvent.getUuid().replace(Constants.HYPHEN, ""));
    }

    // device info
//    record.setIdfa(event.getIdfa());
//    record.setGadid(event.getGadid());
    record.setDeviceId(baseEvent.getEndUserContext().getDeviceId());
    String userAgent = baseEvent.getEndUserContext().getUserAgent();
    record.setUserAgent(userAgent);

    // channel type
    ChannelTypeEnum channelTypeEnum = getChannelType(channelType);
    record.setChannelType(channelTypeEnum.getValue());

    // action type
    String actionType = getActionType(channelAction);
    record.setActionType(actionType);

    // partner id
    record.setPartner(getPartner(parameters, channelType));

    // campaign id
    record.setCampaignId(getCampaignId(parameters, channelType));

    // rotation id
    String rotationId = getRotationId(parameters);
    record.setRotationId(rotationId);

    // site id
    String siteId = parameters.getFirst(Constants.SITEID);
    if ((ActionTypeEnum.OPEN.getValue().equals(actionType) || ActionTypeEnum.ROI.getValue().equals(actionType)) && StringUtils.isNotEmpty(siteId)) {
      record.setSiteId(Integer.valueOf(siteId));
    } else {
      record.setSiteId(domainRequest.getSiteId());
    }

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
    record.setRemoteIp(baseEvent.getRemoteIp());

    // page id
    int pageId = PageIdEnum.getPageIdByAction(channelAction);
    record.setPageId(pageId);

    // user geo id
    record.setGeoId(getGeoID(requestContext));

    // isBot. Basic bot detection by user agent.
    record.setIsBot(isBot(userAgent, actionType));

    // payload
    String appId = CollectionServiceUtil.getAppIdFromUserAgent(baseEvent.getUserAgentInfo());

    Map<String, String> fullPayload = getPayload(payload, baseEvent, parameters, appId, channelType, channelAction,
            snapshotId, shortSnapshotId, baseEvent.getRoiEvent(), userId, trackingHeader);

    // append UEP payload
    if ((ChannelType.MRKT_EMAIL.equals(channelType) || ChannelType.MRKT_MESSAGE_CENTER.equals(channelType)
        || ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.SITE_MESSAGE_CENTER.equals(channelType))) {
      Map<String, String> uepPayload =
          UepPayloadHelper.getInstance().getUepPayload(baseEvent.getUrl(), null,
              ActionTypeEnum.valueOf(actionType), channelTypeEnum);
      if (uepPayload != null && uepPayload.size() > 0) {
        fullPayload.putAll(uepPayload);
      }
    }

    record.setPayload(deleteNullOrEmptyValue(fullPayload));

    // data governance
    long startTs = System.currentTimeMillis();
    TrackingGovernanceTagCache.getInstance().govern(record);
    MonitorUtil.latency("DataGovernanceLatency", System.currentTimeMillis() - startTs);

    return record;
  }

  private static ChannelTypeEnum getChannelType(ChannelType channelType) {
    switch (channelType) {
      case PAID_SEARCH:
        return ChannelTypeEnum.PAID_SEARCH;
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
      case MRKT_MESSAGE_CENTER:
        return ChannelTypeEnum.MRKT_MESSAGE_CENTER;
      case SITE_MESSAGE_CENTER:
        return ChannelTypeEnum.SITE_MESSAGE_CENTER;
      case GCX_EMAIL:
        return ChannelTypeEnum.GCX_EMAIL;
      case GCX_MESSAGE_CENTER:
        return ChannelTypeEnum.GCX_MESSAGE_CENTER;
      case MRKT_SMS:
        return ChannelTypeEnum.MRKT_SMS;
      case SITE_SMS:
        return ChannelTypeEnum.SITE_SMS;
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
    if (ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.SITE_MESSAGE_CENTER.equals(channelType)) {
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
    } else if (ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.MRKT_EMAIL.equals(channelType) ||
            ChannelType.SITE_MESSAGE_CENTER.equals(channelType) || ChannelType.MRKT_MESSAGE_CENTER.equals(channelType)) {
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
    } else if (ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.SITE_MESSAGE_CENTER.equals(channelType)) {
      campaignId = CollectionServiceUtil.substring(parameters.getFirst(Constants.SOURCE_ID), "e", ".mle");
    } else if (ChannelType.MRKT_EMAIL.equals(channelType) || ChannelType.MRKT_MESSAGE_CENTER.equals(channelType)) {
      if (StringUtils.isNotEmpty(HttpRequestUtil.parseTagFromParams(parameters, Constants.SEGMENT_NAME))) {
        campaignId = Objects.requireNonNull(HttpRequestUtil.parseTagFromParams(parameters, Constants.SEGMENT_NAME)).trim();
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
    if (ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.MRKT_EMAIL.equals(channelType) ||
            ChannelType.SITE_MESSAGE_CENTER.equals(channelType) || ChannelType.MRKT_MESSAGE_CENTER.equals(channelType) ||
        ChannelType.GCX_EMAIL.equals(channelType) || ChannelType.GCX_MESSAGE_CENTER.equals(channelType)) {
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
    if (channelAction != ChannelAction.ROI) {
      // add tags in url param "sojTags" into applicationPayload
      if (ChannelType.MRKT_EMAIL.equals(channelType) || ChannelType.SITE_EMAIL.equals(channelType)
          || ChannelType.MRKT_MESSAGE_CENTER.equals(channelType) || ChannelType.SITE_MESSAGE_CENTER.equals(channelType)
          || ChannelType.GCX_EMAIL.equals(channelType) || ChannelType.GCX_MESSAGE_CENTER.equals(channelType) ) {
        parseTagFromParamByChannel(payload, baseEvent, parameters, channelType);
      } else {
        addSojTags(payload, parameters, channelType, channelAction);
      }
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

    // ep treatmentId
    String xt = HttpRequestUtil.parseTagFromParams(parameters, Constants.XT);
    if (!StringUtils.isEmpty(xt)) {
      payload.put(Constants.XT, xt);
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

    // parse itm from url and put itm to payload
    parseItmTag(payload, baseEvent.getUrl(), parameters);

    // session sequence number
    payload.put(Constants.SEQ_NUM, "1");

    // add isUFESRedirect if the click traffic is converted from Rover to Chocolate by UFES
    if (parameters.containsKey(Constants.UFES_REDIRECT)
            && Boolean.TRUE.toString().equalsIgnoreCase(parameters.getFirst(Constants.UFES_REDIRECT))) {
      payload.put(Constants.TAG_IS_UFES_REDIRECT, "true");
    }

    // 3rd party tag
    if (ChannelAction.CLICK.equals(channelAction)
        && baseEvent.isThirdParty()) {
      payload.put(Constants.TAG_IS_THIRD_PARTY, "true");
    }

    // clientdata
    payload.put("clientData", constructClientData(baseEvent));

    // guid list
    if (needGuidList(channelType, channelAction, baseEvent.isThirdParty())) {
      String guidList = HttpRequestUtil.getHeaderValue(trackingHeader, Constants.GUID_LIST);
      if (!StringUtils.isEmpty(guidList)) {
        payload.put(Constants.GUID_LIST, guidList);
      }
    }

    // sessionId, sessionSkey
    if (isEmailOpenOrThirdPartyClick(channelType, channelAction, baseEvent.isThirdParty())) {
      payload.put(Constants.SESSION_ID, baseEvent.getUuid().replace(Constants.HYPHEN, ""));
      long sessionSkey = ((baseEvent.getTimestamp() * Constants.MILLI2MICRO) + Constants.OFFSET) /
          Constants.SESSION_KEY_DIVISION;
      payload.put(Constants.SESSION_SKEY, String.valueOf(sessionSkey));
    }

    // social tags
    if (ChannelAction.CLICK.equals(channelAction) && ChannelType.SOCIAL_MEDIA.equals(channelType)) {
      socialMediaParamTags.forEach((key, val) -> payload.put(key, HttpRequestUtil.parseTagFromParams(parameters, val)));
    }

    // email best guess user id, decrypted from bu parameter
    String bu = baseEvent.getUrlParameters().getFirst(Constants.BEST_GUESS_USER);
    if (StringUtils.isNotEmpty(bu)) {
      long emailUserId = EncryptUtil.decryptUserId(Long.parseLong(bu));
      payload.put(Constants.EMAIL_USER_ID, String.valueOf(emailUserId));
    } else {
      payload.put(Constants.EMAIL_USER_ID, "0");
    }
  
    // add device info for parsing Epsrv api channelId
    // refrence: https://github.corp.ebay.com/experimentation-platform/raptor-experimentation-component/blob/0.18.0-RELEASE-TAG/raptor-experimentation-autoconfigure/src/main/java/com/ebay/experimentation/util/DeviceUtil.java#L34
    UserAgentInfo userAgentInfo = baseEvent.getUserAgentInfo();
    if (userAgentInfo != null) {
      payload.put("isNativeApp", String.valueOf(userAgentInfo.isNativeApp()));
      payload.put("isWeb", String.valueOf(userAgentInfo.requestIsWeb()));
      payload.put("isMobileWeb", String.valueOf(userAgentInfo.requestIsMobileWeb()));
      DDSResponse deviceInfo = userAgentInfo.getDeviceInfo();
      if (deviceInfo != null) {
        payload.put("osiOS", String.valueOf(deviceInfo.osiOS()));
        payload.put("osAndroid", String.valueOf(deviceInfo.osAndroid()));
      }
      UserExperienceClass userExpClass = userAgentInfo.getUserExperienceClass();
      if (userExpClass != null) {
        payload.put("userExpClass", userExpClass.name());
      }
    }

    return encodeTags(payload);
  }

  private static void parseItmTag(Map<String, String> payload, String url, MultiValueMap<String, String> parameters) {
    if (!payload.containsKey(ITEM)) {
      try {
        Matcher deeplinkMatcher = deeplinksites.matcher(url.toLowerCase());
        if (deeplinkMatcher.find()) {
          if (url.toLowerCase().contains(ITEM_VIEW)) {
            payload.put(ITEM, HttpRequestUtil.parseTagFromParams(parameters, ID));
          } else if (url.toLowerCase().contains(ITEM_PRODUCT)) {
            payload.put(ITEM, HttpRequestUtil.parseTagFromParams(parameters, IID));
          }
        } else {
          URI uri = new URI(url);
          String path = uri.getPath();
          if (StringUtils.isNotEmpty(path) && (path.startsWith("/itm/") || path.startsWith("/i/"))) {
            String itemId = path.substring(path.lastIndexOf("/") + 1);
            if (StringUtils.isNumeric(itemId)) {
              payload.put(ITEM, itemId);
            }
          }
        }
      } catch (Exception e) {
        MonitorUtil.info("putItmToPldError");
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
    payload.put(TAG_CHANNEL, HttpRequestUtil.parseTagFromParams(parameters, MKCID));
  }

  /**
   * Add tags in param sojTags
   */
  @Deprecated
  private static Map<String, String> addSojTags(Map<String, String> applicationPayload, MultiValueMap<String, String> parameters,
                                                ChannelType channelType, ChannelAction channelAction) {
    if (parameters.containsKey(Constants.SOJ_TAGS) && parameters.get(Constants.SOJ_TAGS).get(0) != null) {
      String sojTags = parameters.get(Constants.SOJ_TAGS).get(0);
      try {
        sojTags = URLDecoder.decode(sojTags, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        logger.warn("Param sojTags is wrongly encoded", e);
        MonitorUtil.info("ErrorEncodedSojTags", 1, Field.of(Constants.CHANNEL_ACTION, channelAction.toString()),
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

  private static void parseTagFromParamByChannel(Map<String, String> payload, BaseEvent baseEvent,
                                                 MultiValueMap<String, String> parameters, ChannelType channelType) {
    //for debugging
    payload.put("nonSojTag", "1");
    // add tags from parameters
    for (Map.Entry<String, String> entry : Constants.channelParamTagMap
            .getOrDefault(channelType, ImmutableMultimap.<String, String>builder().build()).entries()) {
      String value = HttpRequestUtil.parseTagFromTwoParams(parameters, entry.getValue(), entry.getValue().toLowerCase());
      if (StringUtils.isNotEmpty(value)) {
        payload.put(entry.getKey(), value);
      }
    }

    if (ChannelType.MRKT_EMAIL.equals(channelType)) {
      try {
        if (parameters.containsKey(REDIRECT_URL_SOJ_TAG) && parameters.get(REDIRECT_URL_SOJ_TAG).get(0) != null) {
          payload.put("adcamp_landingpage", URLDecoder.decode(parameters.get(REDIRECT_URL_SOJ_TAG).get(0), "UTF-8"));
        }
      } catch (UnsupportedEncodingException e) {
        logger.warn("adcamp_landingpage is wrongly encoded", e);
        MonitorUtil.info("ErrorEncoded_adcamp_landingpage", 1, Field.of(CHANNEL_ACTION, baseEvent.getActionType()),
                Field.of(CHANNEL_TYPE, baseEvent.getChannelType()));
      }
    }
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
    // enrich roi payload with sale type
    Map<String, String> roiPayload = roiEvent.getPayload();
    if (roiPayload.containsKey("saleTypeId") && StringUtils.isNotEmpty(roiPayload.get("saleTypeId"))) {
      payloadMap.put("saleTypeId", roiPayload.get("saleTypeId"));
    }
    if (roiPayload.containsKey("saleTypeFlow") && StringUtils.isNotEmpty(roiPayload.get("saleTypeFlow"))) {
      payloadMap.put("saleTypeFlow", roiPayload.get("saleTypeFlow"));
    }
    if (roiPayload.containsKey("order_type") && StringUtils.isNotEmpty(roiPayload.get("order_type"))) {
      payloadMap.put("order_type", roiPayload.get("order_type"));
    }
    if (roiPayload.containsKey("is_committed") && StringUtils.isNotEmpty(roiPayload.get("is_committed"))) {
      payloadMap.put("is_committed", roiPayload.get("is_committed"));
    }
    if (roiPayload.containsKey("ff2") && StringUtils.isNotEmpty(roiPayload.get("ff2"))) {
      payloadMap.put("ff2", roiPayload.get("ff2"));
    }
    if (roiPayload.containsKey("roisrc") && StringUtils.isNotEmpty(roiPayload.get("roisrc"))) {
      payloadMap.put("roisrc", roiPayload.get("roisrc"));
    }
    if (roiPayload.containsKey("orderId") && StringUtils.isNotEmpty(roiPayload.get("orderId"))) {
      payloadMap.put("orderId", roiPayload.get("orderId"));
    }
    if (roiPayload.containsKey("purchaseOrderId") && StringUtils.isNotEmpty(roiPayload.get("purchaseOrderId"))) {
      payloadMap.put("purchaseOrderId", roiPayload.get("purchaseOrderId"));
    }

    if (roiPayload.containsKey("quantity") && StringUtils.isNotEmpty(roiPayload.get("quantity"))) {
      payloadMap.put("quantity", roiPayload.get("quantity"));
    }
    if (roiPayload.containsKey("orderTotal") && StringUtils.isNotEmpty(roiPayload.get("orderTotal"))) {
      payloadMap.put("orderTotal", roiPayload.get("orderTotal"));
    }
    if (roiPayload.containsKey("currency") && StringUtils.isNotEmpty(roiPayload.get("currency"))) {
      payloadMap.put("currency", roiPayload.get("currency"));
    }
    if (roiPayload.containsKey("primaryCategoryId") && StringUtils.isNotEmpty(roiPayload.get("primaryCategoryId"))) {
      payloadMap.put("primaryCategoryId", roiPayload.get("primaryCategoryId"));
    }
    if (roiPayload.containsKey("secondaryCategoryId") && StringUtils.isNotEmpty(roiPayload.get("secondaryCategoryId"))) {
      payloadMap.put("secondaryCategoryId", roiPayload.get("secondaryCategoryId"));
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

  /**
   * Only Email Open and Email 3rd party Clicks need fake guid
   */
  private static boolean needFakeGuid(ChannelType channelType, ChannelAction channelAction, boolean isThirdParty) {
    return isEmailOpenOrThirdPartyClick(channelType, channelAction, isThirdParty);
  }

  /**
   * Email Open, Email 3rd party Click, EPN Impression, Display AR need guidList tag
   */
  private static boolean needGuidList(ChannelType channelType, ChannelAction channelAction, boolean isThirdParty) {
    boolean isEPNImpression = ChannelAction.IMPRESSION.equals(channelAction) && ChannelType.EPN.equals(channelType);

    boolean isDisplayServe = ChannelAction.SERVE.equals(channelAction) && ChannelType.DISPLAY.equals(channelType);

    return isEmailOpenOrThirdPartyClick(channelType, channelAction, isThirdParty) || isEPNImpression
        || isDisplayServe;
  }

  /**
   * Identify Email Open and Email 3rd party Clicks
   */
  private static boolean isEmailOpenOrThirdPartyClick(ChannelType channelType, ChannelAction channelAction,
                                                      boolean isThirdParty) {
    boolean isEmailOpen = ChannelAction.EMAIL_OPEN.equals(channelAction) && (
        ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.SITE_MESSAGE_CENTER.equals(channelType) ||
            ChannelType.MRKT_EMAIL.equals(channelType) || ChannelType.MRKT_MESSAGE_CENTER.equals(channelType) ||
            ChannelType.GCX_EMAIL.equals(channelType) ||  ChannelType.GCX_MESSAGE_CENTER.equals(channelType));

    boolean isThirdPartyClick = isThirdParty && ChannelAction.CLICK.equals(channelAction) && (
        ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.SITE_MESSAGE_CENTER.equals(channelType) ||
            ChannelType.MRKT_EMAIL.equals(channelType) || ChannelType.MRKT_MESSAGE_CENTER.equals(channelType));

    return isEmailOpen || isThirdPartyClick;
  }

  /**
   * To construct Client Data according to baseEvent
   * @param baseEvent
   * @return
   */
  private static String constructClientData(BaseEvent baseEvent) {
    if (baseEvent == null || baseEvent.getEndUserContext() == null) {
      return null;
    }

    StringJoiner clientData = new StringJoiner(StringConstants.AND);
    StringJoiner clientDataHeader = new StringJoiner(StringConstants.EQUAL);
    if(StringUtils.isNotEmpty(baseEvent.getEndUserContext().getUserAgent())){
      clientDataHeader.add(ClientDataEnum.USER_AGENT.getHeaderAliasName()).add(baseEvent.getEndUserContext().getUserAgent());
      clientData.add(clientDataHeader.toString());
    }

    Map<String, String> clientHints = baseEvent.getEndUserContext().getClientHints();
    if (MapUtils.isNotEmpty(clientHints)) {
      for (ClientDataEnum dataEnum : ClientDataEnum.getClientHint()) {
        clientDataHeader = new StringJoiner(StringConstants.EQUAL);
        String headerVal = clientHints.get(dataEnum.getHeaderName());
        ;
        if (StringUtils.isNotEmpty(headerVal)) {
          clientDataHeader.add(dataEnum.getHeaderAliasName()).add(headerVal);
          clientData.add(clientDataHeader.toString());
        }
      }
    }

    return clientData.toString();
  }
}
