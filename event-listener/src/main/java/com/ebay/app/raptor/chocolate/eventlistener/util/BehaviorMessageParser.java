package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.util.EncryptUtil;
import com.ebay.kernel.presentation.constants.PresentationConstants;
import com.ebay.kernel.util.FastURLEncoder;
import com.ebay.kernel.util.HeaderMultiValue;
import com.ebay.kernel.util.RequestUtil;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.DDSResponse;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.domain.request.api.DomainRequestData;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import com.ebay.raptorio.request.tracing.RequestTracingContext;
import com.ebay.tracking.common.util.UrlProcessHelper;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by jialili1 on 7/29/20
 *
 * Parse the request for behavior message
 */
public class BehaviorMessageParser {
  private static final Logger logger = LoggerFactory.getLogger(BehaviorMessageParser.class);
  private Metrics metrics = ESMetrics.getInstance();
  private CobrandParser cobrandParser = new CobrandParser();

  private static BehaviorMessageParser INSTANCE;

  private static final String AGENT_TAG = "Agent";

  /**
   * Singleton class
   */
  private BehaviorMessageParser() { }

  /**
   * Initialize singleton instance
   */
  public static synchronized void init() {
    if (INSTANCE == null) {
      INSTANCE = new BehaviorMessageParser();
    }
  }

  /**
   * Return singleton instance
   */
  public static BehaviorMessageParser getInstance() {
    return INSTANCE;
  }

  public BehaviorMessage parseAmsAndImkEvent(final HttpServletRequest request, ContainerRequestContext requestContext,
                                             IEndUserContext endUserContext, MultiValueMap<String, String> parameters,
                                             UserAgentInfo agentInfo, String uri, Long startTime, final ChannelType channelType,
                                             final ChannelAction channelAction, Long snapshotId, int pageId, String pageName, int rdt,
                                             String referer, String guid, String cguid, String userId, String rotationId) {
    try {
      Map<String, String> applicationPayload = new HashMap<>();
      Map<String, String> clientData = new HashMap<>();
      List<Map<String, String>> data = new ArrayList<>();

      // set default value
      BehaviorMessage record = new BehaviorMessage("", "", 0L, null, 0, null, null, null, null, null, null, null,
              null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
              null, applicationPayload, null, clientData, "", "", "", data);

      RequestTracingContext tracingContext = (RequestTracingContext) requestContext.getProperty(RequestTracingContext.NAME);
      DomainRequestData domainRequest = (DomainRequestData) requestContext.getProperty(DomainRequestData.NAME);

      // guid
      record.setGuid(guid);

      // adguid
      String trackingHeader = request.getHeader("X-EBAY-C-TRACKING");
      String adguid = getData(Constants.ADGUID, trackingHeader);
      if (adguid != null) {
        record.setAdguid(adguid);
      }

      // source id
      record.setSid(parseTagFromParams(parameters, Constants.SOURCE_ID));

      record.setUserId(userId);

      // eventTimestamp
      record.setEventTimestamp(startTime);

      // page info
      record.setPageId(pageId);
      record.setPageName(pageName);

      // event family and action
      record.setEventFamily(Constants.EVENT_FAMILY_CRM);
      record.setEventAction(Constants.EVENT_ACTION);

      // snapshotId
      record.setSnapshotId(String.valueOf(snapshotId));

      // fake session info
      record.setSessionId(String.valueOf(snapshotId));
      record.setSeqNum("1");

      // agent info
      record.setAgentInfo(endUserContext.getUserAgent());

      // app info
      String appId = CollectionServiceUtil.getAppIdFromUserAgent(agentInfo);
      record.setAppId(appId);
      if (agentInfo.getAppInfo() != null) {
        record.setAppVersion(agentInfo.getAppInfo().getAppVersion());
      }

      // url query string
      record.setUrlQueryString(UrlProcessHelper.getMaskedUrl(removeBsParam(parameters, uri), domainRequest.isSecure(),
              false));

      // device info
      DDSResponse deviceInfo = agentInfo.getDeviceInfo();
      record.setDeviceFamily(getDeviceFamily(deviceInfo));
      record.setDeviceType(deviceInfo.getOsName());
      record.setBrowserVersion(deviceInfo.getBrowserVersion());
      record.setBrowserFamily(deviceInfo.getBrowser());
      record.setOsVersion(deviceInfo.getDeviceOSVersion());
      record.setOsFamily(deviceInfo.getDeviceOS());
      record.setEnrichedOsVersion(deviceInfo.getDeviceOSVersion());

      Map<String, String> applicationPayload1 = getApplicationPayload(applicationPayload, parameters, agentInfo, requestContext, uri,
              domainRequest, deviceInfo, channelType, channelAction, guid, pageId);
      applicationPayload1.put(Constants.CGUID, cguid);
      applicationPayload1.put("u", userId);
      applicationPayload1.put("userid", userId);
      applicationPayload1.put("rotid", rotationId);
      // applicationPayload
      record.setApplicationPayload(applicationPayload1);

      // cobrand
      record.setCobrand(cobrandParser.parse(appId, endUserContext.getUserAgent()));

      // rlogid
      record.setRlogid(tracingContext.getRlogId());

      // client data
      record.setClientData(getClientData(clientData, domainRequest, endUserContext, request, referer));

      // web server
      record.setWebServer(domainRequest.getHost());

      // ip
      record.setRemoteIP(getRemoteIp(request));
      record.setClientIP(domainRequest.getClientIp());

      // referer hash
      if (domainRequest.getReferrerUrl() != null) {
        record.setRefererHash(String.valueOf(domainRequest.getReferrerUrl().hashCode()));
      }

      // site id
      record.setSiteId(String.valueOf(domainRequest.getSiteId()));

      // rdt
      record.setRdt(rdt);

      // channel type and action
      record.setChannelType(channelType.toString());
      record.setChannelAction(channelAction.toString());

      return record;
    } catch (Exception e) {
      logger.warn("Failed to parse behavior message {} {}", uri, e.getMessage());
      return null;
    }
  }

  public BehaviorMessage parse(final HttpServletRequest request, ContainerRequestContext requestContext,
                               IEndUserContext endUserContext, MultiValueMap<String, String> parameters,
                               UserAgentInfo agentInfo, String referrer, String uri, Long startTime,
                               final ChannelType channelType, final ChannelAction channelAction,
                               Long snapshotId, int rdt) {

    Map<String, String> applicationPayload = new HashMap<>();
    Map<String, String> clientData = new HashMap<>();
    List<Map<String, String>> data = new ArrayList<>();

    // set default value
    BehaviorMessage record = new BehaviorMessage("", "", 0L, null, 0, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, applicationPayload, null, clientData, "", "", "", data);

    RequestTracingContext tracingContext =
        (RequestTracingContext) requestContext.getProperty(RequestTracingContext.NAME);
    DomainRequestData domainRequest = (DomainRequestData) requestContext.getProperty(DomainRequestData.NAME);

    // guid
    String trackingHeader = request.getHeader("X-EBAY-C-TRACKING");
    String guid = getData(Constants.GUID, trackingHeader);
    if (guid != null) {
      record.setGuid(guid);
    }

    // adguid
    String adguid = getData(Constants.ADGUID, trackingHeader);
    if (adguid != null) {
      record.setAdguid(adguid);
    }

    // source id
    record.setSid(parseTagFromParams(parameters, Constants.SOURCE_ID));

    // user id
    record.setUserId(parseTagFromParams(parameters, Constants.BEST_GUESS_USER));

    // eventTimestamp
    record.setEventTimestamp(startTime);

    int pageId = 0;
    // page info
    switch(channelAction) {
      case EMAIL_OPEN:
        pageId = PageIdEnum.EMAIL_OPEN.getId();
        record.setPageId(pageId);
        record.setPageName(PageNameEnum.OPEN.getName());
        break;
      case CLICK:
        pageId= PageIdEnum.CLICK.getId();
        record.setPageId(pageId);
        record.setPageName(PageNameEnum.CLICK.getName());
        break;
    }

    // event family and action
    record.setEventFamily(Constants.EVENT_FAMILY_CRM);
    record.setEventAction(Constants.EVENT_ACTION);

    // snapshotId
    record.setSnapshotId(String.valueOf(snapshotId));

    // fake session info
    record.setSessionId(String.valueOf(snapshotId));
    record.setSeqNum("1");

    // agent info
    record.setAgentInfo(endUserContext.getUserAgent());

    // app info
    String appId = CollectionServiceUtil.getAppIdFromUserAgent(agentInfo);
    record.setAppId(appId);
    if (agentInfo.getAppInfo() != null) {
      record.setAppVersion(agentInfo.getAppInfo().getAppVersion());
    }

    // url query string
    record.setUrlQueryString(UrlProcessHelper.getMaskedUrl(removeBsParam(parameters, uri), domainRequest.isSecure(),
        false));

    // device info
    DDSResponse deviceInfo = agentInfo.getDeviceInfo();
    record.setDeviceFamily(getDeviceFamily(deviceInfo));
    record.setDeviceType(deviceInfo.getOsName());
    record.setBrowserVersion(deviceInfo.getBrowserVersion());
    record.setBrowserFamily(deviceInfo.getBrowser());
    record.setOsVersion(deviceInfo.getDeviceOSVersion());
    record.setOsFamily(deviceInfo.getDeviceOS());
    record.setEnrichedOsVersion(deviceInfo.getDeviceOSVersion());

    // applicationPayload
    record.setApplicationPayload(getApplicationPayload(applicationPayload, parameters, agentInfo, requestContext, uri,
        domainRequest, deviceInfo, channelType, channelAction, guid, pageId));

    // cobrand
    record.setCobrand(cobrandParser.parse(appId, endUserContext.getUserAgent()));

    // rlogid
    record.setRlogid(tracingContext.getRlogId());

    // client data
    record.setClientData(getClientData(clientData, domainRequest, endUserContext, request, referrer));

    // web server
    record.setWebServer(domainRequest.getHost());

    // ip
    record.setRemoteIP(getRemoteIp(request));
    record.setClientIP(domainRequest.getClientIp());

    // referer hash
    if (domainRequest.getReferrerUrl() != null) {
      record.setRefererHash(String.valueOf(domainRequest.getReferrerUrl().hashCode()));
    }

    // site id
    record.setSiteId(String.valueOf(domainRequest.getSiteId()));

    // rdt
    record.setRdt(rdt);

    // channel type and action
    record.setChannelType(channelType.toString());
    record.setChannelAction(channelAction.toString());

    return record;
  }


  /**
   * Get client data
   */
  private Map<String, String> getClientData(Map<String, String> clientData, DomainRequestData domainRequest,
                                            IEndUserContext endUserContext, HttpServletRequest request,
                                            String referrer) {
    clientData.put("ForwardedFor", domainRequest.getXForwardedFor());
    clientData.put("Script", domainRequest.getServletPath());
    clientData.put("Server", domainRequest.getHost());
    InetAddress netAddress = getInetAddress();
    if (netAddress != null) {
      clientData.put("TMachine", netAddress.getHostAddress());
    }
    clientData.put("TName", domainRequest.getCommandName());
    clientData.put(AGENT_TAG, endUserContext.getUserAgent());
    clientData.put("RemoteIP", getRemoteIp(request));
    clientData.put("ContentLength", String.valueOf(domainRequest.getContentLength()));
    String ref = referrer;
    if(StringUtils.isEmpty(ref)) {
      ref = UrlProcessHelper.getMaskedUrl(domainRequest.getReferrerUrl(), false, true);
    }

    clientData.put("Referer", ref);
    clientData.put("Encoding", domainRequest.getAcceptEncoding());

    return deleteNullOrEmptyValue(clientData);
  }

  /**
   * Get device family
   */
  private String getDeviceFamily(DDSResponse deviceInfo) {
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
   * Get application payload
   */
  private Map<String, String> getApplicationPayload(Map<String, String> applicationPayload,
                                                    MultiValueMap<String, String> parameters, UserAgentInfo agentInfo,
                                                    ContainerRequestContext requestContext, String uri,
                                                    DomainRequestData domainRequest, DDSResponse deviceInfo,
                                                    ChannelType channelType, ChannelAction channelAction, String guid,
                                                    int pageId) {
    // add tags from parameters
    for (Map.Entry<String, String> entry : Constants.emailTagParamMap.entrySet()) {
      if (parameters.containsKey(entry.getValue()) && parameters.getFirst(entry.getValue()) != null) {
        applicationPayload.put(entry.getKey(), parseTagFromParams(parameters, entry.getValue()));
      }
    }

    // add tags in url param "sojTags" into applicationPayload
    applicationPayload = addSojTags(applicationPayload, parameters, channelType, channelAction);

    // add other tags
    // app id
    applicationPayload.put("app", CollectionServiceUtil.getAppIdFromUserAgent(agentInfo));

    // buyer access site id
    if (ChannelAction.EMAIL_OPEN.equals(channelAction)) {
      applicationPayload.put("bs", parseTagFromParams(parameters, Constants.CHOCO_BUYER_ACCESS_SITE_ID));
    } else {
      UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);
      applicationPayload.put("bs", String.valueOf(userPrefsCtx.getGeoContext().getSiteId()));
    }

    // facebook prefetch
    if (isFacebookPrefetchEnabled(requestContext)) {
      applicationPayload.put("fbprefetch", "true");
    }

    // guid
    applicationPayload.put("g", guid);

    // mobile operating system
    applicationPayload.put("mos", deviceInfo.getDeviceOS());

    // mobile operating system version
    applicationPayload.put("osv", deviceInfo.getDeviceOSVersion());

    // page id
    applicationPayload.put("p", String.valueOf(pageId));

    // referer
    applicationPayload.put("ref", UrlProcessHelper.getMaskedUrl(domainRequest.getReferrerUrl(), false, true));

    // site id
    applicationPayload.put("t", String.valueOf(domainRequest.getSiteId()));

    // decrypted user id
    applicationPayload.put("u", getDecryptedUserId(parameters));

    // delete choco_bs param if it exists
    uri = removeBsParam(parameters, uri);

    // landing page and tracking url
    applicationPayload.put("url_mpre", uri);

    // sid for DSS, just in tracking_event
    if (ChannelType.SITE_EMAIL.equals(channelType) || ChannelType.MRKT_EMAIL.equals(channelType)) {
      applicationPayload.put("sid", parseTagFromParams(parameters, Constants.SOURCE_ID));
    }

    // agent and payload
    applicationPayload.put(AGENT_TAG, agentInfo.getUserAgentRawData());
    applicationPayload.put("Payload", UrlProcessHelper.getMaskedUrl(uri, domainRequest.isSecure(), false));

    return encodeTags(deleteNullOrEmptyValue(applicationPayload));
  }

  /**
   * Add tags in param sojTags
   */
  private Map<String, String> addSojTags(Map<String, String> applicationPayload, MultiValueMap<String, String> parameters,
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
      if (!StringUtils.isEmpty(sojTags)) {
        StringTokenizer stToken = new StringTokenizer(sojTags, PresentationConstants.COMMA);
        while (stToken.hasMoreTokens()) {
          StringTokenizer sojNvp = new StringTokenizer(stToken.nextToken(), PresentationConstants.EQUALS);
          if (sojNvp.countTokens() == 2) {
            String sojTag = sojNvp.nextToken().trim();
            String urlParam = sojNvp.nextToken().trim();
            if (!StringUtils.isEmpty(urlParam) && !StringUtils.isEmpty(sojTag)) {
              applicationPayload.put(sojTag, parseTagFromParams(parameters, urlParam));
            }
          }
        }
      }
    }

    return applicationPayload;
  }

  /**
   * Get decrypted user id
   */
  private String getDecryptedUserId(MultiValueMap<String, String> parameters) {
    String bu = parseTagFromParams(parameters, Constants.BEST_GUESS_USER);

    if (!StringUtils.isEmpty(bu)) {
      return String.valueOf(EncryptUtil.decryptUserId(Long.valueOf(bu)));
    }

    return null;
  }

  /**
   * Parse tag from url query string and add to sojourner
   */
  public static String parseTagFromParams(MultiValueMap<String, String> parameters, String param) {
    if (parameters.containsKey(param) && parameters.get(param).get(0) != null) {
      return parameters.getFirst(param);
    }

    return "";
  }

  /**
   * Get the address of the local host
   */
  private static InetAddress getInetAddress() {
    try {
      return InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }

  /**
   * Delete map entry with null or empty value
   */
  private Map<String, String> deleteNullOrEmptyValue(Map<String, String> map) {
    Set<Map.Entry<String, String>> entrySet = map.entrySet();
    Iterator<Map.Entry<String, String>> iterator = entrySet.iterator();

    while(iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      if (StringUtils.isEmpty(entry.getValue())) {
        iterator.remove();
      }
    }

    return map;
  }

  /**
   * Encode tags
   */
  private Map<String, String> encodeTags(Map<String, String> inputMap) {
    Map<String, String> outputMap = new HashMap<>();
    for (Map.Entry<String, String> entry : inputMap.entrySet()) {
      if (AGENT_TAG.equals(entry.getKey()) || "Payload".equals(entry.getKey())) {
        outputMap.put(entry.getKey(), entry.getValue());
      } else {
        outputMap.put(entry.getKey(), FastURLEncoder.encode(entry.getValue(), "UTF-8"));
      }
    }

    return outputMap;
  }

  /**
   * Get request header value
   */
  private String getData(String key, String headerValue) {
    try {
      HeaderMultiValue headerMultiValue;
      if (headerValue != null) {
        headerMultiValue = new HeaderMultiValue(headerValue, "utf-8");
        return headerMultiValue.get(key);
      }
    } catch (UnsupportedEncodingException e) {
      logger.warn("Failed to parse header {}", headerValue, e);
    }

    return null;
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
   * Remove choco_bs param if it exists
   */
  private String removeBsParam(MultiValueMap<String, String> parameters, String uri) {
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
   * Get remote ip
   */
  private String getRemoteIp(HttpServletRequest request) {
    String remoteIp = null;
    String xForwardFor = request.getHeader("X-Forwarded-For");
    if (xForwardFor != null && !xForwardFor.isEmpty()) {
      remoteIp = xForwardFor.split(",")[0];
    }

    if (remoteIp == null || remoteIp.isEmpty()) {
      remoteIp = RequestUtil.getRemoteAddr(request);
    }

    return remoteIp == null ? "" : remoteIp;
  }

}
