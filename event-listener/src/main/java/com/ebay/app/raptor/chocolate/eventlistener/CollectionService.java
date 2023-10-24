package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.AkamaiMessage;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.collector.*;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.app.raptor.chocolate.eventlistener.request.CommonRequestHandler;
import com.ebay.app.raptor.chocolate.eventlistener.request.CustomizedSchemeRequestHandler;
import com.ebay.app.raptor.chocolate.eventlistener.request.StaticPageRequestHandler;
import com.ebay.app.raptor.chocolate.eventlistener.util.*;
import com.ebay.app.raptor.chocolate.gen.model.*;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import com.ebay.raptor.opentracing.SpanEventHelper;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.tracking.util.TrackerTagValueUtil;
import com.ebay.traffic.chocolate.kafka.AkamaiKafkaSink;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.kafka.UnifiedTrackingKafkaSink;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum;
import com.ebay.traffic.chocolate.utp.common.model.UnifiedTrackingMessage;
import com.ebay.traffic.monitoring.Field;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.ebay.app.raptor.chocolate.constant.ChannelActionEnum.*;
import static com.ebay.app.raptor.chocolate.constant.ChannelIdEnum.parse;
import static com.ebay.app.raptor.chocolate.constant.ChannelIdEnum.*;
import static com.ebay.app.raptor.chocolate.constant.Constants.*;
import static com.ebay.app.raptor.chocolate.eventlistener.util.UrlPatternUtil.*;

/**
 * @author xiangli4
 * The main logic of collection service:
 * 1. Check headers
 * 2. Parse everything from headers and bodies
 * 3. Add compatible headers
 * 4. Parse to ListenerMessage
 */
@Component
@DependsOn("EventListenerService")
public class CollectionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CollectionService.class);
  private ListenerMessageParser listenerMessageParser;
  private Producer unifiedTrackingProducer;
  private Producer akamaiProducer;
  private String unifiedTrackingTopic;
  private String internalClickTopic;
  private String akamaiTopic;
  private static CollectionService instance = null;
  private UnifiedTrackingMessageParser utpParser;
  private static final String TYPE_INFO = "Info";
  private static final String STATUS_OK = "0";

  @Autowired
  private PerformanceMarketingCollector performanceMarketingCollector;
  @Autowired
  private MrktEmailCollector mrktEmailCollector;
  @Autowired
  private SiteEmailCollector siteEmailCollector;
  @Autowired
  private ROICollector roiCollector;
  @Autowired
  private SMSCollector smsCollector;
  @Autowired
  private StaticPageRequestHandler staticPageRequestHandler;
  @Autowired
  private CustomizedSchemeRequestHandler customizedSchemeRequestHandler;
  @Autowired
  private CommonRequestHandler commonRequestHandler;
  @Autowired
  private GCXCollector gcxCollector;
  @Autowired
  private AdsClickCollector adsClickCollector;

  @Autowired
  private CouchbaseClientV2 couchbaseClientV2;
  private static final String PLATFORM = "platform";
  private static final String LANDING_PAGE_TYPE = "landingPageType";
  private static final String ADGUID_PARAM = "adguid";
  private static final String ROI_SOURCE = "roisrc";
  private static final List<String> REFERER_WHITELIST = Arrays.asList(
          "https://ebay.mtag.io", "https://ebay.pissedconsumer.com", "https://secureir.ebaystatic.com",
          "http://ebay.mtag.io", "http://ebay.pissedconsumer.com", "http://secureir.ebaystatic.com");
  private static final String ROI_TRANS_TYPE = "roiTransType";

  private  static final String FLEX_FIELD_2 = "ff2";

  @PostConstruct
  public void postInit() throws Exception {
    this.listenerMessageParser = ListenerMessageParser.getInstance();
    this.unifiedTrackingProducer = UnifiedTrackingKafkaSink.get();
    this.unifiedTrackingTopic = ApplicationOptions.getInstance().getUnifiedTrackingTopic();
    this.akamaiTopic = ApplicationOptions.getInstance().getAkamaiTopic();
    this.internalClickTopic = ApplicationOptions.getInstance().getInternalItmClickTopic();
    this.utpParser = new UnifiedTrackingMessageParser();
    this.akamaiProducer = AkamaiKafkaSink.get();
  }

  public boolean missMandatoryParams(MultiValueMap<String, String> parameters) {
    if (parameters.size() == 0) {
      LOGGER.warn(Errors.ERROR_NO_QUERY_PARAMETER);
      MonitorUtil.info(Errors.ERROR_NO_QUERY_PARAMETER);
      return true;
    }

    // XC-1695. no mkevt, rejected but return 201 accepted for clients since app team has started unconditionally call
    if (!parameters.containsKey(MKEVT) || parameters.get(MKEVT).get(0) == null) {
      LOGGER.warn(Errors.ERROR_NO_MKEVT);
      MonitorUtil.info(Errors.ERROR_NO_MKEVT);
      return true;
    }

    // XC-1695. mkevt != 1, rejected but return 201 accepted for clients
    String mkevt = parameters.get(MKEVT).get(0);
    if (!mkevt.equals(VALID_MKEVT_CLICK)) {
      LOGGER.warn(Errors.ERROR_INVALID_MKEVT);
      MonitorUtil.info(Errors.ERROR_INVALID_MKEVT);
      return true;
    }

    // parse channel from query mkcid
    // no mkcid, rejected but return 201 accepted for clients
    if (!parameters.containsKey(MKCID) || parameters.get(MKCID).get(0) == null) {
      LOGGER.warn(Errors.ERROR_NO_MKCID);
      MonitorUtil.info("NoMkcidParameter");
      return true;
    }
    return false;
  }

  /**
   * Decorate final target url and referer. There are usecases MCS has to reformat the URLs.
   * @param targetUrl original targetUrl
   * @param referer original referer
   * @return final target URL and referer
   * @throws Exception exception
   */
  protected Triple<String, String, ChannelIdEnum> getFinalUrlRefAndChannel(String targetUrl, String referer,
                                                                           UserPrefsCtx userPrefsCtx) throws Exception {
    String finalUrl = targetUrl;
    String finalRef = referer;
    // For e page, the real target url is in the referer
    // Since Chrome strict policy, referer may be cut off, so use 'originalUrl' parameter first as target url
    // if referer is existed, it will be in the target url (request body) parameter
    if (ePageSites.matcher(targetUrl.toLowerCase()).find()) {
      MonitorUtil.info("ePageIncoming");

      Event staticPageEvent = staticPageRequestHandler.parseStaticPageEvent(targetUrl, referer);
      finalUrl = staticPageEvent.getTargetUrl();
      finalRef = staticPageEvent.getReferrer();
    }

    // Now we support to track two kind of deeplink cases
    // XC-1797, extract and decode actual target url from referrer parameter in targetUrl,
    // only accept the url when the domain of referrer parameter belongs to ebay sites
    // XC-3349, for native uri with Chocolate parameters,
    // re-construct Chocolate url based on native uri and track (only support /itm page)
    Matcher deeplinkMatcher = deeplinksites.matcher(targetUrl.toLowerCase());
    if (deeplinkMatcher.find()) {
      MonitorUtil.info("IncomingAppDeepLink");

      Event customizedSchemeEvent = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetUrl, referer);
      if(customizedSchemeEvent == null) {
        logError(Errors.ERROR_NO_VALID_TRACKING_PARAMS_DEEPLINK);
      } else {
        finalUrl = customizedSchemeEvent.getTargetUrl();
        finalRef = customizedSchemeEvent.getReferrer();
      }
    }

    // get channel type
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(finalUrl).build();
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();

    // XC-1695. no query parameter,
    // rejected but return 201 accepted for clients since app team has started unconditionally call
    // check mandatory params, mkevt, mkcid
    if(missMandatoryParams(parameters)) {
      return null;
    }

    // get valid channel type
    ChannelIdEnum channelType;

    channelType = parse(parameters.get(MKCID).get(0));
    if (channelType == null) {
      LOGGER.warn(Errors.ERROR_INVALID_MKCID + " {}", targetUrl);
      MonitorUtil.info("InvalidMkcid");
      return null;
    }

    // XC-4947 Overwrite mkcid if the url is redirected from UFES and contains mksrc, this is for Message Center case
    if ((channelType == SITE_EMAIL || channelType == MRKT_EMAIL)
            && parameters.containsKey(UFES_REDIRECT)
            && (Boolean.TRUE.toString().equalsIgnoreCase(parameters.getFirst(UFES_REDIRECT)))
            && parameters.containsKey(MKSRC)
            && (SITE_MESSAGE_CENTER.getValue().equals(parameters.get(MKSRC).get(0)) || MRKT_MESSAGE_CENTER.getValue().equals(parameters.get(MKSRC).get(0)))) {
      MonitorUtil.info("OverwriteChannelIdForUFESMC");
      finalUrl = CollectionServiceUtil.replaceUrlParam(finalUrl, MKCID, parameters.get(MKSRC).get(0));
      uriComponents = UriComponentsBuilder.fromUriString(finalUrl).build();
      parameters = uriComponents.getQueryParams();
      channelType = parse(parameters.get(MKCID).get(0));
    }

    // for search engine free listings, append mkrid
    if (channelType == SEARCH_ENGINE_FREE_LISTINGS) {
      String rotationId = performanceMarketingCollector.getSearchEngineFreeListingsRotationId(userPrefsCtx);
      finalUrl = finalUrl + "&" + MKRID + "=" + rotationId;
    }

    // overwrite referer if the url is transformed from Rover to Chocolate by UFES
    Matcher roverSitesMatcher = roversites.matcher(finalRef.toLowerCase());
    if (roverSitesMatcher.find()
            && parameters.containsKey(UFES_REDIRECT)
            && (Boolean.TRUE.toString().equalsIgnoreCase(parameters.getFirst(UFES_REDIRECT)))) {
      finalRef = org.apache.commons.lang3.StringUtils.EMPTY;
    }

    return new ImmutableTriple<>(finalUrl, finalRef, channelType);
  }

  /**
   * Collect event and publish to kafka
   * @param request             raw request
   * @param endUserContext      wrapped end user context
   * @param raptorSecureContext wrapped secure header context. only click and roi need this.
   * @param requestContext      wrapped raptor request context
   * @param event               post body event
   * @return OK or Error message
   * @throws Exception when there is an unhandled error
   */
  public boolean collect(HttpServletRequest request, IEndUserContext endUserContext, RaptorSecureContext
          raptorSecureContext, ContainerRequestContext requestContext, Event event) throws Exception {

    Map<String, String> requestHeaders = commonRequestHandler.getHeaderMaps(request);

    validateTrackingHeader(request.getHeader(TRACKING_HEADER));
    validateEndUserCtxHeader(request.getHeader(ENDUSERCTX_HEADER));

    //Process ads product click
    adsClickCollector.processPromotedListingClick(endUserContext, event, requestContext.getHeaders());

    // get original referer from different sources
    String referer = commonRequestHandler.getReferer(event, requestHeaders, endUserContext);

    // get user agent
    String userAgent = endUserContext.getUserAgent();
    if (null == userAgent) {
      logError(Errors.ERROR_NO_USER_AGENT);
    }

    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);

    // get final url, ref and channel
    Triple<String, String, ChannelIdEnum> urlRefChannel = getFinalUrlRefAndChannel(event.getTargetUrl(), referer,
        userPrefsCtx);

    if(urlRefChannel == null) {
      return true;
    }
    // regenerate url parameters based on final url
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(urlRefChannel.getLeft()).build();
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();

    // get email partner
    String partner = siteEmailCollector.getEmailPartner(parameters, urlRefChannel.getRight());

    String landingPageType;
    List<String> pathSegments = uriComponents.getPathSegments();
    if (pathSegments.isEmpty()) {
      landingPageType = "home";
    } else {
      landingPageType = pathSegments.get(0);
    }

    // UFES metrics
    MonitorUtil.info("UFESTraffic", 1, Field.of("isUFES", CollectionServiceUtil.isFromUFES(requestHeaders).toString()),
        Field.of("statusCode", request.getHeader(NODE_REDIRECTION_HEADER_NAME)),
        Field.of(IS_BOT_TRAFFIC, CollectionServiceUtil.isBot(userAgent)));

    // UFES Redirect metrics
    if (parameters.containsKey(UFES_REDIRECT) && (Boolean.TRUE.toString().equalsIgnoreCase(parameters.getFirst(UFES_REDIRECT)))) {
        MonitorUtil.info("UFESRedirect", 1, Field.of(CHANNEL_TYPE, urlRefChannel.getRight().getLogicalChannel().getAvro().toString()),
                Field.of(IS_BOT_TRAFFIC, CollectionServiceUtil.isBot(userAgent)));
    }
    if (requestHeaders.containsKey(UFES_EDGTRKSVC_HDR)
            && Boolean.toString(true).equalsIgnoreCase(requestHeaders.get(UFES_EDGTRKSVC_HDR))) {
      MonitorUtil.info("UFESEdgTrkSvcHdrTrue");
    }

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = CollectionServiceUtil.getPlatform(agentInfo);

    String action = CLICK.toString();
    String type = urlRefChannel.getRight().getLogicalChannel().getAvro().toString();

    if (Arrays.asList(SITE_EMAIL.toString(),MRKT_EMAIL.toString(),GCX_EMAIL.toString(),SITE_MESSAGE_CENTER.toString(),
            MRKT_MESSAGE_CENTER.toString(),GCX_MESSAGE_CENTER.toString()).contains(type)){
      MonitorUtil.info("LandingPageType", 1,
              Field.of(LANDING_PAGE_TYPE, landingPageType),
              Field.of(CHANNEL_TYPE, type),
              Field.of(IS_BOT_TRAFFIC, CollectionServiceUtil.isBot(userAgent)));
    }


    // Self-service events, send them to couchbase
    if (parameters.containsKey(SELF_SERVICE) && parameters.containsKey(SELF_SERVICE_ID)) {
      if ("1".equals(parameters.getFirst(SELF_SERVICE)) &&
          parameters.getFirst(SELF_SERVICE_ID) != null) {
        MonitorUtil.info("SelfServiceIncoming");
        couchbaseClientV2.addSelfServiceRecord(parameters.getFirst(SELF_SERVICE_ID),
            urlRefChannel.getLeft());
        MonitorUtil.info("SelfServiceSuccess");

        return true;
      }
    }

    String userId = commonRequestHandler.getUserId(raptorSecureContext, endUserContext);

    // remote ip
    String remoteIp = commonRequestHandler.getRemoteIp(request);

    // checkout click flag
    boolean isClickFromCheckoutAPI = CollectionServiceUtil.isClickFromCheckoutAPI(
        urlRefChannel.getRight().getLogicalChannel().getAvro(), endUserContext);

    // 3rd party click flag
    boolean isThirdParty = CollectionServiceUtil.isThirdParityClick(parameters);

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type), Field.of(PLATFORM, platform));

    // construct the common event before parsing to different events (ubi, utp, filter, message tracker)
    BaseEvent baseEvent = new BaseEvent();
    baseEvent.setTimestamp(startTime);
    baseEvent.setUrl(urlRefChannel.getLeft());
    baseEvent.setReferer(urlRefChannel.getMiddle());
    baseEvent.setRemoteIp(remoteIp);
    baseEvent.setActionType(CLICK);
    baseEvent.setChannelType(urlRefChannel.getRight());
    baseEvent.setUriComponents(uriComponents);
    baseEvent.setUrlParameters(parameters);
    baseEvent.setRequestHeaders(requestHeaders);
    baseEvent.setUserAgentInfo(agentInfo);
    baseEvent.setUserPrefsCtx(userPrefsCtx);
    baseEvent.setEndUserContext(endUserContext);
    baseEvent.setUid(userId);
    baseEvent.setCheckoutApi(isClickFromCheckoutAPI);
    baseEvent.setPayload(event.getPayload());
    baseEvent.setThirdParty(isThirdParty);

    // update startTime if the click comes from checkoutAPI
    baseEvent = performanceMarketingCollector.setCheckoutTimestamp(baseEvent);

    // Overwrite the referer for the clicks from Promoted Listings iframe on ebay partner sites XC-3256
    // only for EPN channel
    boolean isEPNClickFromPromotedListings;
    try {
      isEPNClickFromPromotedListings = CollectionServiceUtil.isEPNPromotedListingsClick(baseEvent.getChannelType(),
          parameters, baseEvent.getReferer());

      if (isEPNClickFromPromotedListings) {
        baseEvent.setReferer(URLDecoder.decode(parameters.get(PLRFR).get(0), StandardCharsets.UTF_8.name()));
        MonitorUtil.info("OverwriteRefererForPromotedListingsClick");
      }
    } catch (Exception e) {
      LOGGER.error("Determine whether the click is from promoted listings iframe error");
      MonitorUtil.info("DeterminePromotedListingsClickError", 1);
    }

    // filter click whose referer is internal, and send to internal topic
    boolean isInternalRef = isInternalRef(baseEvent.getChannelType().getLogicalChannel().getAvro(),
        baseEvent.getReferer(), baseEvent.getUrl(), baseEvent.getUserAgentInfo());

    // filter duplicate clicks which are caused by ULK link and send to internal topic: XC-4032
    boolean isULKDuplicateClick = CollectionServiceUtil.isUlkDuplicateClick(baseEvent.getChannelType().getLogicalChannel().getAvro(),
            baseEvent.getReferer(), baseEvent.getUrl(), baseEvent.getUserAgentInfo());

    if(isInternalRef || isULKDuplicateClick) {
      Producer<Long, ListenerMessage> producer = KafkaSink.get();
      ListenerMessage listenerMessage = listenerMessageParser.parse(baseEvent);
      sendClickToInternalClickTopic(producer, listenerMessage);
    }

    // until now, generate eventId in advance of utp tracking so that it can be emitted into both ubi&utp only for click
    String utpEventId = UUID.randomUUID().toString();
    baseEvent.setUuid(utpEventId);

    // log all request headers for UFES debug
    SpanEventHelper.writeEvent(TYPE_INFO, "eventId", STATUS_OK, utpEventId);
    SpanEventHelper.writeEvent(TYPE_INFO, "requestHeaders", STATUS_OK, requestHeaders.toString());

    if(!isInternalRef && !isULKDuplicateClick) {
      // add channel specific tags, and produce message for EPN and IMK
      if (PM_CHANNELS.contains(baseEvent.getChannelType())) {
        firePMEvent(baseEvent, requestContext);
      } else {
        ChannelIdEnum channel = urlRefChannel.getRight();
        if (channel == SITE_EMAIL || channel == SITE_MESSAGE_CENTER) {
          fireCmEvent(baseEvent, requestContext, siteEmailCollector);
        } else if (channel == MRKT_EMAIL || channel == MRKT_MESSAGE_CENTER) {
          fireCmEvent(baseEvent, requestContext, mrktEmailCollector);
        } else if (channel == MRKT_SMS || channel == SITE_SMS) {
          fireCmEvent(baseEvent, requestContext, smsCollector);
        } else if (channel == GCX_EMAIL || channel == GCX_MESSAGE_CENTER) {
          fireGCXEvent(baseEvent, requestContext);
      }
    }
    }
    stopTimerAndLogData(baseEvent,Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
     Field.of(PLATFORM, platform));

    return true;
  }

  private void validateTrackingHeader(String trackingHeader) throws Exception {
    // validate mandatory cos headers
    if (trackingHeader == null) {
      logError(Errors.ERROR_NO_TRACKING);
    }
  }

  private void validateEndUserCtxHeader(String enduserctxHeader) throws Exception {
    // validate mandatory cos headers
    if (enduserctxHeader == null) {
      logError(Errors.ERROR_NO_ENDUSERCTX);
    }
  }

  protected boolean isInternalRef(ChannelType channelType, String referer, String finalUrl, UserAgentInfo userAgentInfo) {
    if (CollectionServiceUtil.inRefererWhitelist(referer) || CollectionServiceUtil.inPageWhitelist(finalUrl)
         || CollectionServiceUtil.inAdobePageWhitelist(channelType, referer, finalUrl, userAgentInfo)) {
      return false;
    } else if (ChannelType.SITE_MESSAGE_CENTER == channelType || ChannelType.MRKT_MESSAGE_CENTER == channelType
            || ChannelType.GCX_MESSAGE_CENTER == channelType) {
      return false;
    }

    // filter click whose referer is internal
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    return m.find();
  }

  /**
   * Collect roi event and publish to kafka
   * @param request             raw request
   * @param endUserContext      wrapped end user context
   * @param raptorSecureContext wrapped raptor secure context
   * @param requestContext      wrapped  request context
   * @param roiEvent            roi event body
   * @return                    success or failure
   * @throws Exception          when unhandled exception
   */
  public boolean collectROIEvent(HttpServletRequest request, IEndUserContext endUserContext, RaptorSecureContext
      raptorSecureContext, ContainerRequestContext requestContext, ROIEvent roiEvent) throws Exception {

    validateTrackingHeader(request.getHeader(TRACKING_HEADER));
    validateEndUserCtxHeader(request.getHeader(ENDUSERCTX_HEADER));

    Map<String, String> requestHeaders = commonRequestHandler.getHeaderMaps(request);

    String localTimestamp = Long.toString(System.currentTimeMillis());

    String userId = commonRequestHandler.getUserId(raptorSecureContext, endUserContext);

    roiCollector.setItemId(roiEvent);
    roiCollector.setTransTimestamp(roiEvent);
    roiCollector.setTransId(roiEvent);

    // Parse payload fields
    Map<String, String> payloadMap = roiEvent.getPayload();
    if(payloadMap == null) {
      payloadMap = new HashMap<>();
      roiEvent.setPayload(payloadMap);
    }

    // platform check by user agent
    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = CollectionServiceUtil.getPlatform(agentInfo);

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, ChannelActionEnum.ROI.toString()),
        Field.of(CHANNEL_TYPE, ChannelType.ROI.toString()), Field.of(PLATFORM, platform));

    String queryString = CollectionServiceUtil.generateQueryString(roiEvent, payloadMap, localTimestamp, userId);
    String targetUrl = request.getRequestURL() + "?" + queryString;
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();

    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();

    // we get referer from header or payload field,
    // first we get it from header, if it null, read payload
    String referer = endUserContext.getReferer();
    if (StringUtils.isEmpty(referer) && payloadMap.containsKey(REFERRER)) {
      referer = payloadMap.get(REFERRER);
    }

    if (StringUtils.isEmpty(referer) || referer.equalsIgnoreCase(STR_NULL)) {
      LOGGER.warn(Errors.ERROR_NO_REFERER);
      MonitorUtil.info(Errors.ERROR_NO_REFERER);
      referer = "";
    }
    // decode referer if necessary
    if(referer.startsWith(HTTPS_ENCODED) || referer.startsWith(HTTP_ENCODED)) {
      referer = URLDecoder.decode( referer, "UTF-8" );
    }

    // write roi event tags into ubi
    // Don't write into ubi if roi is from Checkout API
    boolean isRoiFromCheckoutAPI = CollectionServiceUtil.isROIFromCheckoutAPI(payloadMap, endUserContext);

    // remote ip
    String remoteIp = commonRequestHandler.getRemoteIp(request);

    // construct the common event before parsing to different events (ubi, utp, filter, message tracker)
    BaseEvent baseEvent = new BaseEvent();

    boolean isBesSrc = payloadMap.containsKey(ROI_SOURCE) && payloadMap.get(ROI_SOURCE).equals(String.valueOf(RoiSourceEnum.BES_SOURCE.getId()));
    boolean isCheckoutFf2 = payloadMap.containsKey(FLEX_FIELD_2) && payloadMap.get(FLEX_FIELD_2).startsWith("CHECKOUT");
    long placedDate = Long.parseLong(roiEvent.getPayload().get("placedDate"));
    if (isBesSrc && isCheckoutFf2 && placedDate > 0) {
      baseEvent.setTimestamp(placedDate);
    } else {
      baseEvent.setTimestamp(Long.parseLong(roiEvent.getTransactionTimestamp()));
    }

    baseEvent.setUrl(targetUrl);
    baseEvent.setReferer(referer);
    baseEvent.setRemoteIp(remoteIp);
    baseEvent.setActionType(ChannelActionEnum.ROI);
    baseEvent.setChannelType(ChannelIdEnum.ROI);
    baseEvent.setUriComponents(uriComponents);
    baseEvent.setUrlParameters(parameters);
    baseEvent.setRequestHeaders(requestHeaders);
    baseEvent.setUserAgentInfo(agentInfo);
    baseEvent.setUserPrefsCtx(userPrefsCtx);
    baseEvent.setEndUserContext(endUserContext);
    baseEvent.setUid(userId);
    baseEvent.setCheckoutApi(isRoiFromCheckoutAPI);
    baseEvent.setRoiEvent(roiEvent);
    baseEvent.setUuid(UUID.randomUUID().toString());

    // fire roi events
    fireROIEvent(baseEvent, requestContext);

    MonitorUtil.info("NewROICountAPI", 1, Field.of(CHANNEL_ACTION, "New-ROI"),
        Field.of(CHANNEL_TYPE, "New-ROI"), Field.of(ROI_SOURCE, String.valueOf(payloadMap.get(ROI_SOURCE))));
    // Log the roi lag between transation time and receive time
    MonitorUtil.latency("RoiTransationLag", startTime - Longs.tryParse(roiEvent.getTransactionTimestamp()),
        Field.of(CHANNEL_ACTION, "ROI"), Field.of(CHANNEL_TYPE, "ROI"));

    stopTimerAndLogData(startTime,
        Field.of(CHANNEL_ACTION, ChannelActionEnum.ROI.toString()), Field.of(CHANNEL_TYPE,
            ChannelType.ROI.toString()), Field.of(PLATFORM, platform));

    return true;
  }

  /**
   * Collect impression event and send pixel response
   * @param request             raw request
   * @param endUserContext      end user context header
   * @param requestContext      wrapped request context
   * @param event               impression event body
   * @return                    success or failure
   * @throws Exception          when unhandled exception
   */
  public boolean collectImpression(HttpServletRequest request, IEndUserContext endUserContext,
      ContainerRequestContext requestContext, Event event) throws Exception {

    Map<String, String> requestHeaders = commonRequestHandler.getHeaderMaps(request);

    SpanEventHelper.writeEvent(TYPE_INFO, "requestHeaders", STATUS_OK, requestHeaders.toString());

    // validate tracking header only, adservice does not construct enduserctx
    validateTrackingHeader(request.getHeader(TRACKING_HEADER));

    String referer = commonRequestHandler.getReferer(event, requestHeaders, endUserContext);

    String userAgent = endUserContext.getUserAgent();
    if (null == userAgent) {
      logError(Errors.ERROR_NO_USER_AGENT);
    }

    ChannelIdEnum channelType;
    ChannelActionEnum channelAction = null;

    // uri is from post body
    String uri = event.getTargetUrl();

    UriComponents uriComponents = UriComponentsBuilder.fromUriString(uri).build();

    // XC-1695
    // no query parameter, rejected but return 201 accepted for clients since app team has started unconditionally call
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
    if (parameters.size() == 0) {
      LOGGER.warn(Errors.ERROR_NO_QUERY_PARAMETER);
      MonitorUtil.info(Errors.ERROR_NO_QUERY_PARAMETER);
      return true;
    }

    // parse action from query param mkevt
    if (!parameters.containsKey(MKEVT) || parameters.get(MKEVT).get(0) == null) {
      LOGGER.warn(Errors.ERROR_NO_MKEVT);
      MonitorUtil.info(Errors.ERROR_NO_MKEVT);
    }

    // TODO refactor ChannelActionEnum
    // mkevt != 2, 3, 4, rejected
    String mkevt = parameters.get(MKEVT).get(0);
    switch (mkevt) {
      case "2":
        channelAction = IMPRESSION;
        break;
      case "3":
        channelAction = VIMP;
        break;
      case "4":
        channelAction = EMAIL_OPEN;
        break;
      case "6":
        channelAction = SERVE;
        break;
      default:
        logError(Errors.ERROR_INVALID_MKEVT);
    }

    // parse channel from query mkcid
    // no mkcid, accepted
    if (!parameters.containsKey(MKCID) || parameters.get(MKCID).get(0) == null) {
      LOGGER.warn(Errors.ERROR_NO_MKCID);
      MonitorUtil.info("NoMkcidParameter");
      return true;
    }

    // invalid mkcid, show error and accept
    channelType = parse(parameters.get(MKCID).get(0));
    if (channelType == null) {
      LOGGER.warn(Errors.ERROR_INVALID_MKCID + " {}", uri);
      MonitorUtil.info("InvalidMkcid");
      return true;
    }

    // check partner for email open
    String partner = siteEmailCollector.getEmailPartner(parameters, channelType);

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = CollectionServiceUtil.getPlatform(agentInfo);

    String action = channelAction.getAvro().toString();
    String type = channelType.getLogicalChannel().getAvro().toString();

    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type)
            , Field.of(PLATFORM, platform));

    // remote ip
    String remoteIp = commonRequestHandler.getRemoteIp(request);

    // construct the common event before parsing to different events (ubi, utp, filter, message tracker)
    BaseEvent baseEvent = new BaseEvent();
    baseEvent.setTimestamp(startTime);
    baseEvent.setUrl(uri);
    baseEvent.setReferer(referer);
    baseEvent.setRemoteIp(remoteIp);
    baseEvent.setActionType(channelAction);
    baseEvent.setChannelType(channelType);
    baseEvent.setUriComponents(uriComponents);
    baseEvent.setUrlParameters(parameters);
    baseEvent.setRequestHeaders(requestHeaders);
    baseEvent.setUserAgentInfo(agentInfo);
    baseEvent.setUserPrefsCtx(userPrefsCtx);
    baseEvent.setEndUserContext(endUserContext);
    String utpEventId = UUID.randomUUID().toString();
    baseEvent.setUuid(utpEventId);

    SpanEventHelper.writeEvent(TYPE_INFO, "eventId", STATUS_OK, utpEventId);

    // add channel specific tags, and produce message for EPN and IMK
    if (channelType == SITE_EMAIL || channelType == SITE_MESSAGE_CENTER) {
      fireCmEvent(baseEvent, requestContext, siteEmailCollector);
    } else if (channelType == MRKT_EMAIL || channelType == MRKT_MESSAGE_CENTER) {
      fireCmEvent(baseEvent, requestContext, mrktEmailCollector);
    } else if (channelType == GCX_EMAIL || channelType == GCX_MESSAGE_CENTER) {
      fireGCXEvent(baseEvent, requestContext);
    } else {
      firePMEvent(baseEvent, requestContext);
    }

    stopTimerAndLogData(baseEvent, Field.of(CHANNEL_ACTION, action),
        Field.of(CHANNEL_TYPE, type), Field.of(PLATFORM, platform));

    return true;
  }

  /**
   *
   * @param baseEvent           base event
   * @return                    roi listener message
   */
  private void fireROIEvent(BaseEvent baseEvent,
                                       ContainerRequestContext containerRequestContext) {

    // Parse the response
    ListenerMessage message = listenerMessageParser.parse(baseEvent);

    // 1. send to listener topic
    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    String kafkaTopic
        = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(baseEvent.getChannelType().getLogicalChannel()
        .getAvro());

    producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);

    // 2. track ubi
    // Checkout ROI won't go to UBI
    // Committed & non-sco wont go to UBI
    Map<String, String> roiPayload = baseEvent.getRoiEvent().getPayload();
    if (baseEvent.isCheckoutApi()) {
      MonitorUtil.info("CheckoutAPIROI", 1);
    } else {
      if (CollectionServiceUtil.isValidROI(roiPayload)) {
        roiCollector.trackUbi(containerRequestContext, baseEvent);
      }
    }

    // 3. fire utp event
    submitChocolateUtpEvent(baseEvent, containerRequestContext,
        message.getSnapshotId(), message.getShortSnapshotId());

    // Mock click for the ROI which has valid mppid
    // in the payload (ROI generated from pre-install app on Android) XC-3464
    boolean isPreInstallROI = CollectionServiceUtil.isPreinstallROI(baseEvent.getRoiEvent().getPayload(),
        baseEvent.getRoiEvent().getTransType());

    if (isPreInstallROI) {

      String clickUrl = CollectionServiceUtil.createPrmClickUrl(baseEvent.getRoiEvent().getPayload(),
          baseEvent.getEndUserContext());
      UriComponents clickUriComponents = UriComponentsBuilder.fromUriString(clickUrl).build();

      MultiValueMap<String, String> clickParameters = clickUriComponents.getQueryParams();

      baseEvent.setUrl(clickUrl);
      baseEvent.setActionType(CLICK);
      baseEvent.setChannelType(DAP);
      baseEvent.setUriComponents(clickUriComponents);
      baseEvent.setUrlParameters(clickParameters);
      baseEvent.setUuid(UUID.randomUUID().toString());

      ListenerMessage mockClickListenerMessage = listenerMessageParser.parse(baseEvent);
      // switch to display channel topic
      kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(baseEvent.getChannelType()
          .getLogicalChannel().getAvro());
      producer.send(
          new ProducerRecord<>(kafkaTopic, mockClickListenerMessage.getSnapshotId(),
              mockClickListenerMessage), KafkaSink.callback);

      // submit utp event
      submitChocolateUtpEvent(baseEvent, containerRequestContext, mockClickListenerMessage.getSnapshotId(),
          mockClickListenerMessage.getShortSnapshotId());

      // Log mock click for pre-install ROI by transaction type
      MonitorUtil.info("PreInstallMockClick", 1, Field.of(CHANNEL_ACTION, CLICK.toString()),
          Field.of(CHANNEL_TYPE, DAP.getLogicalChannel().getAvro().toString()),
          Field.of(ROI_TRANS_TYPE, baseEvent.getRoiEvent().getTransType()));
    }
  }


  /**
   * Collect sync event and publish to ubi only
   *
   * @param request             raw request
   * @param event               post body event
   * @return OK or Error message
   */
  public boolean collectSync(HttpServletRequest request, ContainerRequestContext requestContext,
                             Event event) throws Exception {

    validateTrackingHeader(request.getHeader(TRACKING_HEADER));

    String referer = null;
    if (!StringUtils.isEmpty(event.getReferrer())) {
      referer = event.getReferrer();
    }

    // targetUrl is from post body
    String targetUrl = event.getTargetUrl();

    // illegal url, rejected
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();

    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
    if (parameters.size() == 0) {
      LOGGER.warn(Errors.ERROR_NO_QUERY_PARAMETER);
      MonitorUtil.info(Errors.ERROR_NO_QUERY_PARAMETER);
      return true;
    }

    String adguid = parameters.getFirst(ADGUID_PARAM);

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);

    // write ubi. We cannot use addCommonTags here since the referer is internal of ebay
    try {
      // Ubi tracking
      IRequestScopeTracker requestTracker =
          (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

      // page id. share the same page id with ar and impression
      requestTracker.addTag(TrackerTagValueUtil.PageIdTag, PageIdEnum.AR.getId(), Integer.class);

      // event action
      requestTracker.addTag(TrackerTagValueUtil.EventActionTag, EVENT_ACTION, String.class);

      // target url
      if (!StringUtils.isEmpty(targetUrl)) {
        requestTracker.addTag(SOJ_MPRE_TAG, targetUrl, String.class);
      }

      // referer
      if (!StringUtils.isEmpty(referer)) {
        requestTracker.addTag("ref", referer, String.class);
      }

      // adguid
      requestTracker.addTag(ADGUID_PARAM, adguid, String.class);

      // populate device info
      CollectionServiceUtil.populateDeviceDetectionParams(agentInfo, requestTracker);

    } catch (Exception e) {
      LOGGER.warn("Error when tracking ubi for adguid", e);
      MonitorUtil.info("ErrorWriteAdguidToUBI");
    }

    return true;
  }

  /**
   * Collect unified tracking event and publish to kafka
   *
   * @param event               post body event
   */
  public void collectUnifiedTrackingEvent(UnifiedTrackingEvent event) {
    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, event.getActionType()),
        Field.of(CHANNEL_TYPE, event.getChannelType()),Field.of(PLATFORM, "NULL"));


    UnifiedTrackingMessage message = utpParser.parse(event);
    SpanEventHelper.writeEvent(TYPE_INFO, "eventId", STATUS_OK, message.getEventId());
    SpanEventHelper.writeEvent(TYPE_INFO, "producerEventId", STATUS_OK, message.getProducerEventId());
    SpanEventHelper.writeEvent(TYPE_INFO, "service", STATUS_OK, message.getService());
    SpanEventHelper.writeEvent(TYPE_INFO, "server", STATUS_OK, message.getServer());

    if (message != null)
      unifiedTrackingProducer.send(new ProducerRecord<>(unifiedTrackingTopic, message.getEventId().getBytes(), message),
          UnifiedTrackingKafkaSink.callback);

    stopTimerAndLogData(startTime, Field.of(CHANNEL_ACTION, event.getActionType()),
        Field.of(CHANNEL_TYPE, event.getChannelType()), Field.of(PLATFORM, "NULL"));
  }

  /**
   * Collect Akamai cached SEO events
   *
   * @param events post body
   */
  public void collectAkamai(List<AkamaiEvent> events) {
    long startTime = startTimerAndLogData(Field.of(CHANNEL_TYPE, "SEO"));
    if (!events.isEmpty()) {
      for (AkamaiEvent event : events) {
        MonitorUtil.info("AkamaiIncoming");
        AkamaiMessage message = new AkamaiMessage();
        BeanUtils.copyProperties(event, message);

        if (message != null)
          akamaiProducer.send(new ProducerRecord<>(akamaiTopic, message.getReqId().getBytes(), message),
              AkamaiKafkaSink.callback);
      }
    }
    stopTimerAndLogData(startTime, Field.of(CHANNEL_TYPE, "SEO"));
  }

  /**
   * Submit chocolate tracked user behavior into utp event
   * @param baseEvent       base event
   * @param requestContext  request context
   * @param snapshotId      snapshot id
   * @param shortSnapshotId short snapshot id
   */
  private void submitChocolateUtpEvent(BaseEvent baseEvent, ContainerRequestContext requestContext, long snapshotId,
                                       long shortSnapshotId) {
    try {
      UnifiedTrackingMessage utpMessage = utpParser.parse(baseEvent, requestContext, snapshotId,
          shortSnapshotId);
      String channel = utpMessage.getChannelType();
      if (ChannelTypeEnum.SEARCH_ENGINE_FREE_LISTINGS.getValue().equals(channel)
          && utpMessage.getIsBot()) {
        MonitorUtil.info("CollectionServiceSkipFreeListingBot");
      } else if ((ChannelTypeEnum.SITE_EMAIL.getValue().equals(channel)
              || ChannelTypeEnum.SITE_MESSAGE_CENTER.getValue().equals(channel)
              || ChannelTypeEnum.MRKT_EMAIL.getValue().equals(channel)
              || ChannelTypeEnum.MRKT_MESSAGE_CENTER.getValue().equals(channel)
              || ChannelTypeEnum.GCX_EMAIL.getValue().equals(channel)
              || ChannelTypeEnum.GCX_MESSAGE_CENTER.getValue().equals(channel)
              || ChannelTypeEnum.MRKT_SMS.getValue().equals(channel)
              || ChannelTypeEnum.SITE_SMS.getValue().equals(channel))
              && ActionTypeEnum.CLICK.getValue().equals(utpMessage.getActionType()) && !baseEvent.isThirdParty()) {
        MonitorUtil.info("UTPSkipChocolateEmailClick");
      } else {
        unifiedTrackingProducer.send(new ProducerRecord<>(unifiedTrackingTopic, utpMessage.getEventId().getBytes(),
                utpMessage), UnifiedTrackingKafkaSink.callback);
      }
    } catch (Exception e) {
      LOGGER.warn("UTP message process error.", e);
      MonitorUtil.info("UTPMessageError");
    }
  }

  /**
   * Fire PM events to the streams
   * @param baseEvent base event
   * @param requestContext request context
   */
  private void firePMEvent(BaseEvent baseEvent, ContainerRequestContext requestContext) {

    ListenerMessage listenerMessage = performanceMarketingCollector.decorateListenerMessageAndHandleGDPR(baseEvent);

    // 1. send to chocolate topic
    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    String kafkaTopic = ApplicationOptions.getInstance()
        .getSinkKafkaConfigs().get(baseEvent.getChannelType().getLogicalChannel().getAvro());

    producer.send(
        new ProducerRecord<>(kafkaTopic, listenerMessage.getSnapshotId(), listenerMessage),
        KafkaSink.callback);

    // 2. track ubi
    // Checkout click events won't go to UBI
    // Only track click to UBI
    if (baseEvent.isCheckoutApi()) {
      MonitorUtil.info("CheckoutAPIClick", 1);
    } else if (baseEvent.getActionType().equals(CLICK)) {
      performanceMarketingCollector.trackUbi(requestContext, baseEvent, listenerMessage);
    }

    // 3. submit utp event
    submitChocolateUtpEvent(baseEvent, requestContext,
        listenerMessage.getSnapshotId(), listenerMessage.getShortSnapshotId());
  }


  private void fireCmEvent(BaseEvent baseEvent, ContainerRequestContext requestContext,
                               CustomerMarketingCollector cmCollector) {

    // 1. track ubi
    if (CLICK.equals(baseEvent.getActionType())) {
      cmCollector.trackUbi(requestContext, baseEvent);
    }

    // 3. fire utp event
    submitChocolateUtpEvent(baseEvent, requestContext, 0L, 0L);
  }

  private void fireGCXEvent(BaseEvent baseEvent, ContainerRequestContext requestContext) {
    // 1. track ubi
    if (CLICK.equals(baseEvent.getActionType())) {
      gcxCollector.trackUbi(requestContext, baseEvent);
    }
    // fire utp event
    submitChocolateUtpEvent(baseEvent, requestContext, 0L, 0L);
  }

  /**
   * log error, log metric and throw error with error key
   *
   * @param  error error type
   * @throws Exception exception with error key
   */
  private void logError(String error) throws Exception {
    LOGGER.warn(error);
    MonitorUtil.info(error);
    throw new Exception(error);
  }

  /**
   * Starts the timer and logs some basic info
   *
   * @param additionalFields channelAction, channelType, platform, landing page type
   * @return start time
   */
  private long startTimerAndLogData(Field<String, Object>... additionalFields) {
    // the main rover process is already finished at this moment
    // use the timestamp from request as the start time
    long startTime = System.currentTimeMillis();
    LOGGER.debug(String.format("StartTime: %d", startTime));
    MonitorUtil.info("CollectionServiceIncoming", 1,  additionalFields);
    return startTime;
  }

  /**
   * Stops the timer and logs relevant debugging messages
   *
   * @param eventProcessStartTime     actual process start time for incoming event, so that latency can be calculated
   * @param additionalFields channelAction, channelType, platform, landing page type
   */
  private void stopTimerAndLogData(long eventProcessStartTime, Field<String, Object>... additionalFields) {
    long endTime = System.currentTimeMillis();
    LOGGER.debug(String.format("EndTime: %d", endTime));
    MonitorUtil.info("CollectionServiceSuccess", 1,  additionalFields);
    MonitorUtil.latency("CollectionServiceAverageLatency", endTime - eventProcessStartTime);
  }

  private void stopTimerAndLogData(BaseEvent baseEvent, Field<String, Object>... additionalFields) {
    long endTime = System.currentTimeMillis();
    LOGGER.debug(String.format("EndTime: %d", endTime));
    MonitorUtil.info("CollectionServiceSuccess", 1,  additionalFields);
    if (baseEvent.isCheckoutApi()) {
      MonitorUtil.latency("CollectionServiceCheckoutAPIClickAndROIAverageLatency", endTime - baseEvent.getTimestamp());
    } else {
      MonitorUtil.latency("CollectionServiceAverageLatency", endTime - baseEvent.getTimestamp());
    }
  }

  /**
   * Drop internal clicks into internalClickTopic
   */
  private void sendClickToInternalClickTopic(Producer<Long, ListenerMessage> producer, ListenerMessage message) {
    producer.send(new ProducerRecord<>(internalClickTopic, message.getSnapshotId(), message), KafkaSink.callback);
    MonitorUtil.info("InternalClick", 1, Field.of(CHANNEL_ACTION, message.getChannelAction().toString()),
        Field.of(CHANNEL_TYPE, message.getChannelType().toString()));
  }


}
