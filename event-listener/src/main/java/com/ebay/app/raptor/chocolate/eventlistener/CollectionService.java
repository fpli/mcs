package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.traffic.chocolate.utp.common.model.UnifiedTrackingMessage;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.collector.*;
import com.ebay.app.raptor.chocolate.eventlistener.component.GdprConsentHandler;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.request.CommonRequestHandler;
import com.ebay.app.raptor.chocolate.eventlistener.request.CustomizedSchemeRequestHandler;
import com.ebay.app.raptor.chocolate.eventlistener.request.StaticPageRequestHandler;
import com.ebay.app.raptor.chocolate.eventlistener.util.*;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.app.raptor.chocolate.gen.model.UnifiedTrackingEvent;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.cosadaptor.token.ISecureTokenManager;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.tracking.util.TrackerTagValueUtil;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.kafka.UnifiedTrackingKafkaSink;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import com.ebay.userlookup.UserLookup;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;

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
  private Metrics metrics;
  private ListenerMessageParser listenerMessageParser;
  private BehaviorMessageParser behaviorMessageParser;
  private Producer behaviorProducer;
  private String behaviorTopic;
  private Producer unifiedTrackingProducer;
  private String unifiedTrackingTopic;
  // collect duplicate click
  private String duplicateItmClickTopic;
  private static CollectionService instance = null;
  private UnifiedTrackingMessageParser utpParser;

  @Autowired
  private HttpRoverClient roverClient;
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

  private static final String PARTNER = "partner";
  private static final String PLATFORM = "platform";
  private static final String LANDING_PAGE_TYPE = "landingPageType";
  private static final String ADGUID_PARAM = "adguid";
  private static final String ROI_SOURCE = "roisrc";
  private static final String ROVER_INTERNAL_VIP = "internal.rover.vip.ebay.com";

  @PostConstruct
  public void postInit() throws Exception {
    this.metrics = ESMetrics.getInstance();
    this.listenerMessageParser = ListenerMessageParser.getInstance();
    this.behaviorMessageParser = BehaviorMessageParser.getInstance();
    this.behaviorProducer = BehaviorKafkaSink.get();
    this.behaviorTopic = ApplicationOptions.getInstance().getProduceBehaviorTopic();
    this.unifiedTrackingProducer = UnifiedTrackingKafkaSink.get();
    this.unifiedTrackingTopic = ApplicationOptions.getInstance().getUnifiedTrackingTopic();
    this.duplicateItmClickTopic = ApplicationOptions.getInstance().getDuplicateItmClickTopic();
    this.utpParser = new UnifiedTrackingMessageParser();
  }

  public boolean missMandatoryParams(MultiValueMap<String, String> parameters) {
    if (parameters.size() == 0) {
      LOGGER.warn(Errors.ERROR_NO_QUERY_PARAMETER);
      metrics.meter(Errors.ERROR_NO_QUERY_PARAMETER);
      return true;
    }

    // XC-1695. no mkevt, rejected but return 201 accepted for clients since app team has started unconditionally call
    if (!parameters.containsKey(Constants.MKEVT) || parameters.get(Constants.MKEVT).get(0) == null) {
      LOGGER.warn(Errors.ERROR_NO_MKEVT);
      metrics.meter(Errors.ERROR_NO_MKEVT);
      return true;
    }

    // XC-1695. mkevt != 1, rejected but return 201 accepted for clients
    String mkevt = parameters.get(Constants.MKEVT).get(0);
    if (!mkevt.equals(Constants.VALID_MKEVT_CLICK)) {
      LOGGER.warn(Errors.ERROR_INVALID_MKEVT);
      metrics.meter(Errors.ERROR_INVALID_MKEVT);
      return true;
    }

    // parse channel from query mkcid
    // no mkcid, rejected but return 201 accepted for clients
    if (!parameters.containsKey(Constants.MKCID) || parameters.get(Constants.MKCID).get(0) == null) {
      LOGGER.warn(Errors.ERROR_NO_MKCID);
      metrics.meter("NoMkcidParameter");
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
      metrics.meter("ePageIncoming");

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
      metrics.meter("IncomingAppDeepLink");

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

    channelType = ChannelIdEnum.parse(parameters.get(Constants.MKCID).get(0));
    if (channelType == null) {
      LOGGER.warn(Errors.ERROR_INVALID_MKCID + " {}", targetUrl);
      metrics.meter("InvalidMkcid");
      return null;
    }

    // for search engine free listings, append mkrid
    if (channelType == ChannelIdEnum.SEARCH_ENGINE_FREE_LISTINGS) {
      String rotationId = performanceMarketingCollector.getSearchEngineFreeListingsRotationId(userPrefsCtx);
      finalUrl = finalUrl + "&" + Constants.MKRID + "=" + rotationId;
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

    // get original referer from different sources
    String referer = commonRequestHandler.getReferer(event, requestHeaders, endUserContext);

    // legacy rover deeplink case. Forward it to rover. We control this at our backend in case mobile app miss it
    Matcher roverSitesMatcher = roversites.matcher(referer.toLowerCase());
    if (roverSitesMatcher.find()) {
      roverClient.forwardRequestToRover(referer, ROVER_INTERNAL_VIP, request);
      return true;
    }

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
    metrics.meter("UFESTraffic", 1, Field.of("isUFES", CollectionServiceUtil.isFromUFES(requestHeaders).toString()),
        Field.of(LANDING_PAGE_TYPE, landingPageType),
        Field.of("statusCode", request.getHeader(Constants.NODE_REDIRECTION_HEADER_NAME)));

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = CollectionServiceUtil.getPlatform(agentInfo);

    String action = ChannelActionEnum.CLICK.toString();
    String type = urlRefChannel.getRight().getLogicalChannel().getAvro().toString();

    // Self-service events, send them to couchbase
    if (parameters.containsKey(Constants.SELF_SERVICE) && parameters.containsKey(Constants.SELF_SERVICE_ID)) {
      if ("1".equals(parameters.getFirst(Constants.SELF_SERVICE)) &&
          parameters.getFirst(Constants.SELF_SERVICE_ID) != null) {
        metrics.meter("SelfServiceIncoming");
        CouchbaseClient.getInstance().addSelfServiceRecord(parameters.getFirst(Constants.SELF_SERVICE_ID),
            urlRefChannel.getLeft());
        metrics.meter("SelfServiceSuccess");

        return true;
      }
    }

    String userId = commonRequestHandler.getUserId(raptorSecureContext, endUserContext);

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
        Field.of(PARTNER, partner), Field.of(PLATFORM, platform),
        Field.of(LANDING_PAGE_TYPE, landingPageType));

    // construct the common event before parsing to different events (ubi, utp, filter, message tracker)
    BaseEvent baseEvent = new BaseEvent();
    baseEvent.setTimestamp(startTime);
    baseEvent.setUrl(urlRefChannel.getLeft());
    baseEvent.setReferer(urlRefChannel.getMiddle());
    baseEvent.setActionType(ChannelActionEnum.CLICK);
    baseEvent.setChannelType(urlRefChannel.getRight());
    baseEvent.setUriComponents(uriComponents);
    baseEvent.setUrlParameters(parameters);
    baseEvent.setRequestHeaders(requestHeaders);
    baseEvent.setUserAgentInfo(agentInfo);
    baseEvent.setUserPrefsCtx(userPrefsCtx);
    baseEvent.setEndUserContext(endUserContext);
    baseEvent.setUid(userId);

    // update startTime if the click comes from checkoutAPI
    try {
      baseEvent = performanceMarketingCollector.setCheckoutApiFlag(baseEvent);
    } catch (Exception e) {
      LOGGER.warn(e.getMessage());
      LOGGER.warn("Error click timestamp from Checkout API " + baseEvent.getTimestamp());
      metrics.meter("ErrorCheckoutAPIClickTimestamp", 1);
    }

    // Overwrite the referer for the clicks from Promoted Listings iframe on ebay partner sites XC-3256
    // only for EPN channel
    boolean isEPNClickFromPromotedListings;
    try {
      isEPNClickFromPromotedListings = CollectionServiceUtil.isEPNPromotedListingsClick(baseEvent.getChannelType(),
          parameters, baseEvent.getReferer());

      if (isEPNClickFromPromotedListings) {
        baseEvent.setReferer(URLDecoder.decode(parameters.get(Constants.PLRFR).get(0), StandardCharsets.UTF_8.name()));
        metrics.meter("OverwriteRefererForPromotedListingsClick");
      }
    } catch (Exception e) {
      LOGGER.error("Determine whether the click is from promoted listings iframe error");
      metrics.meter("DeterminePromotedListingsClickError", 1);
    }

    // filter click whose referer is internal
    Matcher m = ebaysites.matcher(baseEvent.getReferer().toLowerCase());
    boolean isInternalRef = m.find();
    // Determine whether the click is a duplicate click
    // If duplicate click, then drop into duplicateItmClickTopic
    // If not, drop into normal topic
    boolean isDuplicateClick = false;
    try {
      isDuplicateClick = CollectionServiceUtil.isDuplicateItmClick(baseEvent);

      // send duplicate click to a dedicate listener topic
      if(isDuplicateClick || isInternalRef) {
        Producer<Long, ListenerMessage> producer = KafkaSink.get();
        ListenerMessage listenerMessage = listenerMessageParser.parse(baseEvent);
        Long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId()).getRepresentation();
        listenerMessage.setSnapshotId(snapshotId);
        listenerMessage.setShortSnapshotId(0L);
        sendClickToDuplicateItmClickTopic(producer, listenerMessage);
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      LOGGER.error("Determine whether the click is duplicate item click error.");
      metrics.meter("DetermineDuplicateItmClickError", 1);
    }

    // until now, generate eventId in advance of utp tracking so that it can be emitted into both ubi&utp only for click
    String utpEventId = UUID.randomUUID().toString();
    baseEvent.setUuid(utpEventId);

    if(!isDuplicateClick && !isInternalRef ) {
      ListenerMessage listenerMessage = null;
      // add channel specific tags, and produce message for EPN and IMK
      if (PM_CHANNELS.contains(baseEvent.getChannelType())) {

        listenerMessage = firePMEvent(baseEvent, requestContext);
      }
      else if (urlRefChannel.getRight() == ChannelIdEnum.SITE_EMAIL) {
        fireCmEvent(baseEvent, requestContext, siteEmailCollector);
      }
      else if (urlRefChannel.getRight() == ChannelIdEnum.MRKT_EMAIL) {
        fireCmEvent(baseEvent, requestContext, mrktEmailCollector);
      }
      else if (urlRefChannel.getRight() == ChannelIdEnum.MRKT_SMS
          || urlRefChannel.getRight() == ChannelIdEnum.SITE_SMS) {
        fireCmEvent(baseEvent, requestContext, smsCollector);
      }

      // send to unified tracking topic
      if (listenerMessage != null) {
        submitChocolateUtpEvent(baseEvent, requestContext, null,
            listenerMessage.getSnapshotId(), listenerMessage.getShortSnapshotId(), utpEventId, startTime);
      } else {
        submitChocolateUtpEvent(baseEvent, requestContext, null, 0L,
            0L, utpEventId, startTime);
      }
    }
    stopTimerAndLogData(baseEvent, Field.of(CHANNEL_ACTION, action),
        Field.of(CHANNEL_TYPE, type), Field.of(PARTNER, partner), Field.of(PLATFORM, platform),
        Field.of(LANDING_PAGE_TYPE, landingPageType));

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
      metrics.meter(Errors.ERROR_NO_REFERER);
      referer = "";
    }
    // decode referer if necessary
    if(referer.startsWith(HTTPS_ENCODED) || referer.startsWith(HTTP_ENCODED)) {
      referer = URLDecoder.decode( referer, "UTF-8" );
    }

    // write roi event tags into ubi
    // Don't write into ubi if roi is from Checkout API
    boolean isRoiFromCheckoutAPI = CollectionServiceUtil.isROIFromCheckoutAPI(payloadMap, endUserContext);
    if(isRoiFromCheckoutAPI) {
      metrics.meter("CheckoutAPIROI", 1);
    }

    // construct the common event before parsing to different events (ubi, utp, filter, message tracker)
    BaseEvent baseEvent = new BaseEvent();
    baseEvent.setTimestamp(Long.parseLong(roiEvent.getTransactionTimestamp()));
    baseEvent.setUrl(targetUrl);
    baseEvent.setReferer(referer);
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

    // Write roi event to kafka output topic
    ListenerMessage listenerMessage = fireROIEvent(baseEvent, requestContext);

    metrics.meter("NewROICountAPI", 1, Field.of(CHANNEL_ACTION, "New-ROI"),
        Field.of(CHANNEL_TYPE, "New-ROI"), Field.of(ROI_SOURCE, String.valueOf(payloadMap.get(ROI_SOURCE))));
    // Log the roi lag between transation time and receive time
    metrics.mean("RoiTransationLag", startTime - Longs.tryParse(roiEvent.getTransactionTimestamp()),
        Field.of(CHANNEL_ACTION, "ROI"), Field.of(CHANNEL_TYPE, "ROI"));
    stopTimerAndLogData(startTime,
        Field.of(CHANNEL_ACTION, ChannelActionEnum.ROI.toString()), Field.of(CHANNEL_TYPE,
            ChannelType.ROI.toString()), Field.of(PLATFORM, platform));

    submitChocolateUtpEvent(baseEvent, requestContext, roiEvent,
        listenerMessage.getSnapshotId(), listenerMessage.getShortSnapshotId(), null, startTime);

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
      metrics.meter(Errors.ERROR_NO_QUERY_PARAMETER);
      return true;
    }

    // parse action from query param mkevt
    if (!parameters.containsKey(Constants.MKEVT) || parameters.get(Constants.MKEVT).get(0) == null) {
      LOGGER.warn(Errors.ERROR_NO_MKEVT);
      metrics.meter(Errors.ERROR_NO_MKEVT);
    }

    // TODO refactor ChannelActionEnum
    // mkevt != 2, 3, 4, rejected
    String mkevt = parameters.get(Constants.MKEVT).get(0);
    switch (mkevt) {
      case "2":
        channelAction = ChannelActionEnum.IMPRESSION;
        break;
      case "3":
        channelAction = ChannelActionEnum.VIMP;
        break;
      case "4":
        channelAction = ChannelActionEnum.EMAIL_OPEN;
        break;
      case "6":
        channelAction = ChannelActionEnum.SERVE;
        break;
      default:
        logError(Errors.ERROR_INVALID_MKEVT);
    }

    // parse channel from query mkcid
    // no mkcid, accepted
    if (!parameters.containsKey(Constants.MKCID) || parameters.get(Constants.MKCID).get(0) == null) {
      LOGGER.warn(Errors.ERROR_NO_MKCID);
      metrics.meter("NoMkcidParameter");
      return true;
    }

    // invalid mkcid, show error and accept
    channelType = ChannelIdEnum.parse(parameters.get(Constants.MKCID).get(0));
    if (channelType == null) {
      LOGGER.warn(Errors.ERROR_INVALID_MKCID + " {}", uri);
      metrics.meter("InvalidMkcid");
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

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
        Field.of(PARTNER, partner), Field.of(PLATFORM, platform));


    // construct the common event before parsing to different events (ubi, utp, filter, message tracker)
    BaseEvent baseEvent = new BaseEvent();
    baseEvent.setTimestamp(startTime);
    baseEvent.setUrl(uri);
    baseEvent.setReferer(referer);
    baseEvent.setActionType(channelAction);
    baseEvent.setChannelType(channelType);
    baseEvent.setUriComponents(uriComponents);
    baseEvent.setUrlParameters(parameters);
    baseEvent.setRequestHeaders(requestHeaders);
    baseEvent.setUserAgentInfo(agentInfo);
    baseEvent.setUserPrefsCtx(userPrefsCtx);
    baseEvent.setEndUserContext(endUserContext);


    // add channel specific tags, and produce message for EPN and IMK
    ListenerMessage listenerMessage = null;
    if (channelType == ChannelIdEnum.SITE_EMAIL) {
      fireCmEvent(baseEvent, requestContext, siteEmailCollector);
    }
    else if (channelType == ChannelIdEnum.MRKT_EMAIL) {
      fireCmEvent(baseEvent, requestContext, mrktEmailCollector);
    }
    else {
      listenerMessage = firePMEvent(baseEvent, requestContext);
    }

    // send to unified tracking topic
    if(listenerMessage!=null) {
      submitChocolateUtpEvent(baseEvent, requestContext, null, listenerMessage.getSnapshotId(),
          listenerMessage.getShortSnapshotId(), null, startTime);
    } else {
      submitChocolateUtpEvent(baseEvent, requestContext, null, 0L,
          0L, null, startTime);
    }

    stopTimerAndLogData(baseEvent, Field.of(CHANNEL_ACTION, action),
        Field.of(CHANNEL_TYPE, type), Field.of(PARTNER, partner), Field.of(PLATFORM, platform));

    return true;
  }

  /**
   *
   * @param baseEvent           base event
   * @return                    roi listener message
   */
  private ListenerMessage fireROIEvent(BaseEvent baseEvent,
                                       ContainerRequestContext containerRequestContext) {

    // Parse the response
    ListenerMessage message = listenerMessageParser.parse(baseEvent);

    // 1. send to listener topic
    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    String kafkaTopic
        = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(baseEvent.getChannelType().getLogicalChannel()
        .getAvro());

    producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);

    // 2. track uibi
    if(!baseEvent.isCheckoutApi()) {
      roiCollector.trackUbi(containerRequestContext, baseEvent);
    }
    return message;
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
      metrics.meter(Errors.ERROR_NO_QUERY_PARAMETER);
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
      requestTracker.addTag(TrackerTagValueUtil.EventActionTag, Constants.EVENT_ACTION, String.class);

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
      metrics.meter("ErrorWriteAdguidToUBI");
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
        Field.of(CHANNEL_TYPE, event.getChannelType()));

    UnifiedTrackingMessage message = utpParser.parse(event);

    if (message != null) {
      unifiedTrackingProducer.send(new ProducerRecord<>(unifiedTrackingTopic, message.getEventId().getBytes(), message),
          UnifiedTrackingKafkaSink.callback);

      stopTimerAndLogData(startTime, Field.of(CHANNEL_ACTION, event.getActionType()),
          Field.of(CHANNEL_TYPE, event.getChannelType()));
      }
  }

  /**
   * Submit chocolate tracked user behavior into utp event
   * @param baseEvent       base event
   * @param requestContext  request context
   * @param roiEvent        roi event
   * @param snapshotId      snapshot id
   * @param shortSnapshotId short snapshot id
   * @param eventId         utp event id
   * @param startTime       start time
   */
  private void submitChocolateUtpEvent(BaseEvent baseEvent, ContainerRequestContext requestContext,
                                       ROIEvent roiEvent, long snapshotId,
                                       long shortSnapshotId, String eventId, long startTime) {
    try {
      Matcher m = ebaysites.matcher(baseEvent.getReferer().toLowerCase());
      if (ChannelActionEnum.EMAIL_OPEN.equals(baseEvent.getActionType())
          || ChannelActionEnum.ROI.equals(baseEvent.getActionType())
          || CollectionServiceUtil.inRefererWhitelist(baseEvent.getChannelType().getLogicalChannel().getAvro(),
              baseEvent.getReferer())
          || !m.find()) {
        UnifiedTrackingMessage utpMessage = utpParser.parse(baseEvent, requestContext, roiEvent, snapshotId,
            shortSnapshotId, startTime);
        if(!StringUtils.isEmpty(eventId)) {
          utpMessage.setEventId(eventId);
        }
        unifiedTrackingProducer.send(new ProducerRecord<>(unifiedTrackingTopic, utpMessage.getEventId().getBytes(),
            utpMessage), UnifiedTrackingKafkaSink.callback);
      } else {
        metrics.meter("UTPInternalDomainRef", 1, Field.of(CHANNEL_ACTION, baseEvent.getActionType().toString()),
            Field.of(CHANNEL_TYPE, baseEvent.getChannelType().toString()));
      }
    } catch (Exception e) {
      LOGGER.warn("UTP message process error.", e);
      metrics.meter("UTPMessageError");
    }
  }

  /**
   * Process AMS and IMK events
   * @param requestContext      wrapped request context
   * @param targetUrl           landing page url
   * @param referer             referer of the request
   * @param parameters          url parameters
   * @param channelType         channel type
   * @param channelAction       action type
   * @param request             http request
   * @param startTime           start timestamp of the request
   * @param endUserContext      enduserctx header
   * @param raptorSecureContext wrapped raptor secure context
   * @param agentInfo           user agent
   * @return                    a listener message
   */
  /**
   * Fire PM events to the streams
   * @param baseEvent base event
   * @param requestContext request context
   * @return listener message
   */
  private ListenerMessage firePMEvent(BaseEvent baseEvent, ContainerRequestContext requestContext) {

    ListenerMessage listenerMessage;

    listenerMessage = performanceMarketingCollector.decorateListenerMessageAndHandleGDPR(baseEvent);

    // 1. send to chocolate topic
    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    String kafkaTopic = ApplicationOptions.getInstance()
        .getSinkKafkaConfigs().get(baseEvent.getChannelType().getLogicalChannel().getAvro());

    producer.send(
        new ProducerRecord<>(kafkaTopic, listenerMessage.getSnapshotId(), listenerMessage),
        KafkaSink.callback);

    // 2. track ubi
    if (!baseEvent.getActionType().equals(ChannelActionEnum.SERVE)) {
      performanceMarketingCollector.trackUbi(requestContext, baseEvent, listenerMessage);
    }

    return listenerMessage;
  }


  private boolean fireCmEvent(BaseEvent baseEvent, ContainerRequestContext requestContext,
                               CustomerMarketingCollector cmCollector) {

    // 1. track ubi
    if (ChannelActionEnum.CLICK.equals(baseEvent.getActionType())) {
      cmCollector.trackUbi(requestContext, baseEvent);
    }

    // 2. send email open/click to behavior topic
    BehaviorMessage message = behaviorMessageParser.parse(baseEvent, requestContext);

    if (message != null) {
      // If the click is a duplicate click from itm page, then drop into duplicateItmClickTopic
      // else drop into normal topic
      behaviorProducer.send(new ProducerRecord<>(behaviorTopic, message.getSnapshotId().getBytes(), message),
          KafkaSink.callback);

      return true;
    } else
      return false;
  }

  /**
   * log error, log metric and throw error with error key
   *
   * @param  error error type
   * @throws Exception exception with error key
   */
  private void logError(String error) throws Exception {
    LOGGER.warn(error);
    metrics.meter(error);
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
    metrics.meter("CollectionServiceIncoming", 1, startTime, additionalFields);
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
    metrics.meter("CollectionServiceSuccess", 1, eventProcessStartTime, additionalFields);
    metrics.mean("CollectionServiceAverageLatency", endTime - eventProcessStartTime);
  }

  private void stopTimerAndLogData(BaseEvent baseEvent, Field<String, Object>... additionalFields) {
    long endTime = System.currentTimeMillis();
    LOGGER.debug(String.format("EndTime: %d", endTime));
    metrics.meter("CollectionServiceSuccess", 1, baseEvent.getTimestamp(), additionalFields);
    if (baseEvent.isCheckoutApi()) {
      metrics.mean("CollectionServiceCheckoutAPIClickAndROIAverageLatency", endTime - baseEvent.getTimestamp());
    } else {
      metrics.mean("CollectionServiceAverageLatency", endTime - baseEvent.getTimestamp());
    }
  }

  /**
   * If the click is a duplicate click from itm page, then drop into duplicateItmClickTopic
   * else drop into normal topic
   */
  private void sendClickToDuplicateItmClickTopic(Producer<Long, ListenerMessage> producer, ListenerMessage message) {
    producer.send(new ProducerRecord<>(duplicateItmClickTopic, message.getSnapshotId(), message), KafkaSink.callback);
    metrics.meter("DuplicateItmClick", 1, Field.of(CHANNEL_ACTION, message.getChannelAction().toString()),
            Field.of(CHANNEL_TYPE, message.getChannelType().toString()));
  }

  /**
   * Only for test
   */
  public Producer getBehaviorProducer() {
    return behaviorProducer;
  }
}
