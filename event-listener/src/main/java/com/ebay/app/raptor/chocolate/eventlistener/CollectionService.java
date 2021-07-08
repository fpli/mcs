package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.constant.GdprConsentConstant;
import com.ebay.raptor.opentracing.SpanEventHelper;
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
import com.ebay.app.raptor.chocolate.gen.model.EventPayload;
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
import static com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil.isLongNumeric;
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
  private ListenerMessageParser parser;
  private BehaviorMessageParser behaviorMessageParser;
  private Producer behaviorProducer;
  private String behaviorTopic;
  private Producer unifiedTrackingProducer;
  private String unifiedTrackingTopic;
  // collect duplicate click
  private String duplicateItmClickTopic;
  private static CollectionService instance = null;
  private EventEmitterPublisher eventEmitterPublisher;
  private UnifiedTrackingMessageParser utpParser;
  private static final String TYPE_INFO = "Info";
  private static final String STATUS_OK = "0";

  @Autowired
  private HttpRoverClient roverClient;

  @Autowired
  private HttpClientConnectionManager httpClientConnectionManager;

  @Inject
  private ISecureTokenManager tokenGenerator;

  @Inject
  private UserLookup userLookup;

  @Autowired
  private GdprConsentHandler gdprConsentHandler;

  @Autowired
  private PerformanceMarketingCollector performanceMarketingCollector;

  @Autowired
  private MrktEmailCollector mrktEmailCollector;

  @Autowired
  private SiteEmailCollector siteEmailCollector;

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
  private static final String CHECKOUT_API_USER_AGENT = "checkoutApi";
  private static final String ROVER_INTERNAL_VIP = "internal.rover.vip.ebay.com";
  private static final List<String> REFERER_WHITELIST = Arrays.asList("https://ebay.mtag.io", "https://ebay.pissedconsumer.com", "https://secureir.ebaystatic.com");
  private static final String VOD_PAGE = "vod";
  private static final String VOD_SUB_PAGE = "FetchOrderDetails";
  private static final String ROI_TRANS_TYPE = "roiTransType";

  @PostConstruct
  public void postInit() throws Exception {
    this.metrics = ESMetrics.getInstance();
    this.parser = ListenerMessageParser.getInstance();
    this.behaviorMessageParser = BehaviorMessageParser.getInstance();
    this.behaviorProducer = BehaviorKafkaSink.get();
    this.behaviorTopic = ApplicationOptions.getInstance().getProduceBehaviorTopic();
    this.unifiedTrackingProducer = UnifiedTrackingKafkaSink.get();
    this.unifiedTrackingTopic = ApplicationOptions.getInstance().getUnifiedTrackingTopic();
    this.eventEmitterPublisher = new EventEmitterPublisher(tokenGenerator);
    this.duplicateItmClickTopic = ApplicationOptions.getInstance().getDuplicateItmClickTopic();
    this.utpParser = new UnifiedTrackingMessageParser();
  }

  /**
   * Collect event and publish to kafka
   * @param request             raw request
   * @param endUserContext      wrapped end user context
   * @param raptorSecureContext wrapped secure header context
   * @param requestContext      wrapped raptor request context
   * @param event               post body event
   * @return OK or Error message
   * @throws Exception when there is an unhandled error
   */
  public boolean collect(HttpServletRequest request, IEndUserContext endUserContext, RaptorSecureContext
          raptorSecureContext, ContainerRequestContext requestContext, Event event) throws Exception {

    Map<String, String> requestHeaders = commonRequestHandler.getHeaderMaps(request);

    if (requestHeaders.get(TRACKING_HEADER) == null) {
      logError(Errors.ERROR_NO_TRACKING);
    }

    if (requestHeaders.get(ENDUSERCTX_HEADER) == null) {
      logError(Errors.ERROR_NO_ENDUSERCTX);
    }

    String referer = commonRequestHandler.getReferer(event, requestHeaders, endUserContext);

    String userAgent = endUserContext.getUserAgent();
    if (null == userAgent) {
      logError(Errors.ERROR_NO_USER_AGENT);
    }

    // legacy rover deeplink case. Forward it to rover. We control this at our backend in case mobile app miss it
    Matcher roverSitesMatcher = roversites.matcher(referer.toLowerCase());
    if (roverSitesMatcher.find()) {
      roverClient.forwardRequestToRover(referer, ROVER_INTERNAL_VIP, request);
      return true;
    }

    ChannelIdEnum channelType;
    ChannelActionEnum channelAction = ChannelActionEnum.CLICK;

    // targetUrl is from post body
    String targetUrl = event.getTargetUrl();

    // For e page, the real target url is in the referer
    // Since Chrome strict policy, referer may be cut off, so use 'originalUrl' parameter first as target url
    // if referer is existed, it will be in the target url (request body) parameter
    if (ePageSites.matcher(targetUrl.toLowerCase()).find()) {
      MonitorUtil.info("ePageIncoming");

      Event staticPageEvent = staticPageRequestHandler.parseStaticPageEvent(targetUrl, referer);
      targetUrl = staticPageEvent.getTargetUrl();
      referer = staticPageEvent.getReferrer();
    }

    // Now we support to track two kind of deeplink cases
    // XC-1797, extract and decode actual target url from referrer parameter in targetUrl, only accept the url when the domain of referrer parameter belongs to ebay sites
    // XC-3349, for native uri with Chocolate parameters, re-construct Chocolate url based on native uri and track (only support /itm page)
    Matcher deeplinkMatcher = deeplinksites.matcher(targetUrl.toLowerCase());
    if (deeplinkMatcher.find()) {
      MonitorUtil.info("IncomingAppDeepLink");

      Event customizedSchemeEvent = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetUrl, referer);
      if(customizedSchemeEvent == null) {
        logError(Errors.ERROR_NO_VALID_TRACKING_PARAMS_DEEPLINK);
      } else {
        targetUrl = customizedSchemeEvent.getTargetUrl();
        referer = customizedSchemeEvent.getReferrer();
      }
    }

    // parse channel from uri
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();

    // XC-1695. no query parameter,
    // rejected but return 201 accepted for clients since app team has started unconditionally call
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
    if (parameters.size() == 0) {
      LOGGER.warn(Errors.ERROR_NO_QUERY_PARAMETER);
      MonitorUtil.info(Errors.ERROR_NO_QUERY_PARAMETER);
      return true;
    }

    // XC-1695. no mkevt, rejected but return 201 accepted for clients since app team has started unconditionally call
    if (!parameters.containsKey(Constants.MKEVT) || parameters.get(Constants.MKEVT).get(0) == null) {
      LOGGER.warn(Errors.ERROR_NO_MKEVT);
      MonitorUtil.info(Errors.ERROR_NO_MKEVT);

      return true;
    }

    // XC-1695. mkevt != 1, rejected but return 201 accepted for clients
    String mkevt = parameters.get(Constants.MKEVT).get(0);
    if (!mkevt.equals(Constants.VALID_MKEVT_CLICK)) {
      LOGGER.warn(Errors.ERROR_INVALID_MKEVT);
      MonitorUtil.info(Errors.ERROR_INVALID_MKEVT);

      return true;
    }

    // parse channel from query mkcid
    // no mkcid, rejected but return 201 accepted for clients
    if (!parameters.containsKey(Constants.MKCID) || parameters.get(Constants.MKCID).get(0) == null) {
      LOGGER.warn(Errors.ERROR_NO_MKCID);
      MonitorUtil.info("NoMkcidParameter");

      return true;
    }

    // invalid mkcid, show error and accept
    channelType = ChannelIdEnum.parse(parameters.get(Constants.MKCID).get(0));
    if (channelType == null) {
      LOGGER.warn(Errors.ERROR_INVALID_MKCID + " {}", targetUrl);
      MonitorUtil.info("InvalidMkcid");

      return true;
    }

    // for search engine free listings, append mkrid
    if (channelType == ChannelIdEnum.SEARCH_ENGINE_FREE_LISTINGS) {
      String rotationId = getSearchEngineFreeListingsRotationId(requestContext);
      targetUrl = targetUrl + "&" + Constants.MKRID + "=" + rotationId;
      parameters = UriComponentsBuilder.fromUriString(targetUrl).build().getQueryParams();
    }

    // check partner for email click
    String partner = null;
    if (ChannelIdEnum.SITE_EMAIL.equals(channelType) || ChannelIdEnum.MRKT_EMAIL.equals(channelType)) {
      // no mkpid, accepted
      if (!parameters.containsKey(Constants.MKPID) || parameters.get(Constants.MKPID).get(0) == null) {
        LOGGER.warn(Errors.ERROR_NO_MKPID);
        MonitorUtil.info("NoMkpidParameter");
      } else {
        // invalid mkpid, accepted
        partner = EmailPartnerIdEnum.parse(parameters.get(Constants.MKPID).get(0));
        if (StringUtils.isEmpty(partner)) {
          LOGGER.warn(Errors.ERROR_INVALID_MKPID);
          MonitorUtil.info("InvalidMkpid");
        }
      }
    }

    String landingPageType;
    List<String> pathSegments = uriComponents.getPathSegments();
    if (pathSegments.isEmpty()) {
      landingPageType = "home";
    } else {
      landingPageType = pathSegments.get(0);
    }

    // UFES metrics
    MonitorUtil.info("UFESTraffic", 1, Field.of("isUFES", isFromUFES(requestHeaders).toString()),
        Field.of(LANDING_PAGE_TYPE, landingPageType),
        Field.of("statusCode", request.getHeader(Constants.NODE_REDIRECTION_HEADER_NAME)));

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = getPlatform(agentInfo);

    String action = ChannelActionEnum.CLICK.toString();
    String type = channelType.getLogicalChannel().getAvro().toString();

    // Self-service events, send them to couchbase
    if (parameters.containsKey(Constants.SELF_SERVICE) && parameters.containsKey(Constants.SELF_SERVICE_ID)) {
      if ("1".equals(parameters.getFirst(Constants.SELF_SERVICE)) &&
          parameters.getFirst(Constants.SELF_SERVICE_ID) != null) {
        MonitorUtil.info("SelfServiceIncoming");

        CouchbaseClient.getInstance().addSelfServiceRecord(parameters.getFirst(Constants.SELF_SERVICE_ID), targetUrl);
        MonitorUtil.info("SelfServiceSuccess");

        return true;
      }
    }

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));

    // this attribute is used to log actual process time for incoming event in mcs
    long eventProcessStartTime = startTime;

    // update startTime if the click comes from checkoutAPI
    boolean checkoutAPIClickFlag = false;
    if (channelType == ChannelIdEnum.EPN) {
      EventPayload payload = event.getPayload();
      if (payload != null) {
        String checkoutAPIClickTs = payload.getCheckoutAPIClickTs();
        if (!StringUtils.isEmpty(checkoutAPIClickTs)) {
          try {
            long checkoutAPIClickTimestamp = Long.parseLong(checkoutAPIClickTs);
            if (checkoutAPIClickTimestamp > 0) {
              checkoutAPIClickFlag = true;
              startTime = checkoutAPIClickTimestamp;
            }
          } catch (Exception e) {
            LOGGER.warn(e.getMessage());
            LOGGER.warn("Error click timestamp from Checkout API " + checkoutAPIClickTs);
            MonitorUtil.info("ErrorCheckoutAPIClickTimestamp", 1);

          }
        }
      }
    }
    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);

    // Overwrite the referer for the clicks from Promoted Listings iframe on ebay partner sites XC-3256
    // only for EPN channel
    boolean isEPNClickFromPromotedListings;
    try {
      isEPNClickFromPromotedListings
          = CollectionServiceUtil.isEPNPromotedListingsClick(channelType, parameters, referer);

      if (isEPNClickFromPromotedListings) {
        referer = URLDecoder.decode(parameters.get(Constants.PLRFR).get(0), StandardCharsets.UTF_8.name());
        MonitorUtil.info("OverwriteRefererForPromotedListingsClick");

      }
    } catch (Exception e) {
      LOGGER.error("Determine whether the click is from promoted listings iframe error");
      MonitorUtil.info("DeterminePromotedListingsClickError", 1);
    }

    boolean isInternalRef = isInternalRef(channelType.getLogicalChannel().getAvro(), referer);
    // Determine whether the click is a duplicate click
    // If duplicate click, then drop into duplicateItmClickTopic
    // If not, drop into normal topic
    boolean isDuplicateClick = false;
    try {
      isDuplicateClick = CollectionServiceUtil.isDuplicateItmClick(
          request.getHeader(Constants.NODE_REDIRECTION_HEADER_NAME), endUserContext.getUserAgent(),
          targetUrl, agentInfo.requestIsFromBot(), agentInfo.isMobile(), agentInfo.requestIsMobileWeb());

      // send duplicate click to a dedicate listener topic
      if(isDuplicateClick || isInternalRef) {
        Producer<Long, ListenerMessage> producer = KafkaSink.get();
        ListenerMessage listenerMessage = parser.parse(requestHeaders, endUserContext, userPrefsCtx, startTime,
            0L, channelType.getLogicalChannel().getAvro(), ChannelActionEnum.CLICK, "", targetUrl,
            referer, 0L, "");
        Long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId()).getRepresentation();
        listenerMessage.setSnapshotId(snapshotId);
        listenerMessage.setShortSnapshotId(0L);
        sendClickToDuplicateItmClickTopic(producer, listenerMessage);
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      LOGGER.error("Determine whether the click is duplicate item click error.");
      MonitorUtil.info("DetermineDuplicateItmClickError", 1);

    }

    // until now, generate eventId in advance of utp tracking so that it can be emitted into both ubi&utp only for click
    String utpEventId = UUID.randomUUID().toString();

    Boolean vodInternal = isVodInternal(channelType, pathSegments);
    if(!isDuplicateClick && !isInternalRef || vodInternal) {
      ListenerMessage listenerMessage = null;
      // add channel specific tags, and produce message for EPN and IMK
      if (PM_CHANNELS.contains(channelType)) {
        listenerMessage = processPMEvent(requestContext, requestHeaders, userPrefsCtx, targetUrl, referer, utpEventId,
            parameters, channelType, channelAction, request, startTime, endUserContext, raptorSecureContext, agentInfo);
      }
      else if (channelType == ChannelIdEnum.SITE_EMAIL) {
        processCmEvent(requestContext, endUserContext, referer, parameters, type, action, request, agentInfo,
            targetUrl, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), utpEventId,
            siteEmailCollector);
      }
      else if (channelType == ChannelIdEnum.MRKT_EMAIL) {
        processCmEvent(requestContext, endUserContext, referer, parameters, type, action, request, agentInfo,
            targetUrl, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), utpEventId,
            mrktEmailCollector);
      }
      else if (channelType == ChannelIdEnum.MRKT_SMS || channelType == ChannelIdEnum.SITE_SMS) {
        processSMSEvent(requestContext, referer, targetUrl, agentInfo, parameters, type, action);
      }

      // send to unified tracking topic
      if (listenerMessage != null) {
        processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, requestHeaders,
            agentInfo, parameters, targetUrl, referer, channelType.getLogicalChannel().getAvro(),
            channelAction.getAvro(), null, listenerMessage.getSnapshotId(),
            listenerMessage.getShortSnapshotId(), utpEventId, startTime, vodInternal);
      } else {
        processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, requestHeaders,
            agentInfo, parameters, targetUrl, referer, channelType.getLogicalChannel().getAvro(),
            channelAction.getAvro(), null, 0L, 0L, utpEventId, startTime, vodInternal);
      }
    }

    stopTimerAndLogData(eventProcessStartTime, startTime, checkoutAPIClickFlag, Field.of(CHANNEL_ACTION, action),
        Field.of(CHANNEL_TYPE, type), Field.of(PARTNER, partner), Field.of(PLATFORM, platform),
        Field.of(LANDING_PAGE_TYPE, landingPageType));

    return true;
  }

  protected boolean isInternalRef(ChannelType channelType, String referer) {
    if (inRefererWhitelist(channelType, referer)) {
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

    if (request.getHeader(TRACKING_HEADER) == null) {
      logError(Errors.ERROR_NO_TRACKING);
    }

    if (request.getHeader(ENDUSERCTX_HEADER) == null) {
      logError(Errors.ERROR_NO_ENDUSERCTX);
    }

    String localTimestamp = Long.toString(System.currentTimeMillis());

    String userId = commonRequestHandler.getUserId(raptorSecureContext, endUserContext);

    try {
      long itemId = Long.parseLong(roiEvent.getItemId());
      if (itemId < 0) {
        roiEvent.setItemId("");
      }
    } catch (Exception e) {
      LOGGER.warn("Error itemId " + roiEvent.getItemId());
      MonitorUtil.info("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));

      roiEvent.setItemId("");
    }
    // Parse timestamp if it null or invalid, change it to localTimestamp
    long transTimestamp = 0;
    try {
      transTimestamp = Long.parseLong(roiEvent.getTransactionTimestamp());
      if(transTimestamp <= 0) {
        roiEvent.setTransactionTimestamp(localTimestamp);
        transTimestamp = Long.parseLong(localTimestamp);
      }
    } catch (Exception e) {
      LOGGER.warn("Error timestamp " + roiEvent.getTransactionTimestamp());
      MonitorUtil.info("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));

      roiEvent.setTransactionTimestamp(localTimestamp);
      transTimestamp = Long.parseLong(localTimestamp);
    }
    // Parse payload fields
    Map<String, String> payloadMap = roiEvent.getPayload();
    if(payloadMap == null) {
      payloadMap = new HashMap<>();
    }

    // Parse transId
    try {
      String transId = roiEvent.getUniqueTransactionId();
      if (Long.parseLong(transId) < 0) {
        roiEvent.setUniqueTransactionId("");
      }
    } catch (Exception e) {
      LOGGER.warn("Error transactionId " + roiEvent.getUniqueTransactionId());
      MonitorUtil.info("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));

      roiEvent.setUniqueTransactionId("");
    }

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = getPlatform(agentInfo);

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, ChannelActionEnum.ROI.toString()),
        Field.of(CHANNEL_TYPE, ChannelType.ROI.toString()));

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
    Boolean isRoiFromCheckoutAPI = isROIFromCheckoutAPI(payloadMap, endUserContext);
    if (!isRoiFromCheckoutAPI) {
      addRoiSojTags(requestContext, roiEvent, userId);
    } else {
      MonitorUtil.info("CheckoutAPIROI", 1);

    }

    // Write roi event to kafka output topic
    boolean processFlag = processROIEvent(requestContext, targetUrl, referer, parameters, ChannelIdEnum.ROI,
        ChannelActionEnum.ROI, request, transTimestamp, endUserContext, raptorSecureContext, agentInfo, roiEvent);

    // Mock click for the ROI which has valid mppid in the payload (ROI generated from pre-install app on Android) XC-3464
    boolean processPreInstallROIEventFlag = processPreInstallROIEvent(requestContext, referer, request, endUserContext,
            raptorSecureContext, agentInfo, payloadMap, roiEvent, transTimestamp);

    if (processFlag && processPreInstallROIEventFlag) {
      MonitorUtil.info("NewROICountAPI", 1, Field.of(CHANNEL_ACTION, "New-ROI"),
          Field.of(CHANNEL_TYPE, "New-ROI"), Field.of(ROI_SOURCE, String.valueOf(payloadMap.get(ROI_SOURCE))));

      // Log the roi lag between transation time and receive time
      MonitorUtil.latency("RoiTransationLag", startTime - transTimestamp, Field.of(CHANNEL_ACTION, "ROI"),
          Field.of(CHANNEL_TYPE, "ROI"));
      stopTimerAndLogData(startTime, startTime, false,
          Field.of(CHANNEL_ACTION, ChannelActionEnum.ROI.toString()), Field.of(CHANNEL_TYPE,
          ChannelType.ROI.toString()), Field.of(PLATFORM, platform));
    }
    return true;
  }

  /**
   * Collect impression event and send pixel response
   * @param request             raw request
   * @param endUserContext      end user context header
   * @param raptorSecureContext wrapped raptor secure context
   * @param requestContext      wrapped request context
   * @param event               impression event body
   * @return                    success or failure
   * @throws Exception          when unhandled exception
   */
  public boolean collectImpression(HttpServletRequest request, IEndUserContext endUserContext, RaptorSecureContext
      raptorSecureContext, ContainerRequestContext requestContext, Event event) throws Exception {

    Map<String, String> requestHeaders = commonRequestHandler.getHeaderMaps(request);

    if (request.getHeader(TRACKING_HEADER) == null) {
      logError(Errors.ERROR_NO_TRACKING);
    }

    String referer = commonRequestHandler.getReferer(event, requestHeaders, endUserContext);

    String userAgent = request.getHeader("User-Agent");
    if (null == userAgent) {
      LOGGER.warn(Errors.ERROR_NO_USER_AGENT);
      MonitorUtil.info(Errors.ERROR_NO_USER_AGENT);

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
    if (!parameters.containsKey(Constants.MKEVT) || parameters.get(Constants.MKEVT).get(0) == null) {
      LOGGER.warn(Errors.ERROR_NO_MKEVT);
      MonitorUtil.info(Errors.ERROR_NO_MKEVT);

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
      MonitorUtil.info("NoMkcidParameter");
      return true;
    }

    // invalid mkcid, show error and accept
    channelType = ChannelIdEnum.parse(parameters.get(Constants.MKCID).get(0));
    if (channelType == null) {
      LOGGER.warn(Errors.ERROR_INVALID_MKCID + " {}", uri);
      MonitorUtil.info("InvalidMkcid");
      return true;
    }

    // check partner for email open
    String partner = null;
    if (channelAction == ChannelActionEnum.EMAIL_OPEN) {
      // no mkpid, accepted
      if (!parameters.containsKey(Constants.MKPID) || parameters.get(Constants.MKPID).get(0) == null) {
        LOGGER.warn(Errors.ERROR_NO_MKPID);
        MonitorUtil.info("NoMkpidParameter");

      } else {
        // invalid mkpid, accepted
        partner = EmailPartnerIdEnum.parse(parameters.get(Constants.MKPID).get(0));
        if (StringUtils.isEmpty(partner)) {
          LOGGER.warn(Errors.ERROR_INVALID_MKPID);
          MonitorUtil.info("InvalidMkpid");

        }
      }
    }

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = getPlatform(agentInfo);

    String action = channelAction.getAvro().toString();
    String type = channelType.getLogicalChannel().getAvro().toString();

    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));

    // add channel specific tags, and produce message for EPN and IMK
    ListenerMessage listenerMessage = null;
    if (channelType == ChannelIdEnum.SITE_EMAIL) {
      processCmEvent(requestContext, endUserContext, referer, parameters, type, action, request,
          agentInfo, uri, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), null,
          siteEmailCollector);
    }
    else if (channelType == ChannelIdEnum.MRKT_EMAIL) {
      processCmEvent(requestContext, endUserContext, referer, parameters, type, action, request,
          agentInfo, uri, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), null,
          mrktEmailCollector);
    }
    else {
      listenerMessage = processPMEvent(requestContext, requestHeaders, userPrefsCtx, uri, referer, null, parameters,
          channelType, channelAction, request, startTime, endUserContext, raptorSecureContext, agentInfo);
    }

    // send to unified tracking topic
    if(listenerMessage!=null) {
      processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, requestHeaders,
          agentInfo, parameters, uri, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
          null, listenerMessage.getSnapshotId(), listenerMessage.getShortSnapshotId(), null, startTime,
          false);
    } else {
      processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, requestHeaders,
          agentInfo, parameters, uri, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
          null, 0L, 0L, null, startTime, false);
    }

    stopTimerAndLogData(startTime, startTime, false, Field.of(CHANNEL_ACTION, action),
        Field.of(CHANNEL_TYPE, type), Field.of(PARTNER, partner), Field.of(PLATFORM, platform));

    return true;
  }

  /**
   *
   * @param requestContext      wrapped raptor request
   * @param targetUrl           targe url
   * @param referer             referer of the request
   * @param parameters          url parameters
   * @param channelType         channel type
   * @param channelAction       action type
   * @param request             http request
   * @param startTime           start timestamp
   * @param endUserContext      end user context header
   * @param raptorSecureContext wrapped raptor secure context
   * @param agentInfo           user agent
   * @param roiEvent            input roi event body
   * @return                    success or failure
   */
  private boolean processROIEvent(ContainerRequestContext requestContext, String targetUrl, String referer,
                                  MultiValueMap<String, String> parameters, ChannelIdEnum channelType,
                                  ChannelActionEnum channelAction, HttpServletRequest request, long startTime,
                                  IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext,
                                  UserAgentInfo agentInfo, ROIEvent roiEvent) {

    Map<String, String> requestHeaders = commonRequestHandler.getHeaderMaps(request);

    String userId = commonRequestHandler.getUserId(raptorSecureContext, endUserContext);

    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);

    // Parse the response
    ListenerMessage message = parser.parse(requestHeaders, endUserContext, userPrefsCtx, startTime,
        -1L, channelType.getLogicalChannel().getAvro(), channelAction, userId, targetUrl,
        referer, 0L, "");

    BehaviorMessage behaviorMessage = behaviorMessageParser.parseAmsAndImkEvent(request, requestContext, endUserContext,
        parameters, agentInfo, targetUrl, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
        message.getShortSnapshotId(), PageIdEnum.ROI.getId(),
        PageNameEnum.ROI.getName(), 0, referer, message.getGuid(), message.getCguid(), userId,
        String.valueOf(message.getDstRotationId()));
    if (behaviorMessage != null) {
      behaviorProducer.send(new ProducerRecord<>(behaviorTopic, behaviorMessage.getSnapshotId().getBytes(),
              behaviorMessage), KafkaSink.callback);
    }

    processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, requestHeaders, agentInfo,
        parameters, targetUrl, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
        roiEvent, message.getSnapshotId(), message.getShortSnapshotId(), null, startTime, false);

    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    String kafkaTopic
        = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());

    producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);
    return true;
  }

  /**
   * Collect sync event and publish to ubi only
   *
   * @param request             raw request
   * @param raptorSecureContext wrapped secure header context
   * @param event               post body event
   * @return OK or Error message
   */
  public boolean collectSync(HttpServletRequest request, RaptorSecureContext
      raptorSecureContext, ContainerRequestContext requestContext, Event event) throws Exception {

    if (request.getHeader(TRACKING_HEADER) == null) {
      logError(Errors.ERROR_NO_TRACKING);
    }

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
        Field.of(CHANNEL_TYPE, event.getChannelType()));

    UnifiedTrackingMessage message = utpParser.parse(event, userLookup);
    SpanEventHelper.writeEvent(TYPE_INFO, "eventId", STATUS_OK, message.getEventId());
    SpanEventHelper.writeEvent(TYPE_INFO, "producerEventId", STATUS_OK, message.getProducerEventId());
    SpanEventHelper.writeEvent(TYPE_INFO, "service", STATUS_OK, message.getService());
    SpanEventHelper.writeEvent(TYPE_INFO, "server", STATUS_OK, message.getServer());

    if (message != null) {
      unifiedTrackingProducer.send(new ProducerRecord<>(unifiedTrackingTopic, message.getEventId().getBytes(), message),
          UnifiedTrackingKafkaSink.callback);

      stopTimerAndLogData(startTime, startTime, false, Field.of(CHANNEL_ACTION, event.getActionType()),
          Field.of(CHANNEL_TYPE, event.getChannelType()));
      }
  }

  /**
   * Process unified tracking user behavior events
   * @param requestContext      wrapped request context
   * @param request             http request
   * @param endUserContext      enduserctx header
   * @param raptorSecureContext wrapped raptor secure context
   * @param agentInfo           user agent
   * @param parameters          url parameters
   * @param url                 url
   * @param referer             referer
   * @param channelType         channel type
   * @param channelAction       action type
   * @param roiEvent            roi event
   * @param snapshotId          snapshot id
   * @param shortSnapshotId     short snapshot id
   * @param eventId             utp event id
   * @param startTime           start time of the request
   */
  @SuppressWarnings("unchecked")
  private void processUnifiedTrackingEvent(ContainerRequestContext requestContext, HttpServletRequest request,
                                           IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext,
                                           Map<String, String> requestHeaders,
                                           UserAgentInfo agentInfo, MultiValueMap<String, String> parameters,
                                           String url, String referer, ChannelType channelType,
                                           ChannelAction channelAction, ROIEvent roiEvent, long snapshotId,
                                           long shortSnapshotId, String eventId, long startTime, Boolean vodInternal) {
    try {
      Matcher m = ebaysites.matcher(referer.toLowerCase());
      if (ChannelAction.EMAIL_OPEN.equals(channelAction) || ChannelAction.ROI.equals(channelAction)
          || inRefererWhitelist(channelType, referer) || !m.find() || vodInternal) {
        UnifiedTrackingMessage utpMessage = utpParser.parse(requestContext, request, endUserContext,
                raptorSecureContext, requestHeaders, agentInfo, parameters, url, referer, channelType, channelAction,
                roiEvent, snapshotId, shortSnapshotId, startTime);
        if(!StringUtils.isEmpty(eventId)) {
          utpMessage.setEventId(eventId);
        }
        unifiedTrackingProducer.send(new ProducerRecord<>(unifiedTrackingTopic, utpMessage.getEventId().getBytes(),
            utpMessage), UnifiedTrackingKafkaSink.callback);
      } else {
        MonitorUtil.info("UTPInternalDomainRef", 1, Field.of(CHANNEL_ACTION, channelAction.toString()),
                Field.of(CHANNEL_TYPE, channelType.toString()));

      }
    } catch (Exception e) {
      LOGGER.warn("UTP message process error.", e);
      MonitorUtil.info("UTPMessageError");

    }
  }

  /**
   * The ebaysites pattern will treat ebay.abcd.com and ebaystatic as ebay site. So add a whitelist to handle these bad cases.
   * @param channelType channel type
   * @param referer referer
   * @return in whitelist or not
   */
  protected boolean inRefererWhitelist(ChannelType channelType, String referer) {
    // currently, this case only exists in display channel
    if (ChannelType.DISPLAY != channelType) {
      return false;
    }
    String lowerCase = referer.toLowerCase();
    for (String referWhitelist : REFERER_WHITELIST) {
      if (lowerCase.startsWith(referWhitelist)) {
        return true;
      }
    }
    return false;
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
  private ListenerMessage  processPMEvent(ContainerRequestContext requestContext, Map<String, String> requestHeaders,
                                         UserPrefsCtx userPrefsCtx, String targetUrl, String referer, String utpEventId,
                                         MultiValueMap<String, String> parameters, ChannelIdEnum channelType,
                                         ChannelActionEnum channelAction, HttpServletRequest request, long startTime,
                                         IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext,
                                         UserAgentInfo agentInfo) {

    ListenerMessage listenerMessage;

    listenerMessage = performanceMarketingCollector.parseListenerMessage(requestHeaders, userPrefsCtx,
        targetUrl, referer, parameters, channelType, channelAction, request, startTime, endUserContext,
        raptorSecureContext);

    // 1. send to chocolate topic
    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    String kafkaTopic = ApplicationOptions.getInstance()
        .getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());

    producer.send(
        new ProducerRecord<>(kafkaTopic, listenerMessage.getSnapshotId(), listenerMessage),
        KafkaSink.callback);

    // 2. track ubi
    if (!channelAction.equals(ChannelActionEnum.SERVE)) {
      performanceMarketingCollector.trackUbi(requestContext, targetUrl, referer, utpEventId, parameters, channelType,
          channelAction, startTime, endUserContext, listenerMessage);
    }

    // 3. send to behavior topic
    if (channelType != ChannelIdEnum.EPN) {
      BehaviorMessage behaviorMessage = performanceMarketingCollector.parseBehaviorMessage(requestContext, targetUrl,
          referer, parameters, channelType, channelAction, request, startTime, endUserContext, agentInfo,
          listenerMessage);
      if (behaviorMessage != null) {
        behaviorProducer.send(
            new ProducerRecord<>(behaviorTopic, behaviorMessage.getSnapshotId().getBytes(), behaviorMessage),
            KafkaSink.callback);
      }
    }

    return listenerMessage;
  }

  private boolean processCmEvent(ContainerRequestContext requestContext, IEndUserContext endUserContext,
                                 String referer, MultiValueMap<String, String> parameters, String type,
                                 String action, HttpServletRequest request, UserAgentInfo agentInfo, String uri,
                                 Long startTime, ChannelType channelType, ChannelAction channelAction,
                                 String utpEventId, CustomerMarketingCollector cmCollector) {

    long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId()).getRepresentation();

    // 0. temporary, send click and open event to message tracker
    eventEmitterPublisher.publishEvent(requestContext, parameters, uri, channelType, channelAction, snapshotId);

    // 1. track ubi
    if (ChannelAction.CLICK.equals(channelAction)) {
      cmCollector.trackUbi(requestContext, parameters, type, action, request, uri, referer, utpEventId,
          channelAction);
    }

    // 2. send email open/click to behavior topic
    BehaviorMessage message = cmCollector.parseBehaviorMessage(requestContext, endUserContext, referer,
        parameters, request, agentInfo, uri, startTime, channelType, channelAction, snapshotId);

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
   * Process SMS event
   * @param requestContext  request context
   * @param referer         referer of the request
   * @param parameters      url parameters
   * @param type            channel type
   * @param action          action type
   * @return                success or failure
   */
  private boolean processSMSEvent(ContainerRequestContext requestContext, String referer, String targetUrl,
                                  UserAgentInfo agentInfo, MultiValueMap<String, String> parameters, String type,
                                  String action) {

    // Tracking ubi only when refer domain is not ebay.
    // Don't track ubi if the click is a duplicate itm click
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find()) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker
            = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // page id
        requestTracker.addTag(TrackerTagValueUtil.PageIdTag, PageIdEnum.CLICK.getId(), Integer.class);

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

        // populate device info
        CollectionServiceUtil.populateDeviceDetectionParams(agentInfo, requestTracker);

        // event family
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, Constants.EVENT_FAMILY_CRM, String.class);

        // sms unique id
        addTagFromUrlQuery(parameters, requestTracker, Constants.SMS_ID, "smsid", String.class);

      } catch (Exception e) {
        LOGGER.warn("Error when tracking ubi for sms click tags", e);
        MonitorUtil.info("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));

      }
    } else {
      MonitorUtil.info("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));

    }

    return true;
  }

  /**
   * Process Pre-install ROI Event
   * Mock click for the ROI which has valid mppid in the payload (ROI generated from pre-install app on Android) XC-3464
   * @param requestContext      wrapped request context
   * @param referer             referer of the request
   * @param request             http request
   * @param endUserContext      enduserctx header
   * @param raptorSecureContext wrapped raptor secure context
   * @param agentInfo           user agent
   * @param payloadMap          ROI payload parameters
   * @param roiEvent            ROI event
   * @param transTimestamp      ROI transaction timestamp
   * @return                success or failure
   */
  private boolean processPreInstallROIEvent(ContainerRequestContext requestContext, String referer, HttpServletRequest request,
                                            IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext,
                                            UserAgentInfo agentInfo, Map<String, String> payloadMap, ROIEvent roiEvent, long transTimestamp) {

    try {
      boolean isPreInstallROI = CollectionServiceUtil.isPreinstallROI(payloadMap, roiEvent.getTransType());

      if (isPreInstallROI) {
        UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);
        Map<String, String> requestHeaders = commonRequestHandler.getHeaderMaps(request);

        // until now, generate eventId in advance of utp tracking so that it can be emitted into both ubi&utp only for click
        String utpEventId = UUID.randomUUID().toString();

        String clickUrl = CollectionServiceUtil.createPrmClickUrl(payloadMap, endUserContext);

        if (!StringUtils.isEmpty(clickUrl)) {
          UriComponents clickUriComponents = UriComponentsBuilder.fromUriString(clickUrl).build();

          MultiValueMap<String, String> clickParameters = clickUriComponents.getQueryParams();

          ListenerMessage listenerMessage = performanceMarketingCollector.parseListenerMessage(requestHeaders, userPrefsCtx,
                  clickUrl, referer, clickParameters, ChannelIdEnum.DAP, ChannelActionEnum.CLICK, request, transTimestamp, endUserContext,
                  raptorSecureContext);

          Producer<Long, ListenerMessage> producer = KafkaSink.get();
          String kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(ChannelIdEnum.DAP.getLogicalChannel().getAvro());

          producer.send(
                  new ProducerRecord<>(kafkaTopic, listenerMessage.getSnapshotId(), listenerMessage),
                  KafkaSink.callback);

          // send to unified tracking topic
          processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, requestHeaders,
                  agentInfo, clickParameters, clickUrl, referer, ChannelType.DISPLAY,
                  ChannelAction.CLICK, null, listenerMessage.getSnapshotId(),
                  listenerMessage.getShortSnapshotId(), utpEventId, transTimestamp, false);

          // Log mock click for pre-install ROI by transaction type
          MonitorUtil.info("PreInstallMockClick", 1, Field.of(CHANNEL_ACTION, ChannelActionEnum.CLICK.toString()),
                  Field.of(CHANNEL_TYPE, ChannelIdEnum.DAP.getLogicalChannel().getAvro().toString()),
                  Field.of(ROI_TRANS_TYPE, roiEvent.getTransType()));
        }
      }
    } catch (Exception ex) {
      LOGGER.error("Error when processing pre-install ROI events", ex);
      MonitorUtil.info("ErrorProcessPreinstallROIEvent", 1);
    }

    return true;
  }

  /**
   * Add roi sjo tags
   * @param requestContext  request context
   * @param roiEvent        roi event body
   * @param userId          user id
   */
  private void addRoiSojTags(ContainerRequestContext requestContext, ROIEvent roiEvent,
                             String userId) {
    try {
      // Ubi tracking
      IRequestScopeTracker requestTracker =
          (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

      // page id
      requestTracker.addTag(TrackerTagValueUtil.PageIdTag, PageIdEnum.ROI.getId(), Integer.class);

      // site ID is embedded in IRequestScopeTracker default commit tags

      // Item ID
      if(isLongNumeric(roiEvent.getItemId())) {
        requestTracker.addTag("itm", roiEvent.getItemId(), String.class);
      }

      // Transation Type
      if (!StringUtils.isEmpty(roiEvent.getTransType())) {
        requestTracker.addTag("tt", roiEvent.getTransType(), String.class);
      }

      // Transation ID
      if (isLongNumeric(roiEvent.getUniqueTransactionId())) {
        requestTracker.addTag("roi_bti", roiEvent.getUniqueTransactionId(), String.class);
      }

      // user ID
      if (isLongNumeric(userId)) {
        requestTracker.addTag("userid", userId, String.class);
      }

      // Transaction Time
      if (isLongNumeric(roiEvent.getTransactionTimestamp())) {
        requestTracker.addTag("producereventts", Long.parseLong(roiEvent.getTransactionTimestamp()), Long.class);
      }
    } catch (Exception e) {
      LOGGER.warn("Error when tracking ubi for roi event", e);
      MonitorUtil.info("ErrorWriteRoiEventToUBI");

    }
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
   * @param eventProducerStartTime    actual producer event time, add this to distinguish the click from checkout api
   * @param checkoutAPIClickFlag  checkoutAPIClickFlag, if true, add another metrics to distinguish from other events
   * @param additionalFields channelAction, channelType, platform, landing page type
   */
  private void stopTimerAndLogData(long eventProcessStartTime, long eventProducerStartTime,
                                   boolean checkoutAPIClickFlag, Field<String, Object>... additionalFields) {
    long endTime = System.currentTimeMillis();
    LOGGER.debug(String.format("EndTime: %d", endTime));
    MonitorUtil.info("CollectionServiceSuccess", 1,  additionalFields);
    if (checkoutAPIClickFlag) {
      MonitorUtil.latency("CollectionServiceCheckoutAPIClickAndROIAverageLatency", endTime - eventProducerStartTime);
    } else {
      MonitorUtil.latency("CollectionServiceAverageLatency", endTime - eventProcessStartTime);
    }
  }

  /**
   * Check platform by user agent
   */
  private String getPlatform(UserAgentInfo agentInfo) {
    String platform = Constants.PLATFORM_UNKNOWN;
    if (agentInfo.isDesktop()) {
      platform = Constants.PLATFORM_DESKTOP;
    } else if (agentInfo.isTablet()) {
      platform = Constants.PLATFORM_TABLET;
    } else if (agentInfo.isMobile()) {
      platform = Constants.PLATFORM_MOBILE;
    } else if (agentInfo.isNativeApp()) {
      platform = Constants.PLATFORM_NATIVE_APP;
    }

    return platform;
  }

  private String getSearchEngineFreeListingsRotationId(ContainerRequestContext requestContext) {
    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);
    int siteId = userPrefsCtx.getGeoContext().getSiteId();
    return SearchEngineFreeListingsRotationEnum.parse(siteId).getRotation();
  }

  /**
   * Parse tag from url query string and add to sojourner
   */
  private static void addTagFromUrlQuery(MultiValueMap<String, String> parameters, IRequestScopeTracker requestTracker,
                                         String urlParam, String tag, Class tagType) {
    if (parameters.containsKey(urlParam) && parameters.get(urlParam).get(0) != null) {
      requestTracker.addTag(tag, parameters.get(urlParam).get(0), tagType);
    }
  }

  /**
   * Determine whether the roi is from Checkout API
   * If so, don't track into ubi
   */
  private Boolean isROIFromCheckoutAPI(Map<String, String> roiPayloadMap, IEndUserContext endUserContext) {
    Boolean isROIFromCheckoutAPI = false;
    try {
      if (roiPayloadMap.containsKey(ROI_SOURCE)) {
        if (roiPayloadMap.get(ROI_SOURCE).equals(String.valueOf(RoiSourceEnum.CHECKOUT_SOURCE.getId()))
                && endUserContext.getUserAgent().equals(CHECKOUT_API_USER_AGENT)) {
          isROIFromCheckoutAPI = true;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Determine whether the roi from Checkout API error");
      MonitorUtil.info("DetermineCheckoutAPIROIError", 1);

    }
    return isROIFromCheckoutAPI;
  }

  /**
   * If the click is a duplicate click from itm page, then drop into duplicateItmClickTopic
   * else drop into normal topic
   */
  private void sendClickToDuplicateItmClickTopic(Producer<Long, ListenerMessage> producer, ListenerMessage message) {
    producer.send(new ProducerRecord<>(duplicateItmClickTopic, message.getSnapshotId(), message), KafkaSink.callback);
    MonitorUtil.info("DuplicateItmClick", 1, Field.of(CHANNEL_ACTION, message.getChannelAction().toString()),
            Field.of(CHANNEL_TYPE, message.getChannelType().toString()));

  }

  /**
   * Check if the click is from UFES
   */
  private Boolean isFromUFES(Map<String, String> headers) {
    return headers.containsKey(Constants.IS_FROM_UFES_HEADER) && "true".equals(headers.get(Constants.IS_FROM_UFES_HEADER));
  }

  /**
   * Only for test
   */
  public Producer getBehaviorProducer() {
    return behaviorProducer;
  }

  /**
   * Bug fix: for email vod page, exclude signin referer
   */
  protected boolean isVodInternal(ChannelIdEnum channelType, List<String> pathSegments) {
    if (ChannelIdEnum.MRKT_EMAIL.equals(channelType) || ChannelIdEnum.SITE_EMAIL.equals(channelType)) {
      if (pathSegments.size() >= 2 && VOD_PAGE.equalsIgnoreCase(pathSegments.get(0))
          && VOD_SUB_PAGE.equalsIgnoreCase(pathSegments.get(1))) {
        MonitorUtil.info("VodInternal");

        return true;
      }
    }

    return false;
  }
}
