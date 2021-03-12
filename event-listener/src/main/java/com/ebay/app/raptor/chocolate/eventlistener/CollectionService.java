package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.*;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.collector.CustomerMarketingCollector;
import com.ebay.app.raptor.chocolate.eventlistener.collector.MrktEmailCollector;
import com.ebay.app.raptor.chocolate.eventlistener.collector.PerformanceMarketingCollector;
import com.ebay.app.raptor.chocolate.eventlistener.collector.SiteEmailCollector;
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
import com.ebay.kernel.presentation.constants.PresentationConstants;
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
import org.apache.catalina.User;
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
import java.io.UnsupportedEncodingException;
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
  private static final String UTF_8 = "UTF-8";
  private static final String SOJ_MPRE_TAG = "url_mpre";
  private static final String CHECKOUT_API_USER_AGENT = "checkoutApi";
  private static final String TRACKING_HEADER = "X-EBAY-C-TRACKING";
  private static final String ENDUSERCTX_HEADER = "X-EBAY-C-ENDUSERCTX";
  private static final String ROVER_INTERNAL_VIP = "internal.rover.vip.ebay.com";
  private static final String STR_NULL = "null";

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

    /* referer is from post body (mobile) and from header (NodeJs and handler)
       By internet standard, referer is typo of referrer.
       From ginger client call, the referer is embedded in enduserctx header, but we also check header for other cases.
       For local test using postman, do not include enduserctx header, the service will generate enduserctx by
       cos-user-context-filter.
       Ginger client call will pass enduserctx in its header.
       Priority 1. native app from body, as they are the most part 2. enduserctx, ginger client calls 3. referer header
     */
    String referer = null;
    if (!StringUtils.isEmpty(event.getReferrer())) {
      referer = event.getReferrer();
    }

    if (StringUtils.isEmpty(referer)) {
      referer = endUserContext.getReferer();
    }

    if(StringUtils.isEmpty(referer) && requestHeaders.get(Constants.REFERER_HEADER) != null) {
      referer = requestHeaders.get(Constants.REFERER_HEADER);
    }

    if(StringUtils.isEmpty(referer) && requestHeaders.get(Constants.REFERER_HEADER_UPCASE) != null) {
      referer = requestHeaders.get(Constants.REFERER_HEADER_UPCASE);
    }

    // return 201 for now for the no referer case. Need investigation further.
    if (StringUtils.isEmpty(referer) || referer.equalsIgnoreCase(STR_NULL) ) {
      LOGGER.warn(Errors.ERROR_NO_REFERER);
      metrics.meter(Errors.ERROR_NO_REFERER);
      referer = "";
    }

    // decode referer if necessary. Currently, android is sending rover url encoded.
    if(referer.startsWith(HTTPS_ENCODED) || referer.startsWith(HTTP_ENCODED)) {
      referer = URLDecoder.decode( referer, UTF_8 );
    }

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
      metrics.meter("ePageIncoming");

      Event staticPageEvent = staticPageRequestHandler.parseStaticPageEvent(targetUrl, referer);
      targetUrl = staticPageEvent.getTargetUrl();
      referer = staticPageEvent.getReferrer();
    }

    //XC-1797, for social app deeplink case, extract and decode actual target url from referrer parameter in targetUrl
    //only accept the url when referrer domain belongs to ebay sites
    Matcher deeplinkMatcher = deeplinksites.matcher(targetUrl.toLowerCase());
    if (deeplinkMatcher.find()) {
      metrics.meter("IncomingSocialAppDeepLink");

      Event customizedSchemeEvent = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetUrl, referer);
      if(customizedSchemeEvent == null) {
        logError(Errors.ERROR_NO_TARGET_URL_DEEPLINK);
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

    // invalid mkcid, show error and accept
    channelType = ChannelIdEnum.parse(parameters.get(Constants.MKCID).get(0));
    if (channelType == null) {
      LOGGER.warn(Errors.ERROR_INVALID_MKCID + " {}", targetUrl);
      metrics.meter("InvalidMkcid");
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
        metrics.meter("NoMkpidParameter");
      } else {
        // invalid mkpid, accepted
        partner = EmailPartnerIdEnum.parse(parameters.get(Constants.MKPID).get(0));
        if (StringUtils.isEmpty(partner)) {
          LOGGER.warn(Errors.ERROR_INVALID_MKPID);
          metrics.meter("InvalidMkpid");
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

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = getPlatform(agentInfo);

    String action = ChannelActionEnum.CLICK.toString();
    String type = channelType.getLogicalChannel().getAvro().toString();

    // Self-service events, send them to couchbase
    if (parameters.containsKey(Constants.SELF_SERVICE) && parameters.containsKey(Constants.SELF_SERVICE_ID)) {
      if ("1".equals(parameters.getFirst(Constants.SELF_SERVICE)) &&
          parameters.getFirst(Constants.SELF_SERVICE_ID) != null) {
        metrics.meter("SelfServiceIncoming");
        CouchbaseClient.getInstance().addSelfServiceRecord(parameters.getFirst(Constants.SELF_SERVICE_ID), targetUrl);
        metrics.meter("SelfServiceSuccess");

        return true;
      }
    }

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
        Field.of(PARTNER, partner), Field.of(PLATFORM, platform),
        Field.of(LANDING_PAGE_TYPE, landingPageType));

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
            metrics.meter("ErrorCheckoutAPIClickTimestamp", 1);
          }
        }
      }
    }
    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);

    // Determine whether the click is a duplicate click
    // If duplicate click, then drop into duplicateItmClickTopic
    // If not, drop into normal topic
    boolean isDuplicateClick = false;
    try {
      isDuplicateClick = CollectionServiceUtil.isDuplicateItmClick(
          request.getHeader(Constants.NODE_REDIRECTION_HEADER_NAME), endUserContext.getUserAgent(),
          targetUrl, agentInfo.requestIsFromBot(), agentInfo.isMobile(), agentInfo.requestIsMobileWeb());

      // send duplicate click to a dedicate listener topic
      if(isDuplicateClick) {
        Producer<Long, ListenerMessage> producer = KafkaSink.get();
//        ListenerMessage listenerMessage = parser.parse(request, requestContext, startTime, 0L,
//            channelType.getLogicalChannel().getAvro(), ChannelActionEnum.CLICK, "", endUserContext, targetUrl,
//            referer, 0L, "");
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
      metrics.meter("DetermineDuplicateItmClickError", 1);
    }

    // Overwrite the referer for the clicks from Promoted Listings iframe on ebay partner sites XC-3256
    // only for EPN channel
    boolean isEPNClickFromPromotedListings = false;
    try {
      isEPNClickFromPromotedListings
          = CollectionServiceUtil.isEPNPromotedListingsClick(channelType, parameters, referer);

      if (isEPNClickFromPromotedListings) {
        referer = URLDecoder.decode(parameters.get(Constants.PLRFR).get(0), StandardCharsets.UTF_8.name());
        metrics.meter("OverwriteRefererForPromotedListingsClick");
      }
    } catch (Exception e) {
      LOGGER.error("Determine whether the click is from promoted listings iframe error");
      metrics.meter("DeterminePromotedListingsClickError", 1);
    }

    // until now, generate eventId in advance of utp tracking so that it can be emitted into both ubi&utp only for click
    String utpEventId = UUID.randomUUID().toString();

    // add tags in url param "sojTags"
    // Don't track ubi if the click is a duplicate itm click
    if(parameters.containsKey(Constants.SOJ_TAGS) && parameters.get(Constants.SOJ_TAGS).get(0) != null
        && !isDuplicateClick) {
      addGenericSojTags(requestContext, parameters, referer, type, action);
    }

    // add tags all channels need
    // Don't track ubi if the click is from Checkout API
    if (!isClickFromCheckoutAPI(channelType.getLogicalChannel().getAvro(), endUserContext)) {
      // Don't track ubi if the click is a duplicate itm click
      if (!isDuplicateClick) {
        addCommonTags(requestContext, targetUrl, referer, agentInfo, utpEventId, type, action,
            PageIdEnum.CLICK.getId());
      }
    } else {
      metrics.meter("CheckoutAPIClick", 1);
    }

    if(!isDuplicateClick) {
      ListenerMessage listenerMessage = null;
      // add channel specific tags, and produce message for EPN and IMK
      if (PM_CHANNELS.contains(channelType)) {
        listenerMessage = processPMEvent(requestContext, requestHeaders, userPrefsCtx, targetUrl, referer, parameters,
            channelType, channelAction, request, startTime, endUserContext, raptorSecureContext, agentInfo);
      }
      else if (channelType == ChannelIdEnum.SITE_EMAIL) {
        processCmEvent(requestContext, endUserContext, referer, parameters, type, action, request, agentInfo,
            targetUrl, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
            siteEmailCollector);
      }
      else if (channelType == ChannelIdEnum.MRKT_EMAIL) {
        processCmEvent(requestContext, endUserContext, referer, parameters, type, action, request, agentInfo,
            targetUrl, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
            mrktEmailCollector);
      }
      else if (channelType == ChannelIdEnum.MRKT_SMS || channelType == ChannelIdEnum.SITE_SMS) {
        processSMSEvent(requestContext, referer, parameters, type, action);
      }

      // send to unified tracking topic
      if (listenerMessage != null) {
        processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, agentInfo, parameters,
            targetUrl, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
            null, listenerMessage.getSnapshotId(), listenerMessage.getShortSnapshotId(), utpEventId, startTime);
      } else {
        processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, agentInfo, parameters,
            targetUrl, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
            null, 0L, 0L, utpEventId, startTime);
      }
    }

    stopTimerAndLogData(eventProcessStartTime, startTime, checkoutAPIClickFlag, Field.of(CHANNEL_ACTION, action),
        Field.of(CHANNEL_TYPE, type), Field.of(PARTNER, partner), Field.of(PLATFORM, platform),
        Field.of(LANDING_PAGE_TYPE, landingPageType));

    return true;
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
    String userId;
    if ("EBAYUSER".equals(raptorSecureContext.getSubjectDomain())) {
      userId = raptorSecureContext.getSubjectImmutableId();
    } else {
      userId = Long.toString(endUserContext.getOrigUserOracleId());
    }

    try {
      long itemId = Long.parseLong(roiEvent.getItemId());
      if (itemId < 0) {
        roiEvent.setItemId("");
      }
    } catch (Exception e) {
      LOGGER.warn("Error itemId " + roiEvent.getItemId());
      metrics.meter("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));
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
      metrics.meter("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));
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
      metrics.meter("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));
      roiEvent.setUniqueTransactionId("");
    }

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = getPlatform(agentInfo);

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
    Boolean isRoiFromCheckoutAPI = isROIFromCheckoutAPI(payloadMap, endUserContext);
    if (!isRoiFromCheckoutAPI) {
      addRoiSojTags(requestContext, roiEvent, userId);
    } else {
      metrics.meter("CheckoutAPIROI", 1);
    }

    // Write roi event to kafka output topic
    boolean processFlag = processROIEvent(requestContext, targetUrl, referer, parameters, ChannelIdEnum.ROI,
        ChannelActionEnum.ROI, request, transTimestamp, endUserContext, raptorSecureContext, agentInfo, roiEvent);

    if (processFlag) {
      metrics.meter("NewROICountAPI", 1, Field.of(CHANNEL_ACTION, "New-ROI"),
          Field.of(CHANNEL_TYPE, "New-ROI"), Field.of(ROI_SOURCE, String.valueOf(payloadMap.get(ROI_SOURCE))));
      // Log the roi lag between transation time and receive time
      metrics.mean("RoiTransationLag", startTime - transTimestamp, Field.of(CHANNEL_ACTION, "ROI"),
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

    // referer is in both request header and body
    // we just get referer from body, and tracking api get it from header
    String referer = null;
    if (!StringUtils.isEmpty(event.getReferrer())) {
      referer = event.getReferrer();
    }

    if (StringUtils.isEmpty(referer)) {
      referer = endUserContext.getReferer();
    }

    if(StringUtils.isEmpty(referer) && request.getHeader(Constants.REFERER_HEADER) != null) {
      referer = request.getHeader(Constants.REFERER_HEADER);
    }

    if(StringUtils.isEmpty(referer) && request.getHeader(Constants.REFERER_HEADER_UPCASE) != null) {
      referer = request.getHeader(Constants.REFERER_HEADER_UPCASE);
    }

    if (StringUtils.isEmpty(referer) || referer.equalsIgnoreCase(STR_NULL)) {
      LOGGER.warn(Errors.ERROR_NO_REFERER);
      metrics.meter(Errors.ERROR_NO_REFERER);
      referer = "";
    }

    // decode referer if necessary. Currently, android is sending rover url encoded.
    if (referer.startsWith(HTTPS_ENCODED) || referer.startsWith(HTTP_ENCODED)) {
      referer = URLDecoder.decode(referer, "UTF-8");
    }

    String userAgent = request.getHeader("User-Agent");
    if (null == userAgent) {
      LOGGER.warn(Errors.ERROR_NO_USER_AGENT);
      metrics.meter(Errors.ERROR_NO_USER_AGENT);
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
    String partner = null;
    if (channelAction == ChannelActionEnum.EMAIL_OPEN) {
      // no mkpid, accepted
      if (!parameters.containsKey(Constants.MKPID) || parameters.get(Constants.MKPID).get(0) == null) {
        LOGGER.warn(Errors.ERROR_NO_MKPID);
        metrics.meter("NoMkpidParameter");
      } else {
        // invalid mkpid, accepted
        partner = EmailPartnerIdEnum.parse(parameters.get(Constants.MKPID).get(0));
        if (StringUtils.isEmpty(partner)) {
          LOGGER.warn(Errors.ERROR_INVALID_MKPID);
          metrics.meter("InvalidMkpid");
        }
      }
    }

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = getPlatform(agentInfo);

    String action = channelAction.getAvro().toString();
    String type = channelType.getLogicalChannel().getAvro().toString();

    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
        Field.of(PARTNER, partner), Field.of(PLATFORM, platform));

    // add channel specific tags, and produce message for EPN and IMK
    ListenerMessage listenerMessage = null;
    if (channelType == ChannelIdEnum.SITE_EMAIL) {
      processCmEvent(requestContext, endUserContext, referer, parameters, type, action, request,
          agentInfo, uri, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
          siteEmailCollector);
    }
    else if (channelType == ChannelIdEnum.MRKT_EMAIL) {
      processCmEvent(requestContext, endUserContext, referer, parameters, type, action, request,
          agentInfo, uri, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
          mrktEmailCollector);
    }
    else {
      listenerMessage = processPMEvent(requestContext, requestHeaders, userPrefsCtx, uri, referer, parameters,
          channelType, channelAction, request, startTime, endUserContext, raptorSecureContext, agentInfo);
    }

    // send to unified tracking topic
    if(listenerMessage!=null) {
      processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, agentInfo, parameters,
          uri, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
          null, listenerMessage.getSnapshotId(), listenerMessage.getShortSnapshotId(), null, startTime);
    } else {
      processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, agentInfo, parameters,
          uri, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
          null, 0L, 0L, null, startTime);
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
    // get user id from auth token if it's user token, else we get from end user ctx
    String userId;
    if ("EBAYUSER".equals(raptorSecureContext.getSubjectDomain())) {
      userId = raptorSecureContext.getSubjectImmutableId();
    } else {
      userId = Long.toString(endUserContext.getOrigUserOracleId());
    }

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

    processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, agentInfo, parameters,
            targetUrl, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
            roiEvent, message.getSnapshotId(), message.getShortSnapshotId(), null, startTime);

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

    UnifiedTrackingMessage message = UnifiedTrackingMessageParser.parse(event, userLookup);

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
                                           UserAgentInfo agentInfo, MultiValueMap<String, String> parameters,
                                           String url, String referer, ChannelType channelType,
                                           ChannelAction channelAction, ROIEvent roiEvent, long snapshotId,
                                           long shortSnapshotId, String eventId, long startTime) {
    try {
      Matcher m = ebaysites.matcher(referer.toLowerCase());
      if (ChannelAction.EMAIL_OPEN.equals(channelAction) || ChannelAction.ROI.equals(channelAction) || !m.find()) {
        UnifiedTrackingMessage utpMessage = UnifiedTrackingMessageParser.parse(requestContext, request, endUserContext,
                raptorSecureContext, agentInfo, userLookup, parameters, url, referer, channelType, channelAction,
                roiEvent, snapshotId, shortSnapshotId, startTime);
        if(!StringUtils.isEmpty(eventId)) {
          utpMessage.setEventId(eventId);
        }
        unifiedTrackingProducer.send(new ProducerRecord<>(unifiedTrackingTopic, utpMessage.getEventId().getBytes(),
            utpMessage), UnifiedTrackingKafkaSink.callback);
      } else {
        metrics.meter("UTPInternalDomainRef", 1, Field.of(CHANNEL_ACTION, channelAction.toString()),
                Field.of(CHANNEL_TYPE, channelType.toString()));
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
  private ListenerMessage processPMEvent(ContainerRequestContext requestContext, Map<String, String> requestHeaders,
                                         UserPrefsCtx userPrefsCtx, String targetUrl, String referer,
                                         MultiValueMap<String, String> parameters, ChannelIdEnum channelType,
                                         ChannelActionEnum channelAction, HttpServletRequest request, long startTime,
                                         IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext,
                                         UserAgentInfo agentInfo) {

    ListenerMessage listenerMessage = null;

    listenerMessage = performanceMarketingCollector.parseListenerMessage(requestHeaders, userPrefsCtx,
        targetUrl, referer, parameters, channelType, channelAction, request, startTime, endUserContext,
        raptorSecureContext);

    // 1. send to chocolate topic
    // If the click is a duplicate click from itm page, then drop into duplicateItmClickTopic
    // else drop into normal topic
    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    String kafkaTopic = ApplicationOptions.getInstance()
        .getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());

    producer.send(
        new ProducerRecord<>(kafkaTopic, listenerMessage.getSnapshotId(), listenerMessage),
        KafkaSink.callback);

    // 2. track ubi
    IRequestScopeTracker requestTracker =
        (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);
    performanceMarketingCollector.trackUbi(requestTracker, referer, parameters, channelType,
        channelAction, startTime, endUserContext, listenerMessage);

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
                                        CustomerMarketingCollector cmCollector) {
    // Tracking ubi only when refer domain is not ebay.
    Matcher m = ebaysites.matcher(referer.toLowerCase());

    // Email open should not be filtered by referer
    if(ChannelAction.EMAIL_OPEN.equals(channelAction) || !m.find()) {
      long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId()).getRepresentation();

      // 0. temporary, send click and open event to message tracker
      eventEmitterPublisher.publishEvent(requestContext, parameters, uri, channelType, channelAction, snapshotId);

      // 1. track ubi
      cmCollector.trackUbi(requestContext, parameters, type, action, request, uri, channelAction);

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
    else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }

    return true;
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
  private boolean processSMSEvent(ContainerRequestContext requestContext, String referer,
                                  MultiValueMap<String, String> parameters, String type, String action) {

    // Tracking ubi only when refer domain is not ebay.
    // Don't track ubi if the click is a duplicate itm click
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find()) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker
            = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // event family
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, Constants.EVENT_FAMILY_CRM, String.class);

        // sms unique id
        addTagFromUrlQuery(parameters, requestTracker, Constants.SMS_ID, "smsid", String.class);

      } catch (Exception e) {
        LOGGER.warn("Error when tracking ubi for sms click tags", e);
        metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
      }
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }

    return true;
  }

  /**
   * Add common soj tags all channels in common
   * @param requestContext  wrapped raptor request context
   * @param targetUrl       landing page url
   * @param referer         referer of the request
   * @param agentInfo       user agent
   * @param utpEventId      utp event id
   * @param type            channel type
   * @param action          action type
   * @param pageId          soj page id
   */
  private void addCommonTags(ContainerRequestContext requestContext, String targetUrl, String referer,
                             UserAgentInfo agentInfo, String utpEventId, String type, String action, int pageId) {
    // Tracking ubi only when refer domain is not ebay.
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find()) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker
            = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // page id
        requestTracker.addTag(TrackerTagValueUtil.PageIdTag, pageId, Integer.class);

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

        // utp event id
        if(!StringUtils.isEmpty(utpEventId)) {
          requestTracker.addTag("utpid", utpEventId, String.class);
        }

        // populate device info
        CollectionServiceUtil.populateDeviceDetectionParams(agentInfo, requestTracker);

      } catch (Exception e) {
        LOGGER.warn("Error when tracking ubi for common tags", e);
        metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
      }
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }
  }

  /**
   * Add generic soj tags for email channel. Those tags are defined in URL which needed to be added as a tag.
   * @param requestContext  wrapped raptor request context
   * @param parameters      url parameters
   * @param referer         referer of the request
   * @param type            channel type
   * @param action          action type
   */
  private void addGenericSojTags(ContainerRequestContext requestContext, MultiValueMap<String, String> parameters,
                                 String referer, String type, String action) {
    // Tracking ubi only when refer domain is not ebay.
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find()) {
      // Ubi tracking
      IRequestScopeTracker requestTracker
          = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

      String sojTags = parameters.get(Constants.SOJ_TAGS).get(0);
      try {
        sojTags = URLDecoder.decode(sojTags, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        LOGGER.warn("Param sojTags is wrongly encoded", e);
        metrics.meter("ErrorEncodedSojTags", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
      }
      if (!StringUtils.isEmpty(sojTags)) {
        StringTokenizer stToken = new StringTokenizer(sojTags, PresentationConstants.COMMA);
        while (stToken.hasMoreTokens()) {
          try {
            StringTokenizer sojNvp = new StringTokenizer(stToken.nextToken(), PresentationConstants.EQUALS);
            if (sojNvp.countTokens() == 2) {
              String sojTag = sojNvp.nextToken().trim();
              String urlParam = sojNvp.nextToken().trim();
              if (!StringUtils.isEmpty(urlParam) && !StringUtils.isEmpty(sojTag)) {
                addTagFromUrlQuery(parameters, requestTracker, urlParam, sojTag, String.class);
              }
            }
          } catch (Exception e) {
            LOGGER.warn("Error when tracking ubi for common tags", e);
            metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
          }
        }
      }
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }
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
      metrics.meter("ErrorWriteRoiEventToUBI");
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
   * @param eventProducerStartTime    actual producer event time, add this to distinguish the click from checkout api
   * @param checkoutAPIClickFlag  checkoutAPIClickFlag, if true, add another metrics to distinguish from other events
   * @param additionalFields channelAction, channelType, platform, landing page type
   */
  private void stopTimerAndLogData(long eventProcessStartTime, long eventProducerStartTime,
                                   boolean checkoutAPIClickFlag, Field<String, Object>... additionalFields) {
    long endTime = System.currentTimeMillis();
    LOGGER.debug(String.format("EndTime: %d", endTime));
    metrics.meter("CollectionServiceSuccess", 1, eventProcessStartTime, additionalFields);
    if (checkoutAPIClickFlag) {
      metrics.mean("CollectionServiceCheckoutAPIClickAndROIAverageLatency", endTime - eventProducerStartTime);
    } else {
      metrics.mean("CollectionServiceAverageLatency", endTime - eventProcessStartTime);
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
   * Determine whether the click is from Checkout API
   * If so, don't track into ubi
   */
  private Boolean isClickFromCheckoutAPI(ChannelType channelType, IEndUserContext endUserContext) {
    Boolean isClickFromCheckoutAPI = false;
    try {
      if (channelType == ChannelType.EPN && endUserContext.getUserAgent().equals(CHECKOUT_API_USER_AGENT)) {
        isClickFromCheckoutAPI = true;
      }
    } catch (Exception e) {
      LOGGER.error("Determine whether the click from Checkout API error");
      metrics.meter("DetermineCheckoutAPIClickError", 1);
    }
    return isClickFromCheckoutAPI;
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
      metrics.meter("DetermineCheckoutAPIROIError", 1);
    }
    return isROIFromCheckoutAPI;
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
