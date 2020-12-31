package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.*;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.CommonConstant;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.component.GdprConsentHandler;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.util.*;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.gen.model.EventPayload;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.app.raptor.chocolate.gen.model.UnifiedTrackingEvent;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
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
import org.apache.http.client.utils.URIBuilder;
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
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ebay.app.raptor.chocolate.constant.Constants.REFERRER;
import static com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil.isLongNumeric;

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
  private static final Logger logger = LoggerFactory.getLogger(CollectionService.class);
  private Metrics metrics;
  private ListenerMessageParser parser;
  private BehaviorMessageParser behaviorMessageParser;
  private Producer behaviorProducer;
  private String behaviorTopic;
  private Producer unifiedTrackingProducer;
  private String unifiedTrackingTopic;
  public String duplicateItmClickTopic;
  private static CollectionService instance = null;
  private EventEmitterPublisher eventEmitterPublisher;
  private String ROVER_INTERNAL_VIP = "internal.rover.vip.ebay.com";

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

  private static final String CHANNEL_ACTION = "channelAction";
  private static final String CHANNEL_TYPE = "channelType";
  private static final String PARTNER = "partner";
  private static final String PLATFORM = "platform";
  private static final String LANDING_PAGE_TYPE = "landingPageType";
  private static final String PAGE_ID = "pageId";
  private static final String ADGUID_PARAM = "adguid";
  private static final String SITE_ID = "siteId";
  private static final String ROI_SOURCE = "roisrc";
  private static final String UTF_8 = "UTF-8";
  private static final String ROVER_MPRE_PARAM = "mpre";
  private static final String SOJ_MPRE_TAG = "url_mpre";
  private static final String CHECKOUT_API_USER_AGENT = "checkoutApi";

  // do not filter /ulk XC-1541
  private static Pattern ebaysites = Pattern.compile("^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)", Pattern.CASE_INSENSITIVE);
  private static Pattern roversites = Pattern.compile("^(http[s]?:\\/\\/)?rover\\.(qa\\.)?ebay\\.[\\w-.]+(\\/.*)", Pattern.CASE_INSENSITIVE);

  // app deeplink sites XC-1797
  private static Pattern deeplinksites = Pattern.compile("^ebay:\\/\\/link\\/([\\w-$%?&/.])?", Pattern.CASE_INSENSITIVE);
  // determine whether the url belongs to ebay sites for app deep link, and don't do any filter
  private static Pattern deeplinkEbaySites = Pattern.compile("^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/.*)", Pattern.CASE_INSENSITIVE);

  // e page target url sites
  private static Pattern ePageSites = Pattern.compile("^(http[s]?:\\/\\/)?c\\.([\\w.]+\\.)?(qa\\.)?ebay\\.[\\w-.]+\\/marketingtracking\\/v1\\/pixel\\?(.*)", Pattern.CASE_INSENSITIVE);

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
   *
   * @param request             raw request
   * @param endUserContext      wrapped end user context
   * @param raptorSecureContext wrapped secure header context
   * @param event               post body event
   * @return OK or Error message
   */
  public boolean collect(HttpServletRequest request, IEndUserContext endUserContext, RaptorSecureContext
          raptorSecureContext, ContainerRequestContext requestContext, Event event) throws Exception {

    if (request.getHeader("X-EBAY-C-TRACKING") == null) {
      logError(Errors.ERROR_NO_TRACKING);
    }

    if (request.getHeader("X-EBAY-C-ENDUSERCTX") == null) {
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

    if(StringUtils.isEmpty(referer) && request.getHeader(Constants.REFERER_HEADER) != null) {
      referer = request.getHeader(Constants.REFERER_HEADER);
    }

    if(StringUtils.isEmpty(referer) && request.getHeader(Constants.REFERER_HEADER_UPCASE) != null) {
      referer = request.getHeader(Constants.REFERER_HEADER_UPCASE);
    }

    // return 201 for now for the no referer case. Need investigation further.
    if (StringUtils.isEmpty(referer) || referer.equalsIgnoreCase("null") ) {
      logger.warn(Errors.ERROR_NO_REFERER);
      metrics.meter(Errors.ERROR_NO_REFERER);
      referer = "";
    }

    // decode referer if necessary. Currently, android is sending rover url encoded.
    if(referer.startsWith("https%3A%2F%2") || referer.startsWith("http%3A%2F%2")) {
      referer = URLDecoder.decode( referer, "UTF-8" );
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

      String originalReferer = "";
      String targetPath = "";
      UriComponents uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();
      if (uriComponents != null && uriComponents.getQueryParams() != null) {
        originalReferer = uriComponents.getQueryParams().getFirst(Constants.EPAGE_REFERER);
        targetPath = uriComponents.getQueryParams().getFirst(Constants.EPAGE_URL);
      }

      if (!StringUtils.isEmpty(targetPath)) {
        URIBuilder uriBuilder = new URIBuilder(URLDecoder.decode(targetPath, "UTF-8"));
        uriBuilder.addParameters(new URIBuilder(targetUrl).getQueryParams());
        targetUrl = HttpRequestUtil.removeParam(uriBuilder.build().toString(), Constants.EPAGE_URL);
      } else {
        targetUrl = referer;
      }

      if (!StringUtils.isEmpty(originalReferer)) {
        referer = URLDecoder.decode(originalReferer, "UTF-8");
        targetUrl = HttpRequestUtil.removeParam(targetUrl, Constants.EPAGE_REFERER);
      } else {
        logger.warn(Errors.ERROR_NO_REFERER);
        metrics.meter(Errors.ERROR_NO_REFERER);
        referer = "";
      }
    }

    //XC-1797, for social app deeplink case, extract and decode actual target url from referrer parameter in targetUrl
    //only accept the url when referrer domain belongs to ebay sites
    Matcher deeplinkMatcher = deeplinksites.matcher(targetUrl.toLowerCase());
    if (deeplinkMatcher.find()) {
      metrics.meter("IncomingSocialAppDeepLink");

      UriComponents deeplinkUriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();

      MultiValueMap<String, String> deeplinkParameters = deeplinkUriComponents.getQueryParams();
      if (deeplinkParameters.size() == 0 || !deeplinkParameters.containsKey(REFERRER)) {
        logError(Errors.ERROR_NO_TARGET_URL_DEEPLINK);
      }

      String deeplinkTargetUrl = deeplinkParameters.get(REFERRER).get(0);

      try {
        if(deeplinkTargetUrl.startsWith("https%3A%2F%2") || deeplinkTargetUrl.startsWith("http%3A%2F%2")) {
          deeplinkTargetUrl = URLDecoder.decode(deeplinkTargetUrl, "UTF-8");
        }
      } catch (Exception ex) {
        metrics.meter("DecodeDeepLinkTargetUrlError");
        logger.warn("Decode deeplink target url error.");
      }

      Matcher deeplinkEbaySitesMatcher = deeplinkEbaySites.matcher(deeplinkTargetUrl.toLowerCase());
      if (deeplinkEbaySitesMatcher.find()) {
        targetUrl = deeplinkTargetUrl;
        metrics.meter("IncomingSocialAppDeepLinkSuccess");
      } else {
        logger.warn(Errors.ERROR_INVALID_TARGET_URL_DEEPLINK);
        metrics.meter(Errors.ERROR_INVALID_TARGET_URL_DEEPLINK);
        return true;
      }
    }

    // parse channel from uri
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();

    // XC-1695. no query parameter, rejected but return 201 accepted for clients since app team has started unconditionally call
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
    if (parameters.size() == 0) {
      logger.warn(Errors.ERROR_NO_QUERY_PARAMETER);
      metrics.meter(Errors.ERROR_NO_QUERY_PARAMETER);
      return true;
    }

    // XC-1695. no mkevt, rejected but return 201 accepted for clients since app team has started unconditionally call
    if (!parameters.containsKey(Constants.MKEVT) || parameters.get(Constants.MKEVT).get(0) == null) {
      logger.warn(Errors.ERROR_NO_MKEVT);
      metrics.meter(Errors.ERROR_NO_MKEVT);
      return true;
    }

    // XC-1695. mkevt != 1, rejected but return 201 accepted for clients
    String mkevt = parameters.get(Constants.MKEVT).get(0);
    if (!mkevt.equals(Constants.VALID_MKEVT_CLICK)) {
      logger.warn(Errors.ERROR_INVALID_MKEVT);
      metrics.meter(Errors.ERROR_INVALID_MKEVT);
      return true;
    }

    // parse channel from query mkcid
    // no mkcid, rejected but return 201 accepted for clients
    if (!parameters.containsKey(Constants.MKCID) || parameters.get(Constants.MKCID).get(0) == null) {
      logger.warn(Errors.ERROR_NO_MKCID);
      metrics.meter("NoMkcidParameter");
      return true;
    }

    // invalid mkcid, show error and accept
    channelType = ChannelIdEnum.parse(parameters.get(Constants.MKCID).get(0));
    if (channelType == null) {
      logger.warn(Errors.ERROR_INVALID_MKCID + " {}", targetUrl);
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
        logger.warn(Errors.ERROR_NO_MKPID);
        metrics.meter("NoMkpidParameter");
      } else {
        // invalid mkpid, accepted
        partner = EmailPartnerIdEnum.parse(parameters.get(Constants.MKPID).get(0));
        if (StringUtils.isEmpty(partner)) {
          logger.warn(Errors.ERROR_INVALID_MKPID);
          metrics.meter("InvalidMkpid");
        }
      }
    }

    String landingPageType;
    List<String> pathSegments = uriComponents.getPathSegments();
    if (pathSegments == null || pathSegments.size() == 0) {
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
                logger.warn("Error click timestamp from Checkout API" + checkoutAPIClickTs);
                metrics.meter("ErrorCheckoutAPIClickTimestamp", 1);
            }
        }
      }
    }

    // Determine whether the click is a duplicate click
    // If duplicate click, then drop into duplicateItmClickTopic
    // If not, drop into normal topic
    boolean isDuplicateClick = false;
    try {
      isDuplicateClick = CollectionServiceUtil.isDuplicateItmClick(request.getHeader(Constants.NODE_REDIRECTION_HEADER_NAME),
              endUserContext.getUserAgent(), targetUrl, agentInfo.requestIsFromBot(), agentInfo.isMobile(), agentInfo.requestIsMobileWeb());
    } catch (Exception e) {
      logger.error("Determine whether the click is duplicate item click error");
      metrics.meter("DetermineDuplicateItmClickError", 1);
    }

    // add tags in url param "sojTags"
    // Don't track ubi if the click is a duplicate itm click
    if(parameters.containsKey(Constants.SOJ_TAGS) && parameters.get(Constants.SOJ_TAGS).get(0) != null && !isDuplicateClick) {
      addGenericSojTags(requestContext, parameters, referer, type, action);
    }

    // add tags all channels need
    // Don't track ubi if the click is from Checkout API
    if (!isClickFromCheckoutAPI(channelType.getLogicalChannel().getAvro(), endUserContext)) {
      // Don't track ubi if the click is a duplicate itm click
      if (!isDuplicateClick) {
        addCommonTags(requestContext, targetUrl, referer, agentInfo, type, action, PageIdEnum.CLICK.getId());
      }
    } else {
      metrics.meter("CheckoutAPIClick", 1);
    }


    // add channel specific tags, and produce message for EPN and IMK
    boolean processFlag = false;
    if (channelType == ChannelIdEnum.EPN || channelType == ChannelIdEnum.PAID_SEARCH || channelType == ChannelIdEnum.DAP ||
        channelType == ChannelIdEnum.SOCIAL_MEDIA || channelType == ChannelIdEnum.SEARCH_ENGINE_FREE_LISTINGS)
      processFlag = processAmsAndImkEvent(requestContext, targetUrl, referer, parameters, channelType, channelAction,
          request, startTime, endUserContext, raptorSecureContext, agentInfo, isDuplicateClick);
    else if (channelType == ChannelIdEnum.SITE_EMAIL)
      processFlag = processSiteEmailEvent(requestContext, endUserContext, referer, parameters, type, action, request, agentInfo,
          targetUrl, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), isDuplicateClick);
    else if (channelType == ChannelIdEnum.MRKT_EMAIL)
      processFlag = processMrktEmailEvent(requestContext, endUserContext, referer, parameters, type, action, request, agentInfo,
          targetUrl, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), isDuplicateClick);
    else if (channelType == ChannelIdEnum.MRKT_SMS || channelType == ChannelIdEnum.SITE_SMS)
      processFlag = processSMSEvent(requestContext, referer, parameters, type, action, isDuplicateClick);

    // send to unified tracking topic
    // send email channels first
    if (ChannelIdEnum.SITE_EMAIL.equals(channelType) || ChannelIdEnum.MRKT_EMAIL.equals(channelType)) {
      processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, agentInfo, parameters,
          targetUrl, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), null, 0L, 0L, startTime);
    }

    if (processFlag)
      stopTimerAndLogData(eventProcessStartTime, startTime, checkoutAPIClickFlag, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
              Field.of(PARTNER, partner), Field.of(PLATFORM, platform), Field.of(LANDING_PAGE_TYPE, landingPageType));

    return true;
  }

  /**
   * Collect roi event and publish to kafka
   *
   * @param request             raw request
   * @param endUserContext      wrapped end user context
   * @param raptorSecureContext wrapped secure header context
   * @return OK or Error message
   */

  public boolean collectROIEvent(HttpServletRequest request, IEndUserContext endUserContext, RaptorSecureContext
      raptorSecureContext, ContainerRequestContext requestContext, ROIEvent roiEvent) throws Exception {

    if (request.getHeader("X-EBAY-C-TRACKING") == null) {
      logError(Errors.ERROR_NO_TRACKING);
    }

    if (request.getHeader("X-EBAY-C-ENDUSERCTX") == null) {
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
      long itemId = Long.valueOf(roiEvent.getItemId());
      if (itemId < 0)
        roiEvent.setItemId("");
    } catch (Exception e) {
      logger.warn("Error itemId " + roiEvent.getItemId());
      metrics.meter("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));
      roiEvent.setItemId("");
    }
    // Parse timestamp if it null or invalid, change it to localTimestamp
    long transTimestamp = 0;
    try {
      transTimestamp = Long.valueOf(roiEvent.getTransactionTimestamp());
      if(transTimestamp <= 0) {
        roiEvent.setTransactionTimestamp(localTimestamp);
        transTimestamp = Long.parseLong(localTimestamp);
      }
    } catch (Exception e) {
      logger.warn("Error timestamp " + roiEvent.getTransactionTimestamp());
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
      logger.warn("Error transactionId " + roiEvent.getUniqueTransactionId());
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

    if (StringUtils.isEmpty(referer) || referer.equalsIgnoreCase("null")) {
      logger.warn(Errors.ERROR_NO_REFERER);
      metrics.meter(Errors.ERROR_NO_REFERER);
      referer = "";
    }
    // decode referer if necessary
    if(referer.startsWith("https%3A%2F%2") || referer.startsWith("http%3A%2F%2")) {
      referer = URLDecoder.decode( referer, "UTF-8" );
    }

    // write roi event tags into ubi
    // Don't write into ubi if roi is from Checkout API
    Boolean isRoiFromCheckoutAPI = isROIFromCheckoutAPI(payloadMap, endUserContext);
    if (!isRoiFromCheckoutAPI) {
      addRoiSojTags(requestContext, payloadMap, roiEvent, userId);
    } else {
      metrics.meter("CheckoutAPIROI", 1);
    }

    // Write roi event to kafka output topic
    boolean processFlag = processROIEvent(requestContext, targetUrl, referer, parameters, ChannelIdEnum.ROI,
        ChannelActionEnum.ROI, request, transTimestamp, endUserContext, raptorSecureContext, agentInfo, isRoiFromCheckoutAPI, roiEvent);

    if (processFlag) {
      metrics.meter("NewROICountAPI", 1, Field.of(CHANNEL_ACTION, "New-ROI"),
          Field.of(CHANNEL_TYPE, "New-ROI"), Field.of(ROI_SOURCE, String.valueOf(payloadMap.get(ROI_SOURCE))));
      // Log the roi lag between transation time and receive time
      metrics.mean("RoiTransationLag", startTime - transTimestamp, Field.of(CHANNEL_ACTION, "ROI"), Field.of(CHANNEL_TYPE, "ROI"));
      stopTimerAndLogData(startTime, startTime, false, Field.of(CHANNEL_ACTION, ChannelActionEnum.ROI.toString()), Field.of(CHANNEL_TYPE,
          ChannelType.ROI.toString()), Field.of(PLATFORM, platform));
    }
    return true;
  }

  /**
   * Collect impression event and send pixel response
   *
   * @param request raw request
   * @return OK or Error message
   */
  public boolean collectImpression(HttpServletRequest request, IEndUserContext endUserContext, RaptorSecureContext
      raptorSecureContext, ContainerRequestContext requestContext, Event event) throws Exception {

    if (request.getHeader("X-EBAY-C-TRACKING") == null) {
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

    if (StringUtils.isEmpty(referer) || referer.equalsIgnoreCase("null")) {
      logger.warn(Errors.ERROR_NO_REFERER);
      metrics.meter(Errors.ERROR_NO_REFERER);
      referer = "";
    }

    // decode referer if necessary. Currently, android is sending rover url encoded.
    if (referer.startsWith("https%3A%2F%2") || referer.startsWith("http%3A%2F%2")) {
      referer = URLDecoder.decode(referer, "UTF-8");
    }

    String userAgent = request.getHeader("User-Agent");
    if (null == userAgent) {
      logger.warn(Errors.ERROR_NO_USER_AGENT);
      metrics.meter(Errors.ERROR_NO_USER_AGENT);
    }

    ChannelIdEnum channelType;
    ChannelActionEnum channelAction = null;

    // uri is from post body
    String uri = event.getTargetUrl();

    UriComponents uriComponents = UriComponentsBuilder.fromUriString(uri).build();

    // XC-1695. no query parameter, rejected but return 201 accepted for clients since app team has started unconditionally call
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
    if (parameters.size() == 0) {
      logger.warn(Errors.ERROR_NO_QUERY_PARAMETER);
      metrics.meter(Errors.ERROR_NO_QUERY_PARAMETER);
      return true;
    }

    // parse action from query param mkevt
    if (!parameters.containsKey(Constants.MKEVT) || parameters.get(Constants.MKEVT).get(0) == null) {
      logger.warn(Errors.ERROR_NO_MKEVT);
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
      logger.warn(Errors.ERROR_NO_MKCID);
      metrics.meter("NoMkcidParameter");
      return true;
    }

    // invalid mkcid, show error and accept
    channelType = ChannelIdEnum.parse(parameters.get(Constants.MKCID).get(0));
    if (channelType == null) {
      logger.warn(Errors.ERROR_INVALID_MKCID + " {}", uri);
      metrics.meter("InvalidMkcid");
      return true;
    }

    // check partner for email open
    String partner = null;
    if (channelAction == ChannelActionEnum.EMAIL_OPEN) {
      // no mkpid, accepted
      if (!parameters.containsKey(Constants.MKPID) || parameters.get(Constants.MKPID).get(0) == null) {
        logger.warn(Errors.ERROR_NO_MKPID);
        metrics.meter("NoMkpidParameter");
      } else {
        // invalid mkpid, accepted
        partner = EmailPartnerIdEnum.parse(parameters.get(Constants.MKPID).get(0));
        if (StringUtils.isEmpty(partner)) {
          logger.warn(Errors.ERROR_INVALID_MKPID);
          metrics.meter("InvalidMkpid");
        }
      }
    }

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = getPlatform(agentInfo);

    String action = channelAction.getAvro().toString();
    String type = channelType.getLogicalChannel().getAvro().toString();

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
        Field.of(PARTNER, partner), Field.of(PLATFORM, platform));

    // add channel specific tags, and produce message for EPN and IMK
    boolean processFlag = false;
    if (channelType == ChannelIdEnum.SITE_EMAIL)
      processFlag = processSiteEmailEvent(requestContext, endUserContext, referer, parameters, type, action, request,
          agentInfo, uri, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), false);
    else if (channelType == ChannelIdEnum.MRKT_EMAIL)
      processFlag = processMrktEmailEvent(requestContext, endUserContext, referer, parameters, type, action, request,
          agentInfo, uri, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), false);
    else
      processFlag = processAmsAndImkEvent(requestContext, uri, referer, parameters, channelType, channelAction,
          request, startTime, endUserContext, raptorSecureContext, agentInfo, false);

    // send to unified tracking topic
    // send email channels first
    if (ChannelIdEnum.SITE_EMAIL.equals(channelType) || ChannelIdEnum.MRKT_EMAIL.equals(channelType)) {
      processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, agentInfo, parameters,
          uri, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), null, 0L, 0L, startTime);
    }

    if (processFlag)
      stopTimerAndLogData(startTime, startTime, false, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
        Field.of(PARTNER, partner), Field.of(PLATFORM, platform));

    return true;
  }

  /**
   * Process ROI events
   */
  private boolean processROIEvent(ContainerRequestContext requestContext, String targetUrl, String referer,
                                  MultiValueMap<String, String> parameters, ChannelIdEnum channelType,
                                  ChannelActionEnum channelAction, HttpServletRequest request, long startTime,
                                  IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext, UserAgentInfo agentInfo, boolean isRoiFromCheckoutAPI, ROIEvent roiEvent) {

    // get user id from auth token if it's user token, else we get from end user ctx
    String userId;
    if ("EBAYUSER".equals(raptorSecureContext.getSubjectDomain())) {
      userId = raptorSecureContext.getSubjectImmutableId();
    } else {
      userId = Long.toString(endUserContext.getOrigUserOracleId());
    }

    // Parse the response
    ListenerMessage message = parser.parse(request, requestContext, startTime, -1L, channelType
        .getLogicalChannel().getAvro(), channelAction, userId, endUserContext, targetUrl, referer, 0L, "");


    BehaviorMessage behaviorMessage = behaviorMessageParser.parseAmsAndImkEvent(request, requestContext, endUserContext, parameters,
            agentInfo, targetUrl, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), message.getShortSnapshotId(), PageIdEnum.ROI.getId(),
            PageNameEnum.ROI.getName(), 0, referer, message.getGuid(), message.getCguid(), userId,
            String.valueOf(message.getDstRotationId()));
    if (behaviorMessage != null) {
      behaviorProducer.send(new ProducerRecord<>(behaviorTopic, behaviorMessage.getSnapshotId().getBytes(), behaviorMessage),
              KafkaSink.callback);
    }

    // TODO Don't write into ubi if roi is from Checkout API, but still write into IMK
    processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, agentInfo, parameters,
            targetUrl, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
            roiEvent, message.getSnapshotId(), message.getShortSnapshotId(), startTime);

    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    String kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());

    producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);
    return true;
  }

  public boolean collectNotification(HttpServletRequest request, IEndUserContext endUserContext,
                                     ContainerRequestContext requestContext, Event event, int pageId) throws Exception {

    if (request.getHeader("X-EBAY-C-TRACKING") == null) {
      logError(Errors.ERROR_NO_TRACKING);
    }

    if (request.getHeader("X-EBAY-C-ENDUSERCTX") == null) {
      logError(Errors.ERROR_NO_ENDUSERCTX);
    }

    String userAgent = endUserContext.getUserAgent();
    if (null == userAgent) {
      logError(Errors.ERROR_NO_USER_AGENT);
    }

    EventPayload payload = event.getPayload();

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = getPlatform(agentInfo);

    String type = ChannelType.NOTIFICATION.toString();
    String action = ChannelAction.NOTIFICATION.toString();

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
        Field.of(PLATFORM, platform), Field.of(PAGE_ID, pageId));

    // add tags all channels need
    addCommonTags(requestContext, "", "", agentInfo, type, action, pageId);

    // add channel specific tags
    processNotification(requestContext, payload, type, action, pageId);

    stopTimerAndLogData(startTime, startTime, false, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
          Field.of(PLATFORM, platform), Field.of(PAGE_ID, pageId));

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

    if (request.getHeader("X-EBAY-C-TRACKING") == null) {
      logError(Errors.ERROR_NO_TRACKING);
    }

    String referer = null;
    if (!StringUtils.isEmpty(event.getReferrer())) {
      referer = event.getReferrer();
    }

    String userAgent = request.getHeader("User-Agent");

    // targetUrl is from post body
    String targetUrl = event.getTargetUrl();

    // illegal url, rejected
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();

    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
    if (parameters.size() == 0) {
      logger.warn(Errors.ERROR_NO_QUERY_PARAMETER);
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
      logger.warn("Error when tracking ubi for adguid", e);
      metrics.meter("ErrorWriteAdguidToUBI");
    }

    return true;
  }

  /**
   * Collect unified tracking event and publish to kafka
   *
   * @param event               post body event
   * @return OK or Error message
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
   */
  @SuppressWarnings("unchecked")
  private void processUnifiedTrackingEvent(ContainerRequestContext requestContext, HttpServletRequest request,
                                           IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext,
                                           UserAgentInfo agentInfo, MultiValueMap<String, String> parameters, String url,
                                           String referer, ChannelType channelType, ChannelAction channelAction,
                                           ROIEvent roiEvent, long snapshotId, long shortSnapshotId, long startTime) {
    try {
      Matcher m = ebaysites.matcher(referer.toLowerCase());
      if (ChannelAction.EMAIL_OPEN.equals(channelAction) || ChannelAction.ROI.equals(channelAction) || !m.find()) {
        UnifiedTrackingMessage utpMessage = UnifiedTrackingMessageParser.parse(requestContext, request, endUserContext,
                raptorSecureContext, agentInfo, userLookup, parameters, url, referer, channelType, channelAction,
                roiEvent, snapshotId, shortSnapshotId, startTime);
        if (utpMessage != null) {
          unifiedTrackingProducer.send(new ProducerRecord<>(unifiedTrackingTopic, utpMessage.getEventId().getBytes(),
                  utpMessage), UnifiedTrackingKafkaSink.callback);
        }
      } else {
        metrics.meter("UTPInternalDomainRef", 1, Field.of(CHANNEL_ACTION, channelAction.toString()),
                Field.of(CHANNEL_TYPE, channelType.toString()));
      }
    } catch (Exception e) {
      logger.warn("UTP message process error.", e);
      metrics.meter("UTPMessageError");
    }
  }

  /**
   * Process AMS and IMK events
   */
  private boolean processAmsAndImkEvent(ContainerRequestContext requestContext, String targetUrl, String referer,
                                        MultiValueMap<String, String> parameters, ChannelIdEnum channelType,
                                        ChannelActionEnum channelAction, HttpServletRequest request, long startTime,
                                        IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext, UserAgentInfo agentInfo, boolean isDuplicateClick) {

    // logic to filter internal redirection in node, https://jirap.corp.ebay.com/browse/XC-2361
    // currently we only observe the issue in vi pool in mweb case if the url does not contain title of the item
    // log metric here about the header which identifiers if there is a redirection
    String statusCodeStr = request.getHeader(Constants.NODE_REDIRECTION_HEADER_NAME);
    if (statusCodeStr != null) {
      int statusCode;

      try {
        statusCode = Integer.valueOf(statusCodeStr);
        if (statusCode == Response.Status.OK.getStatusCode()) {
          metrics.meter("CollectStatusOK", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
        } else if (statusCode >= Response.Status.MOVED_PERMANENTLY.getStatusCode() &&
            statusCode < Response.Status.BAD_REQUEST.getStatusCode()) {
          metrics.meter("CollectStatusRedirection", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
          logger.debug("CollectStatusRedirection: URL: " + targetUrl + ", UA: " + endUserContext.getUserAgent());
        } else if (statusCode >= Response.Status.BAD_REQUEST.getStatusCode()) {
          metrics.meter("CollectStatusError", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
          logger.error("CollectStatusError: " + targetUrl);
        } else {
          metrics.meter("CollectStatusDefault", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
              Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
        }
      } catch (NumberFormatException ex) {
        metrics.meter("StatusCodeError", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
            Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
        logger.error("Error status code: " + statusCodeStr);
      }

    } else {
      metrics.meter("CollectStatusDefault", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
          Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
    }

    // parse rotation id
    long rotationId = parseRotationId(parameters);

    // parse campaign id
    long campaignId = -1L;
    try {
      campaignId = Long.parseLong(parameters.get(Constants.CAMPID).get(0));
    } catch (Exception e) {
      logger.debug("No campaign id");
    }

    // get user id from auth token if it's user token, else we get from end user ctx
    String userId;
    if ("EBAYUSER".equals(raptorSecureContext.getSubjectDomain())) {
      userId = raptorSecureContext.getSubjectImmutableId();
    } else {
      userId = Long.toString(endUserContext.getOrigUserOracleId());
    }

    // parse session id for EPN channel
    String snid = "";
    if (channelType == ChannelIdEnum.EPN) {
      snid = parseSessionId(parameters);
    }

    GdprConsentDomain gdprConsentDomain = gdprConsentHandler.handleGdprConsent(targetUrl ,channelType);
    boolean allowedStoredPersonalizedData = gdprConsentDomain.isAllowedStoredPersonalizedData();
    boolean allowedStoredContextualData = gdprConsentDomain.isAllowedStoredContextualData();

    // Parse the response
    ListenerMessage message = parser.parse(request, requestContext, startTime, campaignId, channelType
            .getLogicalChannel().getAvro(), channelAction, userId, endUserContext, targetUrl, referer, rotationId, snid);

    // Use the shot snapshot id from requests
    if (parameters.containsKey(Constants.MKRVRID) && parameters.get(Constants.MKRVRID).get(0) != null) {
      message.setShortSnapshotId(Long.valueOf(parameters.get(Constants.MKRVRID).get(0)));
    }

    if (channelType != ChannelIdEnum.EPN) {
      BehaviorMessage behaviorMessage = null;
      switch (channelAction) {
        case CLICK:
          if (!isDuplicateClick) {
            behaviorMessage = behaviorMessageParser.parseAmsAndImkEvent(request, requestContext, endUserContext, parameters,
                    agentInfo, targetUrl, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), message.getShortSnapshotId(), PageIdEnum.CLICK.getId(),
                    PageNameEnum.CLICK.getName(), 0, referer, message.getGuid(), message.getCguid(), userId,
                    String.valueOf(rotationId));
          }
          break;
        case SERVE:
          behaviorMessage = behaviorMessageParser.parseAmsAndImkEvent(request, requestContext, endUserContext, parameters,
                  agentInfo, targetUrl, startTime, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(), message.getShortSnapshotId(), PageIdEnum.AR.getId(),
                  PageNameEnum.ADREQUEST.getName(), 0, referer, message.getGuid(), message.getCguid(), userId,
                  String.valueOf(rotationId));
          break;
        default:
          break;
      }
      if (behaviorMessage != null) {
        behaviorProducer.send(new ProducerRecord<>(behaviorTopic, behaviorMessage.getSnapshotId().getBytes(), behaviorMessage),
                KafkaSink.callback);
      }
    }

    if (channelType != ChannelIdEnum.EPN && !isDuplicateClick) {
      processUnifiedTrackingEvent(requestContext, request, endUserContext, raptorSecureContext, agentInfo, parameters,
              targetUrl, referer, channelType.getLogicalChannel().getAvro(), channelAction.getAvro(),
              null, message.getSnapshotId(), message.getShortSnapshotId(), startTime);
    }

    // Tracking ubi only when refer domain is not ebay. This should be moved to filter later.
    // Don't track ubi if it's AR
    // Don't track ubi if the click is from Checkout API
    // Don't track ubi if the click is a duplicate itm click
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find() && !channelAction.equals(ChannelActionEnum.SERVE) && !isClickFromCheckoutAPI(channelType.getLogicalChannel().getAvro(), endUserContext)
            && !isDuplicateClick) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // event family
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, "mkt", String.class);

        // rotation id
        requestTracker.addTag("rotid", String.valueOf(rotationId), String.class);

        // keyword
        String searchKeyword = "";
        if (parameters.containsKey(Constants.SEARCH_KEYWORD) && parameters.get(Constants.SEARCH_KEYWORD).get(0) != null) {

          searchKeyword = parameters.get(Constants.SEARCH_KEYWORD).get(0);
        }
        requestTracker.addTag("keyword", searchKeyword, String.class);

        // rvr id
        requestTracker.addTag("rvrid", message.getShortSnapshotId(), Long.class);

        // gclid
        String gclid = "";
        if (parameters.containsKey(Constants.GCLID) && parameters.get(Constants.GCLID).get(0) != null) {

          gclid = parameters.get(Constants.GCLID).get(0);
        }
        requestTracker.addTag("gclid", gclid, String.class);

        //producereventts
        requestTracker.addTag("producereventts", startTime, Long.class);

      } catch (Exception e) {
        logger.warn("Error when tracking ubi for imk", e);
        metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
            Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
      }
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, channelAction.getAvro().toString()),
          Field.of(CHANNEL_TYPE, channelType.getLogicalChannel().getAvro().toString()));
    }

    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    String kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());

    if (message != null) {
      if (!allowedStoredContextualData) {
        message.setRemoteIp("");
        message.setUserAgent("");
        message.setGeoId(0L);
        message.setUdid("");
        message.setLangCd("");
        message.setReferer("");
        message.setRequestHeaders("");
      }
      if (!allowedStoredPersonalizedData) {
        message.setUserId(0L);
        message.setGuid(CommonConstant.EMPTY_GUID);
        message.setCguid(CommonConstant.EMPTY_GUID);
      }

      // If the click is a duplicate click from itm page, then drop into duplicateItmClickTopic
      // else drop into normal topic
      if (isDuplicateClick) {
        sendClickToDuplicateItmClickTopic(producer, message);
      } else {
        producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);
      }

      return true;
    } else
      return false;
  }

  /**
   * Process site email event
   */
  private boolean processSiteEmailEvent(ContainerRequestContext requestContext, IEndUserContext endUserContext,
                                        String referer, MultiValueMap<String, String> parameters, String type,
                                        String action, HttpServletRequest request, UserAgentInfo agentInfo, String uri,
                                        Long startTime, ChannelType channelType, ChannelAction channelAction, boolean isDuplicateClick) {

    // Tracking ubi only when refer domain is not ebay.
    Matcher m = ebaysites.matcher(referer.toLowerCase());

    // Email open should not be filtered by referer
    if(ChannelAction.EMAIL_OPEN.equals(channelAction) || !m.find()) {
      Long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId()).getRepresentation();

      // send click and open event to message tracker
      eventEmitterPublisher.publishEvent(requestContext, parameters, uri, channelType, channelAction, snapshotId);

      // send click event to ubi
      // Don't track ubi if the click is a duplicate itm click
      if (ChannelAction.CLICK.equals(channelAction) && !isDuplicateClick) {
        try {
          // Ubi tracking
          IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

          // event family
          requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, Constants.EVENT_FAMILY_CRM, String.class);

          // fbprefetch
          if (isFacebookPrefetchEnabled(request))
            requestTracker.addTag("fbprefetch", true, Boolean.class);

          // channel id
          addTagFromUrlQuery(parameters, requestTracker, Constants.MKCID, "chnl", String.class);

          // source id
          addTagFromUrlQuery(parameters, requestTracker, Constants.SOURCE_ID, "emsid", String.class);

          // email unique id
          addTagFromUrlQuery(parameters, requestTracker, Constants.EMAIL_UNIQUE_ID, "euid", String.class);

          // email experienced treatment
          addTagFromUrlQuery(parameters, requestTracker, Constants.EXPRCD_TRTMT, "ext", String.class);

        } catch (Exception e) {
          logger.warn("Error when tracking ubi for site email click tags", e);
          metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
        }
      }

      // send email open/click to behavior topic
      BehaviorMessage message = behaviorMessageParser.parse(request, requestContext, endUserContext, parameters,
          agentInfo, referer, uri, startTime, channelType, channelAction, snapshotId, 0);

      if (message != null) {
        // If the click is a duplicate click from itm page, then drop into duplicateItmClickTopic
        // else drop into normal topic
        if (isDuplicateClick) {
          Producer<Long, ListenerMessage> producer = KafkaSink.get();
          ListenerMessage listenerMessage = parser.parse(request, requestContext, startTime, 0L, channelType
                  , ChannelActionEnum.CLICK, message.getUserId(), endUserContext, uri, referer, 0L, "");
          listenerMessage.setSnapshotId(snapshotId);
          listenerMessage.setShortSnapshotId(0L);
          sendClickToDuplicateItmClickTopic(producer, listenerMessage);
        } else {
          behaviorProducer.send(new ProducerRecord<>(behaviorTopic, message.getSnapshotId().getBytes(), message),
                  KafkaSink.callback);
        }

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
   * Process marketing email event
   */
  private boolean processMrktEmailEvent(ContainerRequestContext requestContext, IEndUserContext endUserContext,
                                        String referer, MultiValueMap<String, String> parameters, String type,
                                        String action, HttpServletRequest request, UserAgentInfo agentInfo, String uri,
                                        Long startTime, ChannelType channelType, ChannelAction channelAction, boolean isDuplicateClick) {

    // Tracking ubi only when refer domain is not ebay.
    Matcher m = ebaysites.matcher(referer.toLowerCase());

    // Email open should not be filtered by referer
    if(ChannelAction.EMAIL_OPEN.equals(channelAction) || !m.find()) {
      Long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId()).getRepresentation();

      // send click and open event to message tracker
      eventEmitterPublisher.publishEvent(requestContext, parameters, uri, channelType, channelAction, snapshotId);

      // send click event to ubi
      // Don't track ubi if the click is a duplicate itm click
      if (ChannelAction.CLICK.equals(channelAction) && !isDuplicateClick) {
        try {
          // Ubi tracking
          IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

          // event family
          requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, Constants.EVENT_FAMILY_CRM, String.class);

          // fbprefetch
          if (isFacebookPrefetchEnabled(request))
            requestTracker.addTag("fbprefetch", true, Boolean.class);

          // channel id
          addTagFromUrlQuery(parameters, requestTracker, Constants.MKCID, "chnl", String.class);

          // source id
          addTagFromUrlQuery(parameters, requestTracker, Constants.SOURCE_ID, "emsid", String.class);

          // email id
          addTagFromUrlQuery(parameters, requestTracker, Constants.BEST_GUESS_USER, "emid", String.class);

          // campaign run date
          addTagFromUrlQuery(parameters, requestTracker, Constants.CAMP_RUN_DT, "crd", String.class);

          // segment name
          addTagFromUrlQuery(parameters, requestTracker, Constants.SEGMENT_NAME, "segname", String.class);

          // Yesmail message master id
          addTagFromUrlQuery(parameters, requestTracker, Constants.YM_MSSG_MSTR_ID, "ymmmid", String.class);

          // YesMail message id
          addTagFromUrlQuery(parameters, requestTracker, Constants.YM_MSSG_ID, "ymsid", String.class);

          // Yesmail mailing instance
          addTagFromUrlQuery(parameters, requestTracker, Constants.YM_INSTC, "yminstc", String.class);

          // Adobe email redirect url
          if (parameters.containsKey(Constants.REDIRECT_URL_SOJ_TAG)
              && parameters.get(Constants.REDIRECT_URL_SOJ_TAG).get(0) != null) {
            requestTracker.addTag("adcamp_landingpage",
                URLDecoder.decode(parameters.get(Constants.REDIRECT_URL_SOJ_TAG).get(0), "UTF-8"), String.class);
          }

          // Adobe email redirect source
          addTagFromUrlQuery(parameters, requestTracker, Constants.REDIRECT_SRC_SOJ_SOURCE, "adcamp_locationsrc",
              String.class);

          //Adobe campaign public user id
          addTagFromUrlQuery(parameters, requestTracker, Constants.ADOBE_CAMP_PUBLIC_USER_ID, "adcamppu", String.class);

        } catch (Exception e) {
          logger.warn("Error when tracking ubi for marketing email click tags", e);
          metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
        }
      }

      // send email open/click to chocolate topic
      BehaviorMessage message = behaviorMessageParser.parse(request, requestContext, endUserContext, parameters,
          agentInfo, referer, uri, startTime, channelType, channelAction, snapshotId, 0);

      if (message != null) {
        // If the click is a duplicate click from itm page, then drop into duplicateItmClickTopic
        // else drop into normal topic
        if (isDuplicateClick) {
          Producer<Long, ListenerMessage> producer = KafkaSink.get();
          ListenerMessage listenerMessage = parser.parse(request, requestContext, startTime, 0L, channelType
                  , ChannelActionEnum.CLICK, message.getUserId(), endUserContext, uri, referer, 0L, "");
          listenerMessage.setSnapshotId(snapshotId);
          listenerMessage.setShortSnapshotId(0L);
          sendClickToDuplicateItmClickTopic(producer, listenerMessage);
        } else {
          behaviorProducer.send(new ProducerRecord<>(behaviorTopic, message.getSnapshotId().getBytes(), message),
                  KafkaSink.callback);
        }

        return true;
      } else
        return false;
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }

    return true;
  }

  /**
   * Process SMS event
   */
  private boolean processSMSEvent(ContainerRequestContext requestContext, String referer,
                                        MultiValueMap<String, String> parameters, String type, String action, boolean isDuplicateClick) {

    // Tracking ubi only when refer domain is not ebay.
    // Don't track ubi if the click is a duplicate itm click
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find() && !isDuplicateClick) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // event family
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, Constants.EVENT_FAMILY_CRM, String.class);

        // sms unique id
        addTagFromUrlQuery(parameters, requestTracker, Constants.SMS_ID, "smsid", String.class);

      } catch (Exception e) {
        logger.warn("Error when tracking ubi for sms click tags", e);
        metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
      }
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }

    return true;
  }

  /**
   * Process mobile notification event
   */
  private void processNotification(ContainerRequestContext requestContext, EventPayload payload,
                                   String type, String action, int pageId) {
    try {
      // Ubi tracking
      IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext
          .getProperty(IRequestScopeTracker.NAME);

      // event family
      requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, Constants.EVENT_FAMILY_CRM, String.class);

      Map<String, String> tags = payload.getTags();

      // notification id
      addTagFromPayload(tags, requestTracker, Constants.NOTIFICATION_ID);

      // notification type
      addTagFromPayload(tags, requestTracker, Constants.NOTIFICATION_TYPE);

      // notification action
      addTagFromPayload(tags, requestTracker, Constants.NOTIFICATION_ACTION);

      // user name
      addTagFromPayload(tags, requestTracker, Constants.USER_NAME);

      // mc3 canonical message id
      addTagFromPayload(tags, requestTracker, Constants.MC3_MSSG_ID);

      // item id
      addTagFromPayload(tags, requestTracker, Constants.ITEM_ID);

      // notification type evt
      addTagFromPayload(tags, requestTracker, Constants.NOTIFICATION_TYPE_EVT);

    } catch (Exception e) {
      logger.warn("Error when tracking ubi for mobile notification tags", e);
      metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
          Field.of(PAGE_ID, pageId));
    }

  }

  /**
   * Add common tags all channels need
   */
  private void addCommonTags(ContainerRequestContext requestContext, String targetUrl, String referer,
                             UserAgentInfo agentInfo, String type, String action, int pageId) {
    // Tracking ubi only when refer domain is not ebay.
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find()) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

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

        // populate device info
        CollectionServiceUtil.populateDeviceDetectionParams(agentInfo, requestTracker);

      } catch (Exception e) {
        logger.warn("Error when tracking ubi for common tags", e);
        metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
      }
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }
  }

  private void addGenericSojTags(ContainerRequestContext requestContext, MultiValueMap<String, String> parameters,
                                 String referer, String type, String action) {
    // Tracking ubi only when refer domain is not ebay.
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find()) {
      // Ubi tracking
      IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

      String sojTags = parameters.get(Constants.SOJ_TAGS).get(0);
      try {
        sojTags = URLDecoder.decode(sojTags, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        logger.warn("Param sojTags is wrongly encoded", e);
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
            logger.warn("Error when tracking ubi for common tags", e);
            metrics.meter("ErrorTrackUbi", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
          }
        }
      }
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }
  }

  private void addRoiSojTags(ContainerRequestContext requestContext, Map<String, String> payloadMap, ROIEvent roiEvent,
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
      logger.warn("Error when tracking ubi for roi event", e);
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
    logger.warn(error);
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
    logger.debug(String.format("StartTime: %d", startTime));
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
  private void stopTimerAndLogData(long eventProcessStartTime, long eventProducerStartTime, boolean checkoutAPIClickFlag, Field<String, Object>... additionalFields) {
    long endTime = System.currentTimeMillis();
    logger.debug(String.format("EndTime: %d", endTime));
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
   * Parse rotation id from query mkrid
   */
  private long parseRotationId(MultiValueMap<String, String> parameters) {
    long rotationId = -1L;
    if (parameters.containsKey(Constants.MKRID) && parameters.get(Constants.MKRID).get(0) != null) {
      try {
        String rawRotationId = parameters.get(Constants.MKRID).get(0);
        // decode rotationId if rotation is encoded
        // add decodeCnt to avoid looping infinitely
        int decodeCnt = 0;
        while (rawRotationId.contains("%") && decodeCnt < 5) {
          rawRotationId = URLDecoder.decode(rawRotationId, UTF_8);
          decodeCnt = decodeCnt + 1;
        }
        rotationId = Long.parseLong(rawRotationId.replaceAll("-", ""));
      } catch (Exception e) {
        logger.warn(Errors.ERROR_INVALID_MKRID);
        metrics.meter("InvalidMkrid");
      }
    } else {
      logger.warn(Errors.ERROR_NO_MKRID);
      metrics.meter("NoMkrid");
    }

    return rotationId;
  }

  /**
   * Soj tag fbprefetch
   */
  private static boolean isFacebookPrefetchEnabled(HttpServletRequest request) {
    String facebookprefetch = request.getHeader("X-Purpose");
    return facebookprefetch != null && facebookprefetch.trim().equals("preview");
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
   * Parse tag from payload map and add to sojourner
   * @param tags  the name value pair of tags
   * @param tag     the tag name
   */
  private static void addTagFromPayload(Map<String, String> tags, IRequestScopeTracker requestTracker, String tag) {
    if (tags.containsKey(tag) && !StringUtils.isEmpty(tags.get(tag))) {
      requestTracker.addTag(tag, tags.get(tag), String.class);
    }
  }

  /**
   * Parse session id from query mksid for epn channel
   */
  private String parseSessionId(MultiValueMap<String, String> parameters) {
    String sessionId = "";
    if (parameters.containsKey(Constants.MKSID) && parameters.get(Constants.MKSID).get(0) != null) {
      try {
        sessionId = parameters.get(Constants.MKSID).get(0);
      } catch (Exception e) {
        logger.warn(Errors.ERROR_INVALID_MKSID);
        metrics.meter("InvalidMksid");
      }
    } else {
      logger.warn(Errors.ERROR_NO_MKSID);
      metrics.meter("NoMksid");
    }

    return sessionId;
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
      logger.error("Determine whether the click from Checkout API error");
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
      logger.error("Determine whether the roi from Checkout API error");
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
