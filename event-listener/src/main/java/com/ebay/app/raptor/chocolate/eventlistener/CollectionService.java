package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.ApplicationOptionsParser;
import com.ebay.app.raptor.chocolate.common.Hostname;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.util.*;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.gen.model.EventPayload;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.kernel.presentation.constants.PresentationConstants;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.tracking.util.TrackerTagValueUtil;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.HttpContext;
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
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Response;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ebay.app.raptor.chocolate.eventlistener.constant.Constants.REFERRER;
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
  private static CollectionService instance = null;

  @Autowired
  private HttpRoverClient roverClient;

  @Autowired
  private HttpClientConnectionManager httpClientConnectionManager;

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

  // do not filter /ulk XC-1541
  private static Pattern ebaysites = Pattern.compile("^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)", Pattern.CASE_INSENSITIVE);
  private static Pattern roversites = Pattern.compile("^(http[s]?:\\/\\/)?rover\\.(qa\\.)?ebay\\.[\\w-.]+(\\/.*)", Pattern.CASE_INSENSITIVE);

  // app deeplink sites XC-1797
  private static Pattern deeplinksites = Pattern.compile("^ebay:\\/\\/link\\/([\\w-$%?&/.])?", Pattern.CASE_INSENSITIVE);
  // determine whether the url belongs to ebay sites for app deep link, and don't do any filter
  private static Pattern deeplinkEbaySites = Pattern.compile("^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/.*)", Pattern.CASE_INSENSITIVE);

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
    this.parser = ListenerMessageParser.getInstance();
    this.metrics.meter("driver.id", 1, Field.of("ip", Hostname.IP),
            Field.of("driver_id", ApplicationOptionsParser.getDriverIdFromIp()));
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

      URIBuilder uriBuilder = new URIBuilder(referer);
      List<NameValuePair> queryParameters = uriBuilder.getQueryParams();
      Set<String> queryNames = new HashSet<>();
      for (Iterator<NameValuePair> queryParameterItr = queryParameters.iterator(); queryParameterItr.hasNext();) {
        NameValuePair queryParameter = queryParameterItr.next();
        //remove mpre if necessary. When there is mpre, rover won't overwrite guid by udid
        if (queryParameter.getName().equals("mpre")) {
          queryParameterItr.remove();
        }
        queryNames.add(queryParameter.getName());
      }
      uriBuilder.setParameters(queryParameters);

      String guid = "";
      String cguid = "";
      String trackingHeader = request.getHeader("X-EBAY-C-TRACKING");
      for (String seg : trackingHeader.split(",")) {
        String[] keyValue = seg.split("=");
        if (keyValue.length == 2) {
          if (keyValue[0].equalsIgnoreCase("guid")) {
            guid = keyValue[1];
          }
          if (keyValue[0].equalsIgnoreCase("cguid")) {
            cguid = keyValue[1];
          }
        }
      }
      // add udid parameter from tracking header's guid if udid is not in rover url. The udid will be set as guid by rover later
      if (!queryNames.contains("udid")) {
        if (!guid.isEmpty()) {
          uriBuilder.addParameter("udid", guid);
        }
      }

      // add nrd=1 if not exist
      if(!queryNames.contains("nrd")) {
        uriBuilder.addParameter("nrd", "1");
      }

      // add mcs=1 for marking mcs forwarding
      uriBuilder.addParameter("mcs", "1");

      final String rebuiltRoverUrl = uriBuilder.build().toString();

      CloseableHttpClient client = httpClientConnectionManager.getHttpClient();
      HttpContext context = HttpClientContext.create();
      HttpGet httpGet = new HttpGet(rebuiltRoverUrl);

      final Enumeration<String> headers = request.getHeaderNames();
      while (headers.hasMoreElements()) {
        final String header = headers.nextElement();
        if (header.equalsIgnoreCase("x-forwarded-for") ||
            header.equalsIgnoreCase("user-agent") ) {
          final Enumeration<String> values = request.getHeaders(header);
          //just pass one header value to rover. Multiple value will cause parse exception on [] brackets.
          httpGet.addHeader(header, values.nextElement());
        }
      }

      // add guid and cguid in request cookie header
      if (!guid.isEmpty() || !cguid.isEmpty()) {
        String cookie = "npii=";
        String timestamp = generateTimestampForCookie();
        if (!guid.isEmpty())
          cookie += "btguid/" + guid + timestamp + "^";
        if (!cguid.isEmpty())
          cookie += "cguid/" + cguid + timestamp + "^";
        httpGet.addHeader("Cookie", cookie);
      }

      roverClient.forwardRequestToRover(client, httpGet, context);
      return true;
    }

    ChannelIdEnum channelType;
    ChannelActionEnum channelAction = ChannelActionEnum.CLICK;

    // targetUrl is from post body
    String targetUrl = event.getTargetUrl();

    //XC-1797, for social app deeplink case, extract and decode actual target url from referrer parameter in targetUrl
    //only accept the url when referrer domain belongs to ebay sites
    Matcher deeplinkMatcher = deeplinksites.matcher(targetUrl.toLowerCase());
    if (deeplinkMatcher.find()) {
      metrics.meter("IncomingSocialAppDeepLink");

      UriComponents deeplinkUriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();
      if (deeplinkUriComponents == null) {
        logError(Errors.ERROR_ILLEGAL_URL);
      }

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
    // illegal url, rejected
    UriComponents uriComponents;
    uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();
    if (uriComponents == null) {
      logError(Errors.ERROR_ILLEGAL_URL);
    }

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
      logger.warn(Errors.ERROR_INVALID_MKCID);
      metrics.meter("InvalidMkcid");
      return true;
    }

    // check partner for email click
    String partner = null;
    if (ChannelIdEnum.SITE_EMAIL.equals(channelType) || ChannelIdEnum.MRKT_EMAIL.equals(channelType)) {
      // no mkpid, accepted
      if (!parameters.containsKey(Constants.MKPID) || parameters.get(Constants.MKPID).get(0) == null) {
        logger.warn(Errors.ERROR_NO_MKPID);
        metrics.meter("NoMkpidParameter");
      }

      // invalid mkpid, accepted
      partner = EmailPartnerIdEnum.parse(parameters.get(Constants.MKPID).get(0));
      if (partner == null) {
        logger.warn(Errors.ERROR_INVALID_MKPID);
        metrics.meter("InvalidMkpid");
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

    // Self-service events, sent to specific topic
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

    // add tags in url param "sojTags"
    if(parameters.containsKey(Constants.SOJ_TAGS) && parameters.get(Constants.SOJ_TAGS).get(0) != null) {
      addGenericSojTags(requestContext, parameters, referer, type, action);
    }

    // add tags all channels need
    addCommonTags(requestContext, targetUrl, referer, agentInfo, type, action, PageIdEnum.CLICK.getId());

    // add channel specific tags, and produce message for EPN and IMK
    boolean processFlag = false;
    if (channelType == ChannelIdEnum.EPN || channelType == ChannelIdEnum.PAID_SEARCH || channelType == ChannelIdEnum.DAP ||
        channelType == ChannelIdEnum.SOCIAL_MEDIA)
      processFlag = processAmsAndImkEvent(requestContext, targetUrl, referer, parameters, channelType, channelAction,
          request, startTime, endUserContext, raptorSecureContext);
    else if (channelType == ChannelIdEnum.SITE_EMAIL)
      processFlag = processSiteEmailEvent(requestContext, referer, parameters, type, action, request);
    else if (channelType == ChannelIdEnum.MRKT_EMAIL)
      processFlag = processMrktEmailEvent(requestContext, referer, parameters, type, action, request);
    else if (channelType == ChannelIdEnum.MRKT_SMS || channelType == ChannelIdEnum.SITE_SMS)
      processFlag = processSMSEvent(requestContext, referer, parameters, type, action);
    if (processFlag)
      stopTimerAndLogData(startTime, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
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
    try {
      long transTimestamp = Long.valueOf(roiEvent.getTransactionTimestamp());
      if(transTimestamp < 0)
        roiEvent.setTransactionTimestamp(localTimestamp);
    } catch (Exception e) {
      logger.warn("Error timestamp " + roiEvent.getTransactionTimestamp());
      metrics.meter("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));
      roiEvent.setTransactionTimestamp(localTimestamp);
    }
    // Parse payload fields
    Map<String, String> payloadMap = roiEvent.getPayload();
    if(payloadMap == null) {
      payloadMap = new HashMap<String, String>();
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
    UriComponents uriComponents;
    uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();
    if (uriComponents == null) {
      logError(Errors.ERROR_ILLEGAL_URL);
    }
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
    addRoiSojTags(requestContext, payloadMap, roiEvent, userId);

    // Write roi event to kafka output topic
    boolean processFlag = processROIEvent(requestContext, targetUrl, referer, parameters, ChannelIdEnum.ROI,
        ChannelActionEnum.ROI, request, startTime, endUserContext, raptorSecureContext);

    if (processFlag) {
      metrics.meter("NewROICountAPI", 1, Field.of(CHANNEL_ACTION, "New-ROI"),
          Field.of(CHANNEL_TYPE, "New-ROI"), Field.of(ROI_SOURCE, String.valueOf(payloadMap.get(ROI_SOURCE))));
      stopTimerAndLogData(startTime, Field.of(CHANNEL_ACTION, ChannelActionEnum.ROI.toString()), Field.of(CHANNEL_TYPE,
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

    UriComponents uriComponents;
    uriComponents = UriComponentsBuilder.fromUriString(uri).build();
    if (uriComponents == null) {
      logger.warn(Errors.ERROR_ILLEGAL_URL);
      metrics.meter(Errors.ERROR_ILLEGAL_URL);
    }

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
      logger.warn(Errors.ERROR_INVALID_MKCID);
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
      }

      // invalid mkpid, accepted
      partner = EmailPartnerIdEnum.parse(parameters.get(Constants.MKPID).get(0));
      if (partner == null) {
        logger.warn(Errors.ERROR_INVALID_MKPID);
        metrics.meter("InvalidMkpid");
      }
    }

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext.getProperty(UserAgentInfo.NAME);
    String platform = getPlatform(agentInfo);

    String action = channelAction.getAvro().toString();
    String type = channelType.getLogicalChannel().getAvro().toString();

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
        Field.of(PARTNER, partner), Field.of(PLATFORM, platform));

    // add tags in url param "sojTags"
    if(parameters.containsKey(Constants.SOJ_TAGS) && parameters.get(Constants.SOJ_TAGS).get(0) != null) {
      addGenericSojTags(requestContext, parameters, referer, type, action);
    }

    // add tags all channels need
    if (channelAction == ChannelActionEnum.SERVE) {
      addCommonTags(requestContext, uri, referer, agentInfo, type, action, PageIdEnum.AR.getId());
    } else if (channelAction == ChannelActionEnum.IMPRESSION) {
      // impression and ar share same page id (adservice page id)
      addCommonTags(requestContext, uri, referer, agentInfo, type, action, PageIdEnum.AR.getId());
    } else {
      addCommonTags(requestContext, null, referer, agentInfo, type, action, PageIdEnum.EMAIL_OPEN.getId());
    }

    // add channel specific tags, and produce message for EPN and IMK
    boolean processFlag = false;
    if (channelType == ChannelIdEnum.SITE_EMAIL)
      processFlag = processSiteEmailEvent(requestContext, referer, parameters, type, action, request);
    else if (channelType == ChannelIdEnum.MRKT_EMAIL)
      processFlag = processMrktEmailEvent(requestContext, referer, parameters, type, action, request);
    else
      processFlag = processAmsAndImkEvent(requestContext, uri, referer, parameters, channelType, channelAction,
          request, startTime, endUserContext, raptorSecureContext);

    if (processFlag)
      stopTimerAndLogData(startTime, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
        Field.of(PARTNER, partner), Field.of(PLATFORM, platform));

    return true;
  }

  /**
   * Process ROI events
   */
  private boolean processROIEvent(ContainerRequestContext requestContext, String targetUrl, String referer,
                                        MultiValueMap<String, String> parameters, ChannelIdEnum channelType,
                                        ChannelActionEnum channelAction, HttpServletRequest request, long startTime,
                                        IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext) {

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

    // Use the shot snapshot id from requests
    if (parameters.containsKey(Constants.MKRVRID) && parameters.get(Constants.MKRVRID).get(0) != null) {
      message.setShortSnapshotId(Long.valueOf(parameters.get(Constants.MKRVRID).get(0)));
    }

    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    String kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());

    if (message != null) {
      producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);
      return true;
    } else {
      return false;
    }
  }


  public boolean collectNotification(HttpServletRequest request, IEndUserContext endUserContext,
                                     ContainerRequestContext requestContext, Event event) throws Exception {

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

    // no page id, reject
    EventPayload payload = event.getPayload();
    if (payload.getPageId() == null) {
      logError(Errors.ERROR_NO_PAGE_ID);
    }
    // get page id from payload
    int pageId = payload.getPageId();

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

    stopTimerAndLogData(startTime, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
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
    UriComponents uriComponents;
    uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();
    if (uriComponents == null) {
      logError(Errors.ERROR_ILLEGAL_URL);
    }

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
      requestTracker.addTag(TrackerTagValueUtil.EventActionTag, "mktc", String.class);

      // target url
      if (!StringUtils.isEmpty(targetUrl)) {
        requestTracker.addTag("url_mpre", targetUrl, String.class);
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
   * Process AMS and IMK events
   */
  private boolean processAmsAndImkEvent(ContainerRequestContext requestContext, String targetUrl, String referer,
                                        MultiValueMap<String, String> parameters, ChannelIdEnum channelType,
                                        ChannelActionEnum channelAction, HttpServletRequest request, long startTime,
                                        IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext) {

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

    // Parse the response
    ListenerMessage message = parser.parse(request, requestContext, startTime, campaignId, channelType
            .getLogicalChannel().getAvro(), channelAction, userId, endUserContext, targetUrl, referer, rotationId, snid);

    // Use the shot snapshot id from requests
    if (parameters.containsKey(Constants.MKRVRID) && parameters.get(Constants.MKRVRID).get(0) != null) {
      message.setShortSnapshotId(Long.valueOf(parameters.get(Constants.MKRVRID).get(0)));
    }

    // Tracking ubi only when refer domain is not ebay. This should be moved to filter later.
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find()) {
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
      producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);
      return true;
    } else
      return false;
  }

  /**
   * Process site email event
   */
  private boolean processSiteEmailEvent(ContainerRequestContext requestContext, String referer,
                                        MultiValueMap<String, String> parameters, String type, String action,
                                        HttpServletRequest request) {

    // Tracking ubi only when refer domain is not ebay.
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find()) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // event family
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, "mktcrm", String.class);

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
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }

    return true;
  }

  /**
   * Process marketing email event
   */
  private boolean processMrktEmailEvent(ContainerRequestContext requestContext, String referer,
                                        MultiValueMap<String, String> parameters, String type, String action,
                                        HttpServletRequest request) {

    // Tracking ubi only when refer domain is not ebay.
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find()) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // event family
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, "mktcrm", String.class);

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
    } else {
      metrics.meter("InternalDomainRef", 1, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type));
    }

    return true;
  }

  /**
   * Process SMS event
   */
  private boolean processSMSEvent(ContainerRequestContext requestContext, String referer,
                                        MultiValueMap<String, String> parameters, String type, String action) {

    // Tracking ubi only when refer domain is not ebay.
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(!m.find()) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

        // event family
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, "mktcrm", String.class);

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
      requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, "mktcrm", String.class);

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
        requestTracker.addTag(TrackerTagValueUtil.EventActionTag, "mktc", String.class);

        // target url
        if (!StringUtils.isEmpty(targetUrl)) {
          requestTracker.addTag("url_mpre", targetUrl, String.class);
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
    } catch (Exception e) {
      logger.warn("Error when tracking ubi for roi event", e);
      metrics.meter("ErrorWriteRoiEventToUBI");
    }
  }


  private String generateTimestampForCookie() {
    LocalDateTime now = LocalDateTime.now();

    // GUID, CGUID has 2 years expiration time
    LocalDateTime expiration = now.plusYears(2);

    // the last 8 hex number is the unix timestamp in seconds
    long timeInSeconds = expiration.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() / 1000;
    return Long.toHexString(timeInSeconds);
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
   * @param startTime        the start time, so that latency can be calculated
   * @param additionalFields channelAction, channelType, platform, landing page type
   */
  private void stopTimerAndLogData(long startTime, Field<String, Object>... additionalFields) {
    long endTime = System.currentTimeMillis();
    logger.debug(String.format("EndTime: %d", endTime));
    metrics.meter("CollectionServiceSuccess", 1, startTime, additionalFields);
    metrics.mean("CollectionServiceAverageLatency", endTime - startTime);
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
        while (rawRotationId.contains("%") && decodeCnt<5) {
          rawRotationId = URLDecoder.decode(rawRotationId, UTF_8);
          decodeCnt = decodeCnt + 1;
        }
        rotationId = Long.valueOf(rawRotationId.replaceAll("-", ""));
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
    if (facebookprefetch != null && facebookprefetch.trim().equals("preview")) {
      return true;
    }
    return false;
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

}