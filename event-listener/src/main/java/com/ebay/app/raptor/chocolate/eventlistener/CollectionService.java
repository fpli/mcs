package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.util.*;
import com.ebay.app.raptor.chocolate.gen.model.Event;
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
import java.net.URLDecoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
  private static final String PLATFORM = "platform";
  private static final String LANDING_PAGE_TYPE = "landingPageType";
  // do not filter /ulk XC-1541
  private static Pattern ebaysites = Pattern.compile("^(http[s]?:\\/\\/)?(?!rover)([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)", Pattern.CASE_INSENSITIVE);
  private static Pattern roversites = Pattern.compile("^(http[s]?:\\/\\/)?(rover\\.)?ebay\\.[\\w-.]+(\\/.*)", Pattern.CASE_INSENSITIVE);

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
    this.parser = ListenerMessageParser.getInstance();

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

    // TODO: return 201 for now for the no referer case. Need investigation further.
    if (StringUtils.isEmpty(referer) || referer.equalsIgnoreCase("null")) {
      //logError(ErrorType.NO_REFERER);
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

      // add udid parameter from tracking header's guid if udid is not in rover url. The udid will be set as guid by rover later
      if (!queryNames.contains("udid")) {
        String trackingHeader = request.getHeader("X-EBAY-C-TRACKING");
        String guid = "";
        for (String seg : trackingHeader.split(",")
          ) {
          String[] keyValue = seg.split("=");
          if (keyValue.length == 2) {
            if (keyValue[0].equalsIgnoreCase("guid")) {
              guid = keyValue[1];
            }
          }
        }
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
      
      roverClient.forwardRequestToRover(client, httpGet, context);
      return true;
    }

    ChannelIdEnum channelType;
    ChannelActionEnum channelAction = ChannelActionEnum.CLICK;

    // targetUrl is from post body
    String targetUrl = event.getTargetUrl();

    // parse channel from uri
    // illegal url, rejected
    UriComponents uriComponents;
    uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();
    if (uriComponents == null) {
      logError(Errors.ERROR_ILLEGAL_URL);
    }

    // no query parameter, rejected
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
    if (parameters.size() == 0) {
      logError(Errors.ERROR_NO_QUERY_PARAMETER);
    }

    // no mkevt, rejected
    if (!parameters.containsKey(Constants.MKEVT) || parameters.get(Constants.MKEVT).get(0) == null) {
      logError(Errors.ERROR_NO_MKEVT);
    }

    // mkevt != 1, rejected
    String mkevt = parameters.get(Constants.MKEVT).get(0);
    if (!mkevt.equals(Constants.VALID_MKEVT)) {
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

    String landingPageType;
    List<String> pathSegments = uriComponents.getPathSegments();
    if (pathSegments == null || pathSegments.size() == 0) {
      landingPageType = "home";
    } else {
      landingPageType = pathSegments.get(0);
    }

    // platform check by user agent
    UserAgentInfo agentInfo = (UserAgentInfo) requestContext
            .getProperty(UserAgentInfo.NAME);
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

    String action = ChannelActionEnum.CLICK.toString();
    String type = channelType.getLogicalChannel().getAvro().toString();

    long startTime = startTimerAndLogData(Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
        Field.of(PLATFORM, platform), Field.of(LANDING_PAGE_TYPE, landingPageType));

    boolean processFlag = false;
    if (channelType == ChannelIdEnum.PAID_SEARCH || channelType == ChannelIdEnum.DAP ||
        channelType == ChannelIdEnum.SOCIAL_MEDIA)
      processFlag = processImkEvent(requestContext, targetUrl, referer, agentInfo, parameters, channelType, channelAction,
          request, startTime, endUserContext, raptorSecureContext);

    if (processFlag)
      stopTimerAndLogData(startTime, Field.of(CHANNEL_ACTION, action), Field.of(CHANNEL_TYPE, type),
          Field.of(PLATFORM, platform), Field.of(LANDING_PAGE_TYPE, landingPageType));

    return true;
  }

  /**
   * Process IMK events
   */
  private boolean processImkEvent(ContainerRequestContext requestContext, String targetUrl, String referer,
                                  UserAgentInfo agentInfo, MultiValueMap<String, String> parameters,
                                  ChannelIdEnum channelType, ChannelActionEnum channelAction, HttpServletRequest request,
                                  long startTime, IEndUserContext endUserContext, RaptorSecureContext raptorSecureContext) {

    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    String kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());

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

    // Parse the response
    ListenerMessage message = parser.parse(request, requestContext, startTime, campaignId, channelType
            .getLogicalChannel().getAvro(), channelAction, userId, endUserContext, targetUrl, referer, rotationId, null);

    // Tracking ubi only when refer domain is not ebay. This should be moved to filter later.
    Matcher m = ebaysites.matcher(referer.toLowerCase());
    if(m.find() == false) {
      try {
        // Ubi tracking
        IRequestScopeTracker requestTracker = (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker
                .NAME);

        // page id
        requestTracker.addTag(TrackerTagValueUtil.PageIdTag, 2547208, Integer.class);

        // event action and event family
        requestTracker.addTag(TrackerTagValueUtil.EventActionTag, "mktc", String.class);
        requestTracker.addTag(TrackerTagValueUtil.EventFamilyTag, "mkt", String.class);

        // target url
        requestTracker.addTag("url_mpre", targetUrl, String.class);

        // referer
        requestTracker.addTag("ref", referer, String.class);

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

        // populate device info
        CollectionServiceUtil.populateDeviceDetectionParams(agentInfo, requestTracker);
      } catch (Exception ex) {
        logger.warn("Error when tracking ubi", ex);
        metrics.meter("ErrorTrackUbi");
      }
    } else {
      metrics.meter("InternalDomainRef");
    }

    if (message != null) {
      producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);
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
   * Parse rotation id from query mkrid
   */
  private long parseRotationId(MultiValueMap<String, String> parameters) {
    long rotationId = -1L;
    if (parameters.containsKey(Constants.MKRID) && parameters.get(Constants.MKRID).get(0) != null) {
      try {
        String rawRotationId = parameters.get(Constants.MKRID).get(0);
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
}