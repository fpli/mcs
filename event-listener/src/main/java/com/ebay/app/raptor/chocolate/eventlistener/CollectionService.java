package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.eventlistener.util.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.util.ListenerMessageParser;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import com.ebay.app.raptor.chocolate.gen.model.Event;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xiangli4
 * The main logic of collection service:
 * 1. Check headers
 * 2. Parse everything from headers and bodies
 * 3. Add compatible headers
 * 4. Parse to ListenerMessage
 */
public class CollectionService {
  private static final Logger logger = Logger.getLogger(CollectionService.class);
  private final ESMetrics esMetrics;
  private ListenerMessageParser parser;
  private static CollectionService instance = null;

  private CollectionService() {
    this.esMetrics = ESMetrics.getInstance();
    parser = ListenerMessageParser.getInstance();
  }

  /**
   * singleton get instance
   *
   * @return CollectionService object
   */
  public static CollectionService getInstance() {
    if (instance == null) {
      synchronized (CollectionService.class) {
        if (instance == null) {
          instance = new CollectionService();
        }
      }
    }
    return instance;
  }

  /**
   * Collect event and publish to kafka
   * @param request raw request
   * @param endUserContext wrapped end user context
   * @param raptorSecureContext wrapped secure header context
   * @param event post body event
   * @return OK or Error message
   */
  public String collect(HttpServletRequest request, IEndUserContext endUserContext, RaptorSecureContext
    raptorSecureContext, Event event) {

    if (request.getHeader("X-EBAY-C-TRACKING") == null) {
      logger.error(Constants.ERROR_NO_TRACKING);
      esMetrics.meter("NoTracking");
      return Constants.ERROR_NO_TRACKING;
    }

    // add headers for compatible with chocolate filter and nrt
    Map<String, String> addHeaders = new HashMap<>();

    // response headers for compatible with chocolate filter and nrt
    Map<String, String> responseHeaders = new HashMap<>();

    /* referer is from post body (mobile) and from header (NodeJs and handler)
       By internet standard, referer is typo of referrer.
       From ginger client call, the referer is embedded in enduserctx header, but we also check header for other cases.
       For local test using postman, do not include enduserctx header, the service will generate enduserctx by
       cos-user-context-filter.
       Ginger client call will pass enduserctx in its header.
       Priority 1. native app from body, as they are the most part 2. enduserctx, ginger client calls 3. referer header
     */

    String platform = Constants.PLATFORM_UNKNOWN;

    String referer = null;
    if (!StringUtils.isEmpty(event.getReferrer())) {
      referer = event.getReferrer();
      platform = Constants.PLATFORM_MOBILE;
    }

    if(StringUtils.isEmpty(referer)) {
      referer = endUserContext.getReferer();
      platform = Constants.PLATFORM_DESKTOP;
    }

    if(StringUtils.isEmpty(referer) || referer.equalsIgnoreCase("null")) {
      logger.error(Constants.ERROR_NO_REFERER);
      esMetrics.meter("NoReferer");
      return Constants.ERROR_NO_REFERER;
    }

    String userAgent = endUserContext.getUserAgent();

    if (null == userAgent) {
      logger.error(Constants.ERROR_NO_USER_AGENT);
      esMetrics.meter("NoUserAgent");
      return Constants.ERROR_NO_USER_AGENT;
    }

    addHeaders.put("Referer", referer);
    //addHeaders.put("X-eBay-Client-IP", endUserContext.getIPAddress());
    addHeaders.put("User-Agent", endUserContext.getUserAgent());
    // only add UserId header when token is user token, otherwise it is app consumer id
    String userId;
    if ("EBAYUSER".equals(raptorSecureContext.getSubjectDomain())) {
      userId = raptorSecureContext.getSubjectImmutableId();
      addHeaders.put("UserId", raptorSecureContext.getSubjectImmutableId());
    } else {
      userId = Long.toString(endUserContext.getOrigUserOracleId());
    }
    addHeaders.put("UserId", userId);
    // add guid,cguid to cookie header to addHeaders and responseHeaders for compatibility
    String trackingHeader = request.getHeader("X-EBAY-C-TRACKING");
    StringBuilder cookie = new StringBuilder("npii=");
    if(!StringUtils.isEmpty(trackingHeader)) {
      for (String seg: trackingHeader.split(",")
           ) {
        String[] keyValue = seg.split("=");
        if(keyValue.length == 2) {
          cookie.append(keyValue[0]).append("/").append(keyValue[1]).append("^");
        }
      }
    }
    addHeaders.put("Cookie", cookie.toString());
    responseHeaders.put("Set-Cookie", cookie.toString());

    String kafkaTopic;
    Producer<Long, ListenerMessage> producer;
    ChannelActionEnum channelAction;
    ChannelIdEnum channelType;
    long campaignId = -1L;

    // uri is from post body
    String uri = event.getTargetUrl();

    // parse channel from uri
    // illegal url, rejected
    UriComponents uriComponents;
    try {
      uriComponents = UriComponentsBuilder.fromUriString(uri).build();
    } catch (IllegalArgumentException e) {
      logger.error(Constants.ERROR_ILLEGAL_URL);
      esMetrics.meter("IllegalUrl");
      return Constants.ERROR_ILLEGAL_URL;
    }

    // no query parameter, rejected
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
    if (parameters.size() == 0) {
      logger.error(Constants.ERROR_NO_QUERY_PARAMETER);
      esMetrics.meter("NoQueryParameter");
      return Constants.ERROR_NO_QUERY_PARAMETER;
    }

    // no mkevt, rejected
    if (!parameters.containsKey(Constants.MKEVT) || parameters.get(Constants.MKEVT).get(0) == null) {
      logger.error(Constants.ERROR_NO_MKEVT);
      esMetrics.meter("NoMkevtParameter");
      return Constants.ERROR_NO_MKEVT;
    }

    // mkevt != 1, rejected
    String mkevt = parameters.get(Constants.MKEVT).get(0);
    if (!mkevt.equals(Constants.VALID_MKEVT)) {
      logger.error(Constants.ERROR_INVALID_MKEVT);
      esMetrics.meter("InvalidMkevt");
      return Constants.ERROR_INVALID_MKEVT;
    }

    // parse channel from query cid
    // no cid, accepted
    if (!parameters.containsKey(Constants.CID) || parameters.get(Constants.CID).get(0) == null) {
      logger.error(Constants.ERROR_NO_CID);
      esMetrics.meter("NoCidParameter");
      return Constants.ACCEPTED;
    }

    // invalid cid, show error and accept
    channelType = ChannelIdEnum.parse(parameters.get(Constants.CID).get(0));
    if (channelType == null) {
      logger.error(Constants.ERROR_INVALID_CID);
      esMetrics.meter("InvalidCid");
      return Constants.ACCEPTED;
    }

    try {
      campaignId = Long.parseLong(parameters.get(Constants.CAMPID).get(0));
    } catch (Exception e) {
      logger.debug("No campaign id");
    }

    channelAction = ChannelActionEnum.CLICK;

    String action = ChannelActionEnum.CLICK.toString();

    String type = channelType.getLogicalChannel().getAvro().toString();

    String landingPageType;
    List<String> pathSegments = uriComponents.getPathSegments();
    if (pathSegments == null || pathSegments.size() == 0) {
      landingPageType = "home";
    } else {
      landingPageType = pathSegments.get(0);
    }

    Map<String, Object> additionalFields = new HashMap<>();
    additionalFields.put("channelAction", action);
    additionalFields.put("channelType", type);
    additionalFields.put("platform", platform);
    additionalFields.put("landingPageType", landingPageType);

    long startTime = startTimerAndLogData(additionalFields);

    producer = KafkaSink.get();

    kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());

    // Parse the response
    ListenerMessage message = parser.parse(request,
      startTime, campaignId, channelType.getLogicalChannel().getAvro(), channelAction, uri, null, addHeaders, responseHeaders);


    if (message != null) {
      long eventTime = message.getTimestamp();
      producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);
      stopTimerAndLogData(startTime, eventTime, additionalFields);
    }
    return Constants.ACCEPTED;
  }

  /**
   * Starts the timer and logs some basic info
   *
   * @param additionalFields channelAction, channelType, platform, landing page type
   * @return start time
   */
  private long startTimerAndLogData(Map<String, Object> additionalFields) {
    // the main rover process is already finished at this moment
    // use the timestamp from request as the start time
    long startTime = System.currentTimeMillis();
    logger.debug(String.format("StartTime: %d", startTime));
    esMetrics.meter("CollectionServiceIncoming", 1, startTime, additionalFields);
    return startTime;
  }

  /**
   * Stops the timer and logs relevant debugging messages
   *
   * @param startTime        the start time, so that latency can be calculated
   * @param additionalFields channelAction, channelType, platform, landing page type
   */
  private void stopTimerAndLogData(long startTime, long eventTime, Map<String, Object> additionalFields) {
    long endTime = System.currentTimeMillis();
    logger.debug(String.format("EndTime: %d", endTime));
    esMetrics.meter("CollectionServiceSuccess", 1, eventTime, additionalFields);
    esMetrics.mean("CollectionServiceAverageLatency", endTime - startTime);
  }
}
