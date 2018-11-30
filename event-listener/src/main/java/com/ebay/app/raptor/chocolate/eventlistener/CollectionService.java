package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.eventlistener.util.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.util.ListenerMessageParser;
import com.ebay.app.raptor.chocolate.gen.model.CollectionResponse;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import com.ebay.app.raptor.chocolate.gen.model.Event;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xiangli4
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
   * @param event event body
   * @return Response of status and message
   */
  public CollectionResponse collect(HttpServletRequest request, Event event) {

    String kafkaTopic;
    Producer<Long, ListenerMessage> producer;
    ChannelActionEnum channelAction;
    ChannelIdEnum channelType;
    long campaignId = -1l;

    CollectionResponse response = new CollectionResponse();

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
      response.setStatus(Constants.REJECTED);
      response.setMessage(Constants.ERROR_ILLEGAL_URL);
      return response;
    }

    // no query parameter, rejected
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
    if (parameters.size() == 0) {
      logger.error(Constants.ERROR_NO_QUERY_PARAMETER);
      esMetrics.meter("NoQueryParameter");
      response.setStatus(Constants.REJECTED);
      response.setMessage(Constants.ERROR_NO_QUERY_PARAMETER);
      return response;
    }

    // no mkevt, rejected
    if (!parameters.containsKey(Constants.MKEVT) || parameters.get(Constants.MKEVT).get(0) == null) {
      logger.error(Constants.ERROR_NO_MKEVT);
      esMetrics.meter("NoMkevtParameter");
      response.setStatus(Constants.REJECTED);
      response.setMessage(Constants.ERROR_NO_MKEVT);
      return response;
    }

    // mkevt != 1, rejected
    String mkevt = parameters.get(Constants.MKEVT).get(0);
    if (!mkevt.equals(Constants.VALID_MKEVT)) {
      logger.error(Constants.ERROR_INVALID_MKEVT);
      esMetrics.meter("InvalidMkevt");
      response.setStatus(Constants.REJECTED);
      response.setMessage(Constants.ERROR_INVALID_MKEVT);
      return response;
    }

    // parse channel from query cid
    // no cid, show error and accept
    if (!parameters.containsKey(Constants.CID) || parameters.get(Constants.CID).get(0) == null) {
      logger.error(Constants.ERROR_NO_CID);
      esMetrics.meter("NoCidParameter");
      response.setStatus(Constants.ACCEPTED);
      response.setMessage(Constants.ERROR_NO_CID);
      return response;
    }

    // invalid cid, show error and accept
    channelType = ChannelIdEnum.parse(parameters.get(Constants.CID).get(0));
    if (channelType == null) {
      logger.error(Constants.ERROR_INVALID_CID);
      esMetrics.meter("InvalidCid");
      response.setStatus(Constants.ACCEPTED);
      response.setMessage(Constants.ERROR_INVALID_CID);
      return response;
    }

    try {
      campaignId = Long.parseLong(parameters.get(Constants.CAMPID).get(0));
    } catch (Exception e) {
      logger.debug("No campaign id");
    }

    channelAction = ChannelActionEnum.CLICK;

    String action = ChannelActionEnum.CLICK.toString();

    String type = channelType.getLogicalChannel().getAvro().toString();

    String platform = Constants.PLATFORM_UNKNOWN;
    String userAgent = request.getHeader("User-Agent");
    if (userAgent != null) {
      if (userAgent.contains("Mobi")) {
        platform = Constants.PLATFORM_MOBILE;
      } else {
        platform = Constants.PLATFORM_DESKTOP;
      }
    }

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
      startTime, campaignId, channelType.getLogicalChannel().getAvro(), channelAction, uri, null);


    if (message != null) {
      long eventTime = message.getTimestamp();
      producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);
      stopTimerAndLogData(startTime, eventTime, additionalFields);
    }
    response.setStatus(Constants.ACCEPTED);
    response.setMessage(Constants.ACCEPTED);
    return response;
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
