package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.*;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.springframework.http.server.ServletServerHttpRequest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.Enumeration;
import java.util.HashMap;

public class DefaultChannel implements Channel {
  private static final Logger logger = Logger.getLogger(DefaultChannel.class);
  private final Metrics metrics;
  private MessageObjectParser parser;
  private static final String CAMPAIGN_PATTERN = "campid";
  private static final String SNID_PATTERN = "snid";
  private static final String MALFORMED_Tracking_URL = "malformedTrackingURL";
  private static final String MALFORMED_URL = "malformedURL";
  private static final String CHANNEL_ACTION = "channelAction";
  private static final String CHANNEL_TYPE = "channelType";

  DefaultChannel() {
    this.metrics = ESMetrics.getInstance();
    this.parser = MessageObjectParser.getInstance();
    metrics.meter(MALFORMED_URL, 0);
    metrics.meter(MALFORMED_Tracking_URL, 0);
  }

  /**
   * Default channel handler
   */
  @Override
  public void process(HttpServletRequest request, HttpServletResponse response) {
    String kafkaTopic;
    String listenerFilteredKafkaTopic;
    Producer<Long, ListenerMessage> producer;
    ChannelActionEnum channelAction = null;
    ChannelIdEnum channelType = null;

    String[] result = request.getRequestURI().split("/");
    if (result.length >= 2)
      channelAction = ChannelActionEnum.parse(null, result[1]);

    // handle rover sync separately
    if(channelAction != null && channelAction.equals(ChannelActionEnum.SYNC)) {
      String cookieResponseHeader = response.getHeader("Set-Cookie");
      String cguid = parser.getGuid(null, cookieResponseHeader, null, "cguid");
      String guid = parser.getGuid(null, cookieResponseHeader, null, "tguid");
      if(guid.length() > 0) {
        CouchbaseClient.getInstance().addMappingRecord(guid, cguid);
      }
      return;
    }
    if (result.length == 5)
      channelType = ChannelIdEnum.parse(result[4]);

    String action = null;
    String type = null;
    if (channelAction != null)
      action = channelAction.getAvro().toString();
    if (channelType != null)
      type = channelType.getLogicalChannel().getAvro().toString();

    long startTime = startTimerAndLogData(request, action, type);

    String requestUrl = null;
    try {
      requestUrl = parser.appendURLWithChocolateTag(new ServletServerHttpRequest(request).getURI().toString());
    } catch (Exception e) {
      metrics.meter("AppendNewTagError", 1, startTime, Field.of(CHANNEL_ACTION, action),
          Field.of(CHANNEL_TYPE, type));
      logger.error("Append url with new tag error");
    }

    producer = KafkaSink.get();

    long campaignId = getCampaignID(request, startTime, action, type);

    //TODO: remove the logic when we stable
    try {
      parser.appendTagWhenRedirect(request, response, requestUrl);
    } catch (MalformedURLException | UnsupportedEncodingException e) {
      logger.error("Wrong with URL format/encoding", e);
      metrics.meter(MALFORMED_URL);
      String kafkaMalformedTopic = ListenerOptions.getInstance().getListenerFilteredTopic();
      ListenerMessage message = new ListenerMessage(-1L, -1L, -1L, -1L, "", "", "", "", "", -1L, "", "",
              -1L, -1L, -1L, "", -1L, -1L, "", "", "", ChannelAction.IMPRESSION, ChannelType.DEFAULT, HttpMethod.GET, "", false);
      long snapshotId = SnapshotId.getNext(ListenerOptions.getInstance().getDriverId(), startTime).getRepresentation();
      ShortSnapshotId shortSnapshotId = new ShortSnapshotId(snapshotId);
      message.setSnid("999998");
      message.setUri(requestUrl);
      message.setSnapshotId(snapshotId);
      message.setShortSnapshotId(shortSnapshotId.getRepresentation());
      message.setTimestamp(startTime);
      message.setCampaignId(campaignId);
      message.setHttpMethod(parser.getMethod(request).getAvro());
      producer.send(new ProducerRecord<>(kafkaMalformedTopic,
          message.getSnapshotId(), message), KafkaSink.callback);
    }

    String snid = request.getParameter(SNID_PATTERN);

    if (result.length == 5) {
      channelType = ChannelIdEnum.parse(result[4]);
      if (channelType == null) {
        invalidRequestParam(request, campaignId,"No pattern matched;", startTime, action, type, requestUrl);
        metrics.meter("NoPatternMatched", 1, startTime, Field.of(CHANNEL_ACTION, action),
            Field.of(CHANNEL_TYPE, type));
        return;
      }
      channelAction = ChannelActionEnum.parse(channelType, result[1]);
      if (!channelType.getLogicalChannel().isValidRoverAction(channelAction)) {
        invalidRequestParam(request, campaignId,"Invalid tracking action given a channel;", startTime, action,
            type, requestUrl);
        metrics.meter("InvalidAction", 1, startTime, Field.of(CHANNEL_ACTION, action),
            Field.of(CHANNEL_TYPE, type));
        return;
      }
      if (channelType.isTestChannel()) {
        invalidRequestParam(request, campaignId,"Test channel;", startTime, action, type, requestUrl);
        metrics.meter("TestChannel", 1, startTime, Field.of(CHANNEL_ACTION, action),
            Field.of(CHANNEL_TYPE, type));
        return;
      }

      if (campaignId < 0 && channelType.equals(ChannelIdEnum.EPN)) {
        invalidRequestParam(request, campaignId, "Invalid campaign id;", startTime, action, type, requestUrl);
        metrics.meter("InvalidCampaign", 1, startTime, Field.of(CHANNEL_ACTION, action),
            Field.of(CHANNEL_TYPE, type));
        return;
      }

      kafkaTopic = ListenerOptions.getInstance().getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());
      listenerFilteredKafkaTopic = ListenerOptions.getInstance().getListenerFilteredTopic();
    } else {
      invalidRequestParam(request, campaignId, "Request params count != 5", startTime, action, type, requestUrl);
      return;
    }

    // Parse the response
    ListenerMessage message = parser.parseHeader(request, response,
        startTime, campaignId, channelType.getLogicalChannel().getAvro(), channelAction, snid, requestUrl);

    if (message != null) {
      long eventTime = message.getTimestamp();
      // Only save core site url
      if(parser.isCoreSite(request)) {
        producer.send(new ProducerRecord<>(kafkaTopic,
          message.getSnapshotId(), message), KafkaSink.callback);
        stopTimerAndLogData(startTime, eventTime, action, type);
      }
      // Other site url are sent to another kafka topic
      else {
        producer.send(new ProducerRecord<>(listenerFilteredKafkaTopic,
          message.getSnapshotId(), message), KafkaSink.callback);
        metrics.meter("SendIntlKafkaCount", 1, eventTime);
      }
    } else {
      invalidRequestParam(request, campaignId,"Parse message error;", startTime, action, type, requestUrl);
    }
  }



  /**
   * getCampaignId based on query pattern match
   * Note: parameter is case insensitive
   *
   * @param request incoming HttpServletRequest
   * @return campaignID, default -1L if no pattern match in the query of HttpServletRequest
   */
  public long getCampaignID(final HttpServletRequest request, long eventTime, String channelAction, String channelType) {
    HashMap<String, String> lowerCaseParams = new HashMap<>();
    Enumeration params = request.getParameterNames();
    while (params.hasMoreElements()) {
      String param = params.nextElement().toString();
      lowerCaseParams.put(param.toLowerCase(), param);
    }
    long campaignId = -1L;
    String campaign = lowerCaseParams.get(CAMPAIGN_PATTERN);

    if (campaign != null && !request.getParameter(campaign).isEmpty()) {
      try {
        campaignId = Long.parseLong(request.getParameter(campaign).trim());
      } catch (NumberFormatException e) {
        logger.warn("Invalid campaign: " + request.getParameter(campaign));
        metrics.meter("InvalidCampaign", 1, eventTime, Field.of(CHANNEL_ACTION, channelAction),
            Field.of(CHANNEL_TYPE, channelType));
      }
    }

    logger.debug(String.format("PartitionKey: %d", campaignId));
    return campaignId;
  }

  /**
   * Stops the timer and logs relevant debugging messages
   *
   * @param startTime    the start time, so that latency can be calculated
   * @param channelAction click, impression...
   * @param channelType epn, dap...
   */
  private void stopTimerAndLogData(long startTime, long eventTime, String channelAction, String channelType) {
    long endTime = System.currentTimeMillis();
    logger.debug(String.format("EndTime: %d", endTime));
    metrics.meter("SuccessCount", 1, eventTime, Field.of(CHANNEL_ACTION, channelAction),
        Field.of(CHANNEL_TYPE, channelType));
    metrics.mean("AverageLatency", endTime - startTime);
  }

  /**
   * @return a query message derived from the given string.
   */
  private StringBuffer deriveWarningMessage(StringBuffer sb,
                                            HttpServletRequest servletRequest) {
    sb.append(" URL=").append(servletRequest.getRequestURL().toString())
        .append(" queryStr=").append(servletRequest.getQueryString());
    return sb;
  }

  /**
   * Starts the timer and logs some basic info
   *
   * @param request Incoming Http request
   * @return the start time in milliseconds
   */
  private long startTimerAndLogData(HttpServletRequest request, String channelAction, String channelType) {
    // the main rover process is already finished at this moment
    // use the timestamp from request as the start time
    long startTime = System.currentTimeMillis();

    try {
      startTime = ((Request) request).getTimeStamp();
    } catch (ClassCastException e) {
      // ideally only touch this part in unit test
      logger.warn("Cannot get request start time, use system time instead. ", e);
    }
    logger.debug(String.format("StartTime: %d", startTime));
    metrics.meter("ProxyIncomingCount", 1, startTime, Field.of(CHANNEL_ACTION, channelAction),
        Field.of(CHANNEL_TYPE, channelType));
    return startTime;
  }

  private void invalidRequestParam(HttpServletRequest request, long campaignId, String invalid, long eventTime,
                                   String channelAction, String channelType, String requestUrl) {
    StringBuffer sb = new StringBuffer();
    sb.append(invalid);
    sb = deriveWarningMessage(sb, request);
    logger.warn(sb.toString());
    logger.warn("Un-managed channel request: " + request.getRequestURL().toString());
    metrics.meter("un-managed", 1, eventTime, Field.of(CHANNEL_ACTION, channelAction),
        Field.of(CHANNEL_TYPE, channelType));
    metrics.meter(MALFORMED_Tracking_URL);
    sendMalformedURLToKafka(request, eventTime, campaignId, requestUrl);
  }

  private void sendMalformedURLToKafka(HttpServletRequest request, long startTime, long campaignId, String requestUrl) {
    String kafkaMalformedTopic = ListenerOptions.getInstance().getListenerFilteredTopic();
    Producer<Long, ListenerMessage> producer = KafkaSink.get();
    ListenerMessage message = new ListenerMessage(-1L, -1L, -1L, -1L, "", "", "", "", "", -1L, "", "",
            -1L, -1L, -1L, "", -1L, -1L, "", "", "", ChannelAction.IMPRESSION, ChannelType.DEFAULT, HttpMethod.GET, "", false);
    long snapshotId = SnapshotId.getNext(ListenerOptions.getInstance().getDriverId(), startTime).getRepresentation();
    ShortSnapshotId shortSnapshotId = new ShortSnapshotId(snapshotId);
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(shortSnapshotId.getRepresentation());
    message.setSnid("999999");
    message.setTimestamp(startTime);
    message.setCampaignId(campaignId);
    message.setUri(requestUrl);
    message.setHttpMethod(parser.getMethod(request).getAvro());

    producer.send(new ProducerRecord<>(kafkaMalformedTopic,
        message.getSnapshotId(), message), KafkaSink.callback);
  }
}