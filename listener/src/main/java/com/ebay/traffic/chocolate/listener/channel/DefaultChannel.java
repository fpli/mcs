package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.ChannelActionEnum;
import com.ebay.traffic.chocolate.listener.util.ChannelIdEnum;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.springframework.http.server.ServletServerHttpRequest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Enumeration;
import java.util.HashMap;

public class DefaultChannel implements Channel {
  private static final Logger logger = Logger.getLogger(DefaultChannel.class);
  private final MetricsClient metrics;
  private final ESMetrics esMetrics;
  private MessageObjectParser parser;
  private static final String CAMPAIGN_PATTERN = "campid";
  private static final String SNID_PATTERN = "snid";

  DefaultChannel() {
    this.metrics = MetricsClient.getInstance();
    this.esMetrics = ESMetrics.getInstance();
    this.parser = MessageObjectParser.getInstance();
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
      metrics.meter("AppendNewTagError");
      esMetrics.meter("AppendNewTagError");
      logger.error("Append url with new tag error");
    }

    if (result.length >= 2) {
      if (ChannelActionEnum.CLICK.equals(channelAction)) {
        metrics.meter("ProxyIncomingClickCount");
      }
      if (ChannelActionEnum.IMPRESSION.equals(channelAction)) {
        metrics.meter("ProxyIncomingImpressionCount");
      }
    }

    producer = KafkaSink.get();

    long campaignId = getCampaignID(request);

    metrics.meter("IncomingCount");
    esMetrics.meter("IncomingCount", action, type);

    String snid = request.getParameter(SNID_PATTERN);

    if (result.length == 5) {
      channelType = ChannelIdEnum.parse(result[4]);
      if (channelType == null) {
        invalidRequestParam(request, "No pattern matched;", action, type);
        esMetrics.meter("NoPatternMatched", action, type);
        return;
      }
      channelAction = ChannelActionEnum.parse(channelType, result[1]);
      if (!channelType.getLogicalChannel().isValidRoverAction(channelAction)) {
        invalidRequestParam(request, "Invalid tracking action given a channel;", action, type);
        esMetrics.meter("InvalidAction", action, type);
        return;
      }
      if (channelType.isTestChannel()) {
        invalidRequestParam(request, "Test channel;", action, type);
        esMetrics.meter("TestChannel", action, type);
        return;
      }

      if (campaignId < 0 && channelType.equals(ChannelIdEnum.EPN)) {
        invalidRequestParam(request, "Invalid campaign id;", action, type);
        esMetrics.meter("InvalidCampaign", action, type);
        return;
      }

      kafkaTopic = ListenerOptions.getInstance().getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());
      listenerFilteredKafkaTopic = ListenerOptions.getInstance().getListenerFilteredTopic();
      if (ChannelActionEnum.CLICK.equals(channelAction)) {
        metrics.meter("SendKafkaClickCount");
      }
      if (ChannelActionEnum.IMPRESSION.equals(channelAction)) {
        metrics.meter("SendKafkaImpressionCount");
      }
      esMetrics.meter("SendKafkaCount", action, type);
    } else {
      invalidRequestParam(request, "Request params count != 5", action, type);
      return;
    }

    // Parse the response
    ListenerMessage message = parser.parseHeader(request, response,
        startTime, campaignId, channelType.getLogicalChannel().getAvro(), channelAction, snid, requestUrl);

    if (message != null) {
      // Only save core site url
      if(parser.isCoreSite(request)) {
        producer.send(new ProducerRecord<>(kafkaTopic,
          message.getSnapshotId(), message), KafkaSink.callback);
      }
      // Other site url are sent to another kafka topic
      else {
        producer.send(new ProducerRecord<>(listenerFilteredKafkaTopic,
          message.getSnapshotId(), message), KafkaSink.callback);
      }
    } else {
      invalidRequestParam(request, "Parse message error;", action, type);
    }
    stopTimerAndLogData(startTime, action, type);
  }



  /**
   * getCampaignId based on query pattern match
   * Note: parameter is case insensitive
   *
   * @param request incoming HttpServletRequest
   * @return campaignID, default -1L if no pattern match in the query of HttpServletRequest
   */
  public long getCampaignID(final HttpServletRequest request) {
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
        campaignId = Long.parseLong(request.getParameter(campaign));
      } catch (NumberFormatException e) {
        logger.warn("Invalid campaign: " + request.getParameter(campaign));
        esMetrics.meter("InvalidCampaign");
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
  private void stopTimerAndLogData(long startTime, String channelAction, String channelType) {
    long endTime = System.currentTimeMillis();
    logger.debug(String.format("EndTime: %d", endTime));
    metrics.meter("SuccessCount");
    metrics.mean("AverageLatency", endTime - startTime);
    esMetrics.meter("SuccessCount", channelAction, channelType);
    esMetrics.mean("AverageLatency", endTime - startTime);
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
    metrics.meter("ProxyIncomingCount");
    esMetrics.meter("ProxyIncomingCount", channelAction, channelType);
    return startTime;
  }

  private void invalidRequestParam(HttpServletRequest request, String invalid, String channelAction, String channelType) {
    StringBuffer sb = new StringBuffer();
    sb.append(invalid);
    sb = deriveWarningMessage(sb, request);
    logger.warn(sb.toString());
    logger.warn("Un-managed channel request: " + request.getRequestURL().toString());
    metrics.meter("un-managed");
    esMetrics.meter("un-managed", channelAction, channelType);
  }

}