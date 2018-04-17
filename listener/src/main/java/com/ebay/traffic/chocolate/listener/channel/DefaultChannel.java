package com.ebay.traffic.chocolate.listener.channel;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.ChannelActionEnum;
import com.ebay.traffic.chocolate.listener.util.ChannelIdEnum;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.Enumeration;
import java.util.HashMap;

public class DefaultChannel implements Channel {
    private static final Logger logger = Logger.getLogger(DefaultChannel.class);
    private final MetricsClient metrics;
  private MessageObjectParser parser;
  private static final String CAMPAIGN_PATTERN = "campid";
  private static final String SNID_PATTERN = "snid";

  DefaultChannel() {
    this.metrics = MetricsClient.getInstance();
    this.parser = MessageObjectParser.getInstance();
  }

    /**
     * Default channel handler
     */
    @Override
    public void process(HttpServletRequest request, HttpServletResponse response) {
      String kafkaTopic;
      Producer<Long, ListenerMessage> producer;
      ChannelActionEnum channelAction;
      ChannelIdEnum channel;
      try {
        if (parser.responseShouldBeFiltered(request, response))
          return;
      } catch (MalformedURLException | UnsupportedEncodingException e) {
        logger.error("Wrong with URL format/encoding", e);
      }

      long campaignId = getCampaignID(request);

      long startTime = startTimerAndLogData(request);

      String snid = request.getParameter(SNID_PATTERN);

      String[] result = request.getRequestURI().split("/");

      if (result.length == 5) {
        channel = ChannelIdEnum.parse(result[4]);
        if (channel == null) {
          invalidRequestParam(request, "No pattern matched;");
          return;
        }
        channelAction = ChannelActionEnum.parse(channel, result[1]);
        if (!channel.getLogicalChannel().isValidRoverAction(channelAction)) {
          invalidRequestParam(request, "Invalid tracking action given a channel;");
          return;
        }
        if (channel.isTestChannel()) {
          logger.info("Received test URL; URL = " + request.getRequestURI());
          return;
        }

        kafkaTopic = ListenerOptions.getInstance().getSinkKafkaConfigs().get(channel.getLogicalChannel().getAvro());
        producer = KafkaSink.get();
      } else {
        logger.warn("Un-managed channel request: " + request.getRequestURL().toString());
        metrics.meter("un-managed");
        return;
      }

      // Parse the response
      ListenerMessage message = parser.parseHeader(request, response,
          startTime, campaignId, channel.getLogicalChannel().getAvro(), channelAction, snid);

      if (message != null)
        producer.send(new ProducerRecord<>(kafkaTopic,
            message.getSnapshotId(), message), KafkaSink.callback);
      stopTimerAndLogData(startTime, message.toString());
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
          logger.warn("Invalid campaign: " + campaign);
        }
      }

      logger.debug(String.format("PartitionKey: %d", campaignId));
      return campaignId;
    }

  /**
   * Stops the timer and logs relevant debugging messages
   *
   * @param startTime    the start time, so that latency can be calculated
   * @param kafkaMessage logged to CAL for debug purposes
   */
  private void stopTimerAndLogData(long startTime, String kafkaMessage) {
    long endTime = System.currentTimeMillis();
    logger.debug(String.format("EndTime: %d", endTime));
    metrics.meter("SuccessCount");
    metrics.mean("AverageLatency", endTime - startTime);
  }

  /** @return a query message derived from the given string. */
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
  private long startTimerAndLogData(HttpServletRequest request) {
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
    metrics.meter("IncomingCount");
    return startTime;
  }

  private void invalidRequestParam(HttpServletRequest request, String invalid) {
    StringBuffer sb = new StringBuffer();
    sb.append(invalid);
    sb = deriveWarningMessage(sb, request);
    logger.warn(sb.toString());
    logger.warn("Un-managed channel request: " + request.getRequestURL().toString());
    metrics.meter("un-managed");
  }

}