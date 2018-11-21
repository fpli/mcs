package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.eventlistener.util.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.ListenerMessageParser;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;
import com.ebay.app.raptor.chocolate.gen.model.Event;

import javax.servlet.http.HttpServletRequest;

/**
 * @author xiangli4
 */
public class CollectionService {
  private static final Logger logger = Logger.getLogger(CollectionService.class);
  private final ESMetrics esMetrics;
  private ListenerMessageParser parser;
  private final String CID = "cid";
  private final String CAMPID = "campid";
  private static CollectionService instance = null;

  private CollectionService() {
    this.esMetrics = ESMetrics.getInstance();
    parser = ListenerMessageParser.getInstance();
  }

  public static CollectionService getInstance() {
    if (instance == null)
      instance = new CollectionService();
    return instance;
  }

  public boolean collect(HttpServletRequest request, Event event) {

    String kafkaTopic;
    Producer<Long, ListenerMessage> producer;
    ChannelActionEnum channelAction = null;
    ChannelIdEnum channelType;
    long campaignId = -1l;

    // uri is from post body
    String uri = event.getTargetUrl();

    // parse channel from uri
    MultiValueMap<String, String> parameters = UriComponentsBuilder.fromUriString(uri).build().getQueryParams();
    if (parameters.size() == 0) {
      logger.error("No query parameter");
      esMetrics.meter("NoQueryParameter");
      return false;
    }

    // parse channel from query cid
    if(!parameters.containsKey(CID)) {
      logger.error("No cid parameter");
      esMetrics.meter("NoCIDParameter");
      return false;
    }
    channelType = ChannelIdEnum.parse(parameters.get(CID).get(0));

    if (channelType == null) {
      logger.error("Invalid cid " + uri);
      esMetrics.meter("InvalidCid");
      return false;
    }

    try {
      campaignId = Long.parseLong(parameters.get(CAMPID).get(0));
    } catch (Exception e) {
      logger.debug("No campaign id");
    }

    channelAction = ChannelActionEnum.CLICK;

    String action = ChannelActionEnum.CLICK.toString();

    String type = channelType.getLogicalChannel().getAvro().toString();

    long startTime = startTimerAndLogData(request, action, type);

    producer = KafkaSink.get();

    kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());

    // Parse the response
    ListenerMessage message = parser.parse(request,
      startTime, campaignId, channelType.getLogicalChannel().getAvro(), channelAction, uri, null);

    if (message != null) {
      long eventTime = message.getTimestamp();
      producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);
      stopTimerAndLogData(startTime, eventTime, action, type);
    }
    return true;
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
    logger.debug(String.format("StartTime: %d", startTime));
    esMetrics.meter("CollectionServiceIncoming", 1, startTime, channelAction, channelType);
    return startTime;
  }

  /**
   * Stops the timer and logs relevant debugging messages
   *
   * @param startTime     the start time, so that latency can be calculated
   * @param channelAction click, impression...
   * @param channelType   epn, dap...
   */
  private void stopTimerAndLogData(long startTime, long eventTime, String channelAction, String channelType) {
    long endTime = System.currentTimeMillis();
    logger.debug(String.format("EndTime: %d", endTime));
    esMetrics.meter("CollectionServiceSuccess", 1, eventTime, channelAction, channelType);
    esMetrics.mean("CollectionServiceAverageLatency", endTime - startTime);
  }
}
