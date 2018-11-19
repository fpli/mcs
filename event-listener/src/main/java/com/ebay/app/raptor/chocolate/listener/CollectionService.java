package com.ebay.app.raptor.chocolate.listener;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.listener.util.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.listener.util.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.listener.util.ListenerMessageParser;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import javax.servlet.http.HttpServletRequest;
import org.eclipse.jetty.server.Request;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;

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

  public static CollectionService getInstance() {
    if (instance == null)
      instance = new CollectionService();
    return instance;
  }

  public void process(HttpServletRequest request) {
    String kafkaTopic;
    Producer<Long, ListenerMessage> producer;
    ChannelActionEnum channelAction = null;
    ChannelIdEnum channelType;

    String uri = "";

    try {
      // uri is from post body
      uri = IOUtils.toString(request.getReader());
    } catch (IOException ex) {
      logger.error("Read post body error");
    }

    // parse channel from uri
    MultiValueMap<String, String> parameters = UriComponentsBuilder.fromUriString(uri).build().getQueryParams();

    channelType = ChannelIdEnum.parse(parameters.get("cid").get(0));

    String action = ChannelActionEnum.CLICK.toString();
    String type = channelType.getLogicalChannel().getAvro().toString();

    long startTime = startTimerAndLogData(request, action, type);

    producer = KafkaSink.get();

    kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(channelType.getLogicalChannel().getAvro());

    // Parse the response
    ListenerMessage message = parser.parse(request,
      startTime, null, channelType.getLogicalChannel().getAvro(), channelAction, uri, null);

    if (message != null) {
      long eventTime = message.getTimestamp();
      producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);
      stopTimerAndLogData(startTime, eventTime, action, type);
    }
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
