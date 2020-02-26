package com.ebay.traffic.chocolate.listener.api;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.springframework.http.server.ServletServerHttpRequest;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URL;

/**
 * Handle all request from marketing
 * ex: https://www.ebayadservices.com/adTracking/v1?mkevt=2&mktcid=1&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&[other channel parameters]
 */
public class AdsTrackingServlet extends HttpServlet {
  private static final Logger logger = Logger.getLogger(AdsTrackingServlet.class);

  private static final long serialVersionUID = 5962261687335166542L;

  /**
   * Metrics client instance
   */
  private static Metrics metrics;

  private static Producer<Long, ListenerMessage> producer;

  /**
   * Message object parser instance
   */
  private static MessageObjectParser parser;

  public AdsTrackingServlet(final Metrics metrics, final MessageObjectParser parser) {
    this.metrics = metrics;
    this.parser = parser;
  }

  @Override
  public synchronized void init() {
    producer = KafkaSink.get();

    if (parser == null)
      parser = MessageObjectParser.getInstance();
    if (metrics == null)
      metrics = ESMetrics.getInstance();
  }

  /**
   * doGet method
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) {
    doRequest(request, response);
  }

  /**
   * doPost method
   */
  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response) {
    doRequest(request, response);
  }

  /**
   * Processes the event and sends a response
   *
   * @param request the HTTP request from the client
   * @param response the Listener's response, based on request URL
   */
  private void doRequest(HttpServletRequest request, HttpServletResponse response) {
    try {
      AdsTrackingEvent event = new AdsTrackingEvent(new URL(request.getRequestURL().toString()), request.getParameterMap());
      metrics.meter("TrackingCount", 1, Field.of(EventConstant.CHANNEL_ACTION, event.getAction().getAvro().toString()), Field.of(EventConstant.CHANNEL_TYPE, event.getChannel().toString()));
      process(request, response, event);
    } catch (Exception e) {
      logger.error("Couldn't respond to tracking event for " + request.getRequestURL(), e);
    }
  }

  /**
   * Process the request, send to kafka topic
   */
  private synchronized void process(HttpServletRequest request, HttpServletResponse response, AdsTrackingEvent event) {
    String kafkaTopic = ListenerOptions.getInstance().getSinkKafkaConfigs().get(event.getChannel());
    ListenerMessage message;
    String snid = null;

    try {
      //Make the response
      event.respond(response);

      if (event.getPayload() != null && event.getPayload().containsKey(EventConstant.MK_SESSION_ID)) {
        snid = event.getPayload().get(EventConstant.MK_SESSION_ID).toString();
      }
      String requestUrl = parser.appendURLWithChocolateTag(new ServletServerHttpRequest(request).getURI().toString());
      message = parser.parseHeader(request, response, System.currentTimeMillis(), event.getCampaignID(), event.getChannel(), event.getAction(), snid, requestUrl);
      if (message == null) {
        logger.error("Could not create Avro message for url=" + request.getRequestURL());
        metrics.meter("TrackingFail", 1);
        return;
      }

      producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);

      metrics.meter("TrackingSuccess", 1, Field.of(EventConstant.CHANNEL_ACTION, event.getAction().getAvro().toString()), Field.of(EventConstant.CHANNEL_TYPE, event.getChannel().toString()));

    } catch (Exception e) {
      logger.error("Couldn't respond to tracking event for " + request.getRequestURL(), e);
      metrics.meter("TrackingFail", 1);
    }
  }
}