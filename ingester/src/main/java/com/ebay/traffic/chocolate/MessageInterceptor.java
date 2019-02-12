package com.ebay.traffic.chocolate;

import com.ebay.traffic.monitoring.ESMetrics;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MessageInterceptor implements Interceptor {
  private static final Logger logger = LoggerFactory.getLogger(MessageInterceptor.class);
  private final String TIMESTAMP = "timestamp";
  private static final String FLUME_FILTERED_RECORD_COUNT = "FlumeFilteredRecordCount";

  @Override
  public void initialize() {
    logger.info("Flume interceptor started.");
  }

  @Override
  public Event intercept(Event event) {
    String timestamp = getTimestamp(event.getBody());
    Map<String, String> headers = event.getHeaders();
    headers.put(TIMESTAMP, timestamp);
    ESMetrics.getInstance().meter(FLUME_FILTERED_RECORD_COUNT);
    return event;
  }

  public String getTimestamp(byte[] message) {
    String json = new String(message);
    JsonParser jsonParser = new JsonParser();
    JsonObject jsonObject = (JsonObject) jsonParser.parse(json);
    String timestamp = jsonObject.get(TIMESTAMP).getAsString();
    return timestamp;
  }

  @Override
  public List<Event> intercept(List<Event> list) {
    List<Event> interceptedEvents = new ArrayList<Event>(list.size());
    for (Event event : list) {
      // Intercept any event
      Event interceptedEvent = intercept(event);
      interceptedEvents.add(interceptedEvent);
    }
    return interceptedEvents;
  }

  @Override
  public void close() {

  }

  public static class MessageInterceptorBuilder implements Interceptor.Builder {
    private String esPrefix = "";
    private String esUrl = "";

    @Override
    public void configure(Context context) {
      logger.info("the interceptor init.");
      esPrefix = context.getString("esPrefix");
      esUrl = context.getString("esUrl");
      logger.info("esPrefix: " + esPrefix);
      logger.info("esUrl: " + esUrl);
      //initial ES Metric client.
      ESMetrics.init(esPrefix, esUrl);
      logger.info("the interceptor end.");
    }

    @Override
    public Interceptor build() {
      return new MessageInterceptor();
    }
  }
}
