package com.ebay.traffic.chocolate;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.*;

public class MessageInterceptor implements Interceptor {
  private final String TIMESTAMP = "timestamp";

  @Override
  public void initialize() {

  }

  @Override
  public Event intercept(Event event) {
    String timestamp = getTimestamp(event.getBody());
    Map<String, String> headers = event.getHeaders();
    headers.put(TIMESTAMP, timestamp);
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
    @Override
    public void configure(Context context) {
      // TODO Auto-generated method stub
    }

    @Override
    public Interceptor build() {
      return new MessageInterceptor();
    }
  }
}
