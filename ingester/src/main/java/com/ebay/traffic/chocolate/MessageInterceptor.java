package com.ebay.traffic.chocolate;

import com.ebay.traffic.chocolate.avro.FilterMessage;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class MessageInterceptor implements Interceptor {
  private final String TIMESTAMP = "timestamp";

  @Override
  public void initialize() {

  }

  @Override
  public Event intercept(Event event) {
    try {
      FilterMessage message = FilterMessage.fromByteBuffer(ByteBuffer.wrap(event.getBody()));
      Map<String, String> headers = event.getHeaders();
      headers.put(TIMESTAMP, message.getTimestamp().toString());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return event;
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
