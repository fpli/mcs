package com.ebay.traffic.chocolate;

import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SeedInterceptor
    implements Interceptor {
  private static final Logger logger = LoggerFactory.getLogger(SeedInterceptor.class);
  private static final String MSG_TIME = "time";
  private static final String MSG_USER = "user";
  private static final String MSG_USER_ID = "userId";
  private static final String MSG_DEVICE_ID = "deviceId";
  private static final String MSG_DEVICE_TYPE = "MSG_DEVICE_TYPE";
  private static final String MSG_DEVICE_UNKNOW = "UNKNOW";
  private static final String MSG_DEVICE_DESKTOP = "desktop";

  public void initialize() {
    logger.info("Flume interceptor started.");
  }

  public Event intercept(Event event) {
    String MSG_CHANNEL = SeedInterceptorBuilder.ktopic;
    CouchbaseClient cbClient = SeedInterceptorBuilder.cbClient;
    ESMetrics esMetrics = ESMetrics.getInstance();

    JsonObject record = getBody(event.getBody());

    Long msgTimestamp = Long.valueOf(record.get(MSG_TIME) == null ? System.currentTimeMillis() : record.get(MSG_TIME).getAsLong());
    JsonObject user = record.getAsJsonObject(MSG_USER);

    String deviceType = MSG_DEVICE_UNKNOW;
    if (user.get(MSG_DEVICE_TYPE) != null) {
      deviceType = user.get(MSG_DEVICE_TYPE).getAsString();
    }
    String userId = null;
    if (user.get(MSG_USER_ID) != null) {
      userId = user.get(MSG_USER_ID).getAsString();
    }
    logger.info("userId=" + userId + "  timestamp=" + msgTimestamp);
    if (userId == null) {
      String deviceId = user.get(MSG_DEVICE_ID).getAsString();
      if (MSG_DEVICE_DESKTOP.equals(deviceType)) {
      }
      esMetrics.meter("Seed-UserNull", 1L, msgTimestamp.longValue(), MSG_CHANNEL, deviceType);
    } else {
      try {
        cbClient.upsert(userId, System.currentTimeMillis());
        esMetrics.meter("Seed-CB-Upsert-Success", 1L, msgTimestamp.longValue(), MSG_CHANNEL, deviceType);
      } catch (Exception e) {
        e.printStackTrace();
        esMetrics.meter("Seed-CB-Upsert-Error", 1L, msgTimestamp.longValue());
      }
    }
    long latency = System.currentTimeMillis() - msgTimestamp.longValue();
    esMetrics.meter("Seed-Throughput", 1L, msgTimestamp.longValue(), MSG_CHANNEL, deviceType);
    esMetrics.mean("Seed-Latency", latency);
    return event;
  }

  public JsonObject getBody(byte[] message) {
    String json = new String(message);
    JsonParser jsonParser = new JsonParser();
    return (JsonObject) jsonParser.parse(json);
  }

  public List<Event> intercept(List<Event> list) {
    List<Event> interceptedEvents = new ArrayList(list.size());
    for (Event event : list) {
      Event interceptedEvent = intercept(event);
      interceptedEvents.add(interceptedEvent);
    }
    return interceptedEvents;
  }

  public void close() {
  }

  public static class SeedInterceptorBuilder implements Interceptor.Builder {
    public static String pjUrl = "";
    public static String xidUrl = "";
    public static String ktopic = "";
    public static CouchbaseClient cbClient;
    private String esPrefix = "";
    private String esUrl = "";

    public void configure(Context context) {
      SeedInterceptor.logger.info("#### Start the interceptor init. ####");
      this.esPrefix = context.getString("esPrefix");
      this.esUrl = context.getString("esUrl");
      pjUrl = context.getString("pjUrl");
      xidUrl = context.getString("xidUrl");
      ktopic = context.getString("ktopic");
      SeedInterceptor.logger.info("esPrefix=" + this.esPrefix);
      SeedInterceptor.logger.info("esUrl=" + this.esUrl);
      SeedInterceptor.logger.info("pjUrl=" + pjUrl);
      SeedInterceptor.logger.info("xidUrl=" + xidUrl);
      SeedInterceptor.logger.info("ktopic=" + ktopic);

      try {
        cbClient = CouchbaseClient.getInstance(context.getString("configPath"));
      } catch (IOException e) {
        e.printStackTrace();
        SeedInterceptor.logger.error("Connect Couchbase Exception");
      }

      ESMetrics.init(this.esPrefix, this.esUrl);
      SeedInterceptor.logger.info("ESMetrics has been initialed");

      SeedInterceptor.logger.info("Couchbase has been initialed");
      SeedInterceptor.logger.info("#### End the interceptor init. ####");
    }

    public Interceptor build() {
      return new SeedInterceptor();
    }
  }
}
