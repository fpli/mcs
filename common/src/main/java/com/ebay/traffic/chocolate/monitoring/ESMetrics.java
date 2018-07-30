package com.ebay.traffic.chocolate.monitoring;

import com.google.gson.Gson;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jialili1 on 6/15/18.
 *
 * ElasticSearch metrics, and ES "index" with template mapping should be created so that it can be displayed on Grafana
 *
 * The metrics is aggregated locally and flushed to ES through background thread.
 */
public class ESMetrics {
  private static final Logger logger = LoggerFactory.getLogger(ESMetrics.class);

  private RestClient restClient;

  /**
   * Singleton instance of ESMetrics
   */
  private static ESMetrics INSTANCE = null;

  private static String INDEX_PREFIX;

  /**
   * The timer
   */
  private Timer timer;

  /**
   * Hashmap to aggregate metrics locally
   */
  private final Map<String, Long> meterMetrics = new HashMap<>();

  /**
   * Mean metrics
   */
  private final Map<String, Pair<Long, Long>> meanMetrics = new HashMap<>();

  /**
   * Hashmap for flush purpose
   */
  private final Map<String, Long> toFlushMeter = new HashMap<>();

  /**
   * Hashmap for flush mean purpose
   */
  private final Map<String, Pair<Long, Long>> toFlushMean = new HashMap<>();

  private final Object flushLock = new Object();

  private String hostname;

  ESMetrics(String prefix) {
    this.INDEX_PREFIX = prefix;
    timer = new Timer();
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      logger.warn(e.toString());
    }
  }

  /**
   * Init the ElasticSearch client
   *
   * @param url the url contains hostname, port and http scheme
   */
  public static synchronized void init(String prefix, String url) {
    init(prefix, HttpHost.create(url));
  }

  private static synchronized void init(String prefix, HttpHost httpHost) {
    if (INSTANCE != null) {
      return;
    }
    INSTANCE = new ESMetrics(prefix);

    RestClientBuilder builder = RestClient.builder(httpHost);
    INSTANCE.restClient = builder.build();

    // Start the timer.
    INSTANCE.timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        INSTANCE.flushMetrics();
      }
    }, 30000, 30000); // flush every 30s
  }

  /**
   * @return the instance of ESMetrics
   */
  public static ESMetrics getInstance() {
    return INSTANCE;
  }

  /**
   * Close the ES client
   */
  public void close() {
    if (timer != null) {
      timer.cancel();
      timer = null;
    }
    if (restClient != null) {
      try {
        restClient.close();
        restClient = null;
      } catch (IOException e) {
        // ignore
      }
    }
  }

  @Override
  protected void finalize() {
    close();
  }

  private final SimpleDateFormat sdf0 = new SimpleDateFormat("yyyy.MM.dd");
  //ElasticSearch default date format
  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private final Random random = new SecureRandom();

  /**
   * meter only with name
   */
  public void meter(String name) {
    meter(name, 1L);
  }

  /**
   * meter without additional fields
   *
   * @param name the metric name
   * @param value the metric value
   */
  public void meter(String name, long value) {
    meter(name, value, null);
  }

  /**
   * meter with channelAction and channelType
   */
  public void meter(String name, String channelAction, String channelType) {
    meter(name, 1L, channelAction, channelType);
  }

  /**
   * meter with channelAction and channelType
   *
   * @param channelAction the channel action
   * @param channelType the channel type
   */
  public void meter(String name, long value, String channelAction, String channelType) {
    Map<String, Object> additionalFields = new HashMap<>();
    if (channelAction != null)
      additionalFields.put("channelAction", channelAction);
    if (channelType != null)
      additionalFields.put("channelType", channelType);
    meter(name, value, additionalFields);
  }

  /**
   * meter with additional fields
   *
   * @param additionalFields fields names except name and value
   */
  public synchronized void meter(String name, long value, Map<String, Object> additionalFields){
    try {
      if (additionalFields != null)
        name = metricsNameByFields(name, additionalFields);
    } catch (Exception e){
      logger.warn(e.toString());
    }
    if (!meterMetrics.containsKey(name)) {
      meterMetrics.put(name, 0L);
    }
    long meter = meterMetrics.get(name);
    meter += value; // aggregate locally
    meterMetrics.put(name, meter);
  }

  /**
   * mean only with name
   */
  public void mean(String name) {
    mean(name, 1L);
  }

  /**
   * mean without additional fields
   *
   * @param name the metric name
   * @param value the metric value
   */
  public void mean(String name, long value) {
    mean(name, value, null);
  }

  /**
   * mean with channelAction and channelType
   *
   * @param additionalFields fields names except name and value
   */
  public synchronized void mean(String name, long value, Map<String, Object> additionalFields) {
    try {
      if (additionalFields != null)
        name = metricsNameByFields(name, additionalFields);
    } catch (Exception e) {
      logger.warn(e.toString());
    }
    if (!meanMetrics.containsKey(name)) {
      meanMetrics.put(name, Pair.of(0L, 0L));
    }

    Pair<Long, Long> pair = meanMetrics.get(name);

    // Left is accumulator, right is count
    long accumulator = pair.getLeft() + value;
    long count = pair.getRight() + 1;

    meanMetrics.put(name, Pair.of(accumulator, count));
  }

  /**
   * trace a metric
   *
   * @param name the metric name
   * @param value the metric value
   */
  public void trace(String name, long value) {
    final String index = createIndexIfNecessary();
    sendMeter(index, name, value);
  }

  /**
   * Only call from the timer
   */
  public void flushMetrics() {
    synchronized (flushLock) {
      final String index = createIndexIfNecessary();

      // flush meter
      toFlushMeter.clear();
      synchronized (this) {
        Iterator<Map.Entry<String, Long>> iter = meterMetrics.entrySet().iterator();
        while (iter.hasNext()) {
          Map.Entry<String, Long> entry = iter.next();
          toFlushMeter.put(entry.getKey(), entry.getValue());
        }
        meterMetrics.clear();
      }

      Iterator<Map.Entry<String, Long>> iter = toFlushMeter.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String, Long> entry = iter.next();
        sendMeter(index, entry.getKey(), entry.getValue());
      }

      // flush mean
      toFlushMean.clear();
      synchronized (this) {
        Iterator<Map.Entry<String, Pair<Long, Long>>> mIter = meanMetrics.entrySet().iterator();
        while (mIter.hasNext()) {
          Map.Entry<String, Pair<Long, Long>> entry = mIter.next();
          toFlushMean.put(entry.getKey(), entry.getValue());
        }
        meanMetrics.clear();
      }

      Iterator<Map.Entry<String, Pair<Long, Long>>> mIter = toFlushMean.entrySet().iterator();
      while (mIter.hasNext()) {
        Map.Entry<String, Pair<Long, Long>> entry = mIter.next();
        long accumulator = entry.getValue().getLeft();
        long count = entry.getValue().getRight();

        long mean = 0l;
        if (count > 0) {
          mean = accumulator / count;
        }
        sendMeter(index, entry.getKey(), mean);
      }
    }
  }

  private void sendMeter(String index, String name, long value) {
    final Date date = new Date();
    final String type = "_doc";
    final String id = String.valueOf(System.currentTimeMillis()) + String.format("%04d", random.nextInt(10000));

    String[] fields = name.split(";");
    Map<String, Object> m = new HashMap<>();
    m.put("date", sdf.format(date));
    m.put("key", fields[0]);
    m.put("value", value);
    m.put("host", hostname);
    int n = fields.length;
    if (n > 1) {
      for (int i = 1; i < n; ++i) {
        String[] additionalField = fields[i].split("=");
        m.put(additionalField[0], additionalField[1]);
      }
    }
    Gson gson = new Gson();
    try {
      restClient.performRequest("PUT", "/" + index + "/" + type + "/" + id, new HashMap<>(),
          new NStringEntity(gson.toJson(m), ContentType.APPLICATION_JSON));
    } catch (IOException e) {
      logger.warn(e.toString());
    }
    logger.info("meter: " + name + "=" + value);
  }

  private String createIndexIfNecessary() {
    final Date date = new Date();
    final String index = INDEX_PREFIX + sdf0.format(date);
    try {
      restClient.performRequest("GET", "/" + index);
    } catch (ResponseException e) {
      // create index if not found
      try {
        createIndex(index);
      } catch (IOException e1) {
        logger.error(e1.toString());
      }
    } catch (IOException e) {
      logger.warn(e.toString());
    }

    return index;
  }

  /**
   * Create index with dynamic template, map string to keyword for aggregation
   *
   * @param index
   * @throws IOException
   */
  private void createIndex(String index) throws IOException {
    restClient.performRequest("PUT", "/" + index, new HashMap<>(),
        new NStringEntity("{\n" +
            "  \"mappings\": {\n" +
            "    \"_doc\": {\n" +
            "      \"dynamic_templates\": [\n" +
            "        {\n" +
            "          \"strings_as_keywords\": {\n" +
            "            \"match_mapping_type\": \"string\",\n" +
            "            \"mapping\": {\n" +
            "              \"type\": \"keyword\"\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"properties\": {\n" +
            "        \"date\":   { \"type\": \"date\", \"format\": \"yyyy-MM-dd HH:mm:ss\" },\n" +
            "        \"key\":    { \"type\": \"keyword\" },\n" +
            "        \"value\":  { \"type\": \"long\" },\n" +
            "        \"host\":   { \"type\": \"keyword\" }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}", ContentType.APPLICATION_JSON));
  }

  private static String metricsNameByFields(String name, Map<String, Object> additionalFields) throws Exception{
    if (name.contains(";") || name.contains("="))
      throw new Exception("Metrics name contains ';' or '='");

    Iterator<Map.Entry<String, Object>> iter = additionalFields.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, Object> entry = iter.next();
      String key = entry.getKey();
      if (key.contains(";") || key.contains("="))
        throw new Exception("Additional field name contains ';' or '='");
      Object value = entry.getValue();
      if (value instanceof String) {
        if (((String) value).contains(";") || ((String) value).contains("="))
          throw new Exception("Additional field value contains ';' or '='");
        value = ((String) value).toLowerCase();
      }

      name = name + ";" + key + "=" + value;
    }

    return name;
  }

  /**
   * test
   */
  public static void main(String[] args) throws Exception {
    ESMetrics.init("chocolate-metrics-", "http://10.148.181.34:9200");
    ESMetrics metrics = ESMetrics.getInstance();

    for (int i = 0; i < 1000; i++) {
      metrics.meter("test");
      metrics.meter("test", "CLICK", null);
      metrics.meter("test", "CLICK", "EPN");
      Thread.sleep(10);
    }

    Thread.sleep(2000);
    System.out.println("finished");
  }
}
