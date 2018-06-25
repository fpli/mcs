package com.ebay.traffic.chocolate.monitoring;

import com.google.gson.Gson;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.log4j.Logger;
import org.elasticsearch.client.*;

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
  private static final Logger logger = Logger.getLogger(ESMetrics.class);

  private RestClient restClient;

  /**
   * Singleton instance of ESMetrics
   */
  private static ESMetrics INSTANCE = null;

  private final static String INDEX_PREFIX = "chocolate-metrics-";

  /**
   * The timer
   */
  private final Timer timer;

  /**
   * Hashmap to aggregate metrics locally
   */
  private final Map<String, Long> metrics = new HashMap<>();

  /**
   * Hashmap for flush purpose
   */
  private final Map<String, Long> toFlush = new HashMap<>();

  private String hostname;

  ESMetrics() {
    timer = new Timer();
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      logger.warn(e);
    }
  }

  /**
   * Init the ElasticSearch client
   *
   * @param esHostname the hostname of ES cluster
   * @param esPort the port of ES cluster
   * @param scheme http scheme
   */
  public static synchronized void init(String esHostname, int esPort, String scheme) {
    if (INSTANCE != null) {
      return;
    }
    INSTANCE = new ESMetrics();

    RestClientBuilder builder = RestClient.builder(new HttpHost(esHostname, esPort, scheme));
    INSTANCE.restClient = builder.build();

    // Start the timer.
    INSTANCE.timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        INSTANCE.flushMetrics();
      }
    }, 10000, 10000);
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
    }
    try {
      restClient.close();
    } catch (IOException e) {
      // ignore
    }
  }

  private final SimpleDateFormat sdf0 = new SimpleDateFormat("yyyy.MM.dd");
  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private final Random random = new SecureRandom();

  /**
   * meter
   *
   * @param name the metric name
   */
  public void meter(String name) {
    meter(name, 1L);
  }

  /**
   * meter
   *
   * @param name the metric name
   * @param value the metric value
   */
  public synchronized void meter(String name, long value) {
    if (!metrics.containsKey(name)) {
      metrics.put(name, 0L);
    }
    long meter = metrics.get(name);
    meter += value; // aggregate locally
    metrics.put(name, meter);
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
  private void flushMetrics() {
    final String index = createIndexIfNecessary();

    toFlush.clear();
    synchronized (this) {
      Iterator<Map.Entry<String, Long>> iter = metrics.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String, Long> entry = iter.next();
        toFlush.put(entry.getKey(), entry.getValue());
      }
      metrics.clear();
    }

    Iterator<Map.Entry<String, Long>> iter = toFlush.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, Long> entry = iter.next();
      sendMeter(index, entry.getKey(), entry.getValue());
    }
  }

  private void sendMeter(String index, String name, long value) {
    final Date date = new Date();
    final String type = "_doc";
    final String id = String.valueOf(System.currentTimeMillis()) + String.format("%04d", random.nextInt(10000));
    System.out.println(id);
    KVMetric m = new KVMetric(sdf.format(date), name , value, hostname);
    Gson gson = new Gson();
    try {
      restClient.performRequest("PUT", "/" + index + "/" + type + "/" + id, new HashMap<>(),
              new NStringEntity(gson.toJson(m), ContentType.APPLICATION_JSON));
    } catch (IOException e) {
      logger.warn(e);
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
        logger.error(e1);
      }
    } catch (IOException e) {
      logger.warn(e);
    }

    return index;
  }

  private void createIndex(String index) throws IOException {
    restClient.performRequest("PUT", "/" + index, new HashMap<>(),
            new NStringEntity("{\"mappings\":{\"_doc\":{\"properties\":{\"date\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd HH:mm:ss\"},\"key\":{\"type\":\"text\"},\"value\":{\"type\":\"long\"},\"host\":{\"type\":\"text\"}}}}}", ContentType.APPLICATION_JSON));
  }

  private static class KVMetric {
    private String date;
    private String key;
    private long value;
    private String host;

    KVMetric(String date, String key, long value, String host) {
      this.date = date;
      this.key = key;
      this.value = value;
      this.host = host;
    }
  }

  /**
   * test
   */
  public static void main(String[] args) throws Exception {
    ESMetrics.init("10.148.185.16", 9200, "http");
    ESMetrics metrics = ESMetrics.getInstance();

    for (int i = 0; i < 1000; i++) {
      metrics.meter("test1");
      Thread.sleep(10);
    }
    Thread.sleep(2000);
    System.out.println("finished");
  }
}
