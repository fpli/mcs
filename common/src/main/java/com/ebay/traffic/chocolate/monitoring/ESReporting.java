package com.ebay.traffic.chocolate.monitoring;

import com.google.gson.Gson;
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
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jialili1 on 11/6/18.
 *
 * ElasticSearch api for reporting.
 *
 * Send data to ElasticSearch directly.
 *
 * Doc id must be specified externally, or data will be duplicated if reporting job fails.
 *
 */
public class ESReporting {
  private static final Logger logger = LoggerFactory.getLogger(ESReporting.class);
  private RestClient restClient;

  /**
   * Singleton instance of ESReporting
   */
  private static ESReporting INSTANCE = null;

  private static String INDEX_PREFIX;

  private String hostname;

  ESReporting(String prefix) {
    this.INDEX_PREFIX = prefix;
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
    INSTANCE = new ESReporting(prefix);

    RestClientBuilder builder = RestClient.builder(httpHost);
    INSTANCE.restClient = builder.build();
  }

  /**
   * @return the instance of ESReporting
   */
  public static ESReporting getInstance() {
    return INSTANCE;
  }

  /**
   * Close the ES client
   */
  public void close() {
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
  private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  /**
   * send a reporting data
   */
  public void send(String key, long value, String docId) throws IOException{
    final String index = createIndexIfNecessary(-1);
    sendReport(index, key, value, docId, -1, null);
  }

  /**
   * send with data timestamp
   *
   * @param eventTime data timestamp
   */
  public void send(String key, long value, String docId, long eventTime) throws IOException{
    final String index = createIndexIfNecessary(eventTime);
    sendReport(index, key, value, docId, eventTime, null);
  }

  /**
   * send with additional fields
   *
   * @param additionalFields fields names except key and value
   */
  public void send(String key, long value, String docId, Map<String, Object> additionalFields) throws IOException{
    final String index = createIndexIfNecessary(-1);
    sendReport(index, key, value, docId, -1, additionalFields);
  }

  /**
   * send with data timestamp and additional fields
   */
  public void send(String key, long value, String docId, long eventTime, Map<String, Object> additionalFields) throws IOException{
    final String index = createIndexIfNecessary(eventTime);
    sendReport(index, key, value, docId, eventTime, additionalFields);
  }

  /**
   * send report directly
   *
   * @throws IOException
   */
  private void sendReport(String index, String key, long value, String docId, long eventTime, Map<String, Object> additionalFields) throws IOException{
    final String type = "_doc";
    final String date;
    String logName = key + ";";

    if (eventTime != -1) {
      date = sdf.format(eventTime);
    }
    else {
      date = sdf.format(new Date());
    }

    Map<String, Object> m = new HashMap<>();
    m.put("date", date);
    m.put("key", key);
    m.put("value", value);
    m.put("host", hostname);
    if (additionalFields != null) {
      Iterator<Map.Entry<String, Object>> iter = additionalFields.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String, Object> entry = iter.next();
        Object additionalValue = entry.getValue();
        if (additionalValue instanceof String) {
          additionalValue = ((String) additionalValue).toLowerCase();
        }
        m.put(entry.getKey(), additionalValue);
        logName = logName + entry.getKey() + "=" + additionalValue + ";";
      }
    }

    Gson gson = new Gson();
    restClient.performRequest("PUT", "/" + index + "/" + type + "/" + docId, new HashMap<>(),
        new NStringEntity(gson.toJson(m), ContentType.APPLICATION_JSON));
    logger.info("meter: " + logName + "=" + value);
  }

  private String createIndexIfNecessary(long eventTime) {
    final String index;
    if (eventTime != -1)
      index = INDEX_PREFIX + sdf0.format(eventTime);
    else
      index = INDEX_PREFIX + sdf0.format(new Date());
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

  /**
   * test
   */
  public static void main(String[] args) throws Exception {
    ESReporting.init("chocolate-reporting-", "http://10.148.181.34:9200");
    ESReporting reporting = ESReporting.getInstance();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long t = System.currentTimeMillis() - 200000000;
    System.out.println(sdf.format(t));

    Map<String, Object> additionalFields = new HashMap<>();
    additionalFields.put("channelType", "EPN");
    additionalFields.put("channelAction", "CLICK");

    for (int i = 0; i < 10; i++) {
      reporting.send("test", 1, "1");
      reporting.send("test", 1, "2", additionalFields);
      reporting.send("test", 1, "3", t);
      reporting.send("test", 1,"4", t, additionalFields);
      Thread.sleep(10);
    }

    reporting.send("test", 2, "5", t, additionalFields);

    Thread.sleep(2000);
    System.out.println("finished");
  }
}
