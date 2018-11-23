package com.ebay.traffic.chocolate;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.google.gson.Gson;
import javafx.util.Pair;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class CouchbaseClient {
  private static final Logger logger = Logger.getLogger(CouchbaseClient.class);
  private static volatile CouchbaseClient INSTANCE = null;
  private static Properties cbProp;
  private final Cluster cluster;
  private Bucket bucket;
  private Queue<Pair<String, Long>> buffer;
  private Gson gson = new Gson();


  private CouchbaseClient(String configFilePath) throws IOException {
    initCBProp(configFilePath);
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
        .connectTimeout(Long.valueOf(cbProp.getProperty("seed.couchbase.connect.timeout")))
        .queryTimeout(Long.valueOf(cbProp.getProperty("seed.couchbase.query.timeout")))
        .build();
    this.cluster = CouchbaseCluster.create(env, new String[]{cbProp.getProperty("seed.couchbase.cluster")});
    try {
      this.cluster.authenticate(cbProp.getProperty("seed.couchbase.user"), cbProp.getProperty("seed.couchbase.password"));
      this.bucket = this.cluster.openBucket(cbProp.getProperty("seed.couchbase.bucket"), 1200L, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.error("Couchbase init error", e);
      throw e;
    }
    this.buffer = new LinkedBlockingDeque();
  }

  public Bucket getBucket() {
    return bucket;
  }

  private static void initCBProp(String configFilePath) throws IOException {
    cbProp = new Properties();
    cbProp.load(new FileInputStream(configFilePath + "seed.properties"));
  }

  private static void init(String configFilePath) throws IOException {
    Validate.isTrue(INSTANCE == null, "Instance should be initialized only once", new Object[0]);
    INSTANCE = new CouchbaseClient(configFilePath);
    logger.info("Initial Couchbase cluster");
  }

  public static void init(CouchbaseClient client) {
    Validate.isTrue(INSTANCE == null, "Instance should be initialized only once", new Object[0]);
    INSTANCE = client;
  }

  public static CouchbaseClient getInstance(String configFilePath) throws IOException {
    if (INSTANCE == null) {
      synchronized (CouchbaseClient.class) {
        if (INSTANCE == null) {
          init(configFilePath);
        }
      }
    }
    return INSTANCE;
  }

  public static void close() {
    if (INSTANCE == null) {
      return;
    }
    INSTANCE.bucket.close();
    INSTANCE.cluster.disconnect();
    INSTANCE = null;
  }

  public synchronized void upsert(String userId, long timestamp) {
    flushBuffer();
    try {
      UserInfo userInfo = new UserInfo();
      userInfo.setUser_id(userId);
      userInfo.setActivity_ts(Long.valueOf(timestamp));
      this.bucket.upsert(JsonDocument.create(userId, JsonObject.fromJson(this.gson.toJson(userInfo))));
      logger.debug("Adding new mapping. userId=" + userId + " timestamp=" + timestamp);
    } catch (Exception e) {
      this.buffer.add(new Pair(userId, Long.valueOf(timestamp)));
      logger.warn("Couchbase upsert operation exception", e);
    }
  }

  public List<N1qlQueryRow> query(String queryString) {
    N1qlQueryResult result = this.bucket.query(N1qlQuery.simple(queryString));
    if ((result == null) || (result.allRows() == null) || (result.allRows().size() <= 0)) {
      return null;
    }
    return result.allRows();
  }

  private void flushBuffer() {
    try {
      while (!this.buffer.isEmpty()) {
        Pair<String, Long> kv = (Pair) this.buffer.peek();
        upsert((String) kv.getKey(), ((Long) kv.getValue()).longValue());
        this.buffer.poll();
      }
    } catch (Exception e) {
      logger.warn("Couchbase upsert operation exception", e);
    }
  }
}
