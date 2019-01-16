package com.ebay.traffic.chocolate.couchbase;

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
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.dukes.CacheClient;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class RotationXDCR {
  static Logger logger = LoggerFactory.getLogger(RotationXDCR.class);
  private static Properties couchbasePros;
  private static Cluster choco_cluster;
  private static Bucket choco_bucket;
  private static CorpRotationCouchbaseClient corp_cb_client;
  private static Bucket corp_bucket;

  public static void main(String args[]) throws Exception {

    String configFilePath = args != null ? args[0] : null;
    init(configFilePath);

    try {
      // connect to chocolate cb
      connectChocolateCB();
      // XDCR
      upsertData();
    } catch (Exception e) {
      logger.error(e.getMessage());
      throw e;
    } finally {
      close();
    }
  }


  private static void init(String configFilePath) throws IOException {
    couchbasePros = new Properties();
    InputStream in = new FileInputStream(configFilePath);
    couchbasePros.load(in);
  }

  private static void connectChocolateCB() {
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
        .connectTimeout(Long.valueOf(couchbasePros.getProperty("couchbase.timeout")))
        .queryTimeout(Long.valueOf(couchbasePros.getProperty("couchbase.timeout"))).build();
    choco_cluster = CouchbaseCluster.create(env, couchbasePros.getProperty("couchbase.cluster.rotation"));
    choco_cluster.authenticate(couchbasePros.getProperty("couchbase.user.rotation"), couchbasePros.getProperty("couchbase.password.rotation"));
    choco_bucket = choco_cluster.openBucket(couchbasePros.getProperty("couchbase.bucket.rotation"), 300000000L, TimeUnit.SECONDS);
  }

  public static void upsertData() {
    ESMetrics.init("batch-metrics-", couchbasePros.getProperty("chocolate.elasticsearch.url"));
    Metrics esMetrics = ESMetrics.getInstance();

    String choco_n1qlQueryString = couchbasePros.getProperty("job.dumpRotationFiles.n1ql");
    logger.info("couchbase n1qlQueryString = " + choco_n1qlQueryString);
    N1qlQueryResult result = choco_bucket.query(N1qlQuery.simple(choco_n1qlQueryString));

    if (result == null) return;

    List<N1qlQueryRow> list = result.allRows();
    logger.info( "total records in chocolate couchbase: " + list.size());

    // connect to corp cb
    corp_cb_client = new CorpRotationCouchbaseClient(couchbasePros);
    CacheClient cacheClient = corp_cb_client.getCacheClient();
    corp_bucket = corp_cb_client.getBuctet(cacheClient);

    JsonObject jsonObject = null;
    int updateCnt = 0;
    for (N1qlQueryRow row : list) {
      jsonObject = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
      if (jsonObject == null) continue;

      String rotationStr = jsonObject.getString(RotationConstant.FIELD_ROTATION_STRING);
      if (StringUtils.isEmpty(rotationStr)) continue;

      corp_bucket.upsert(JsonDocument.create(rotationStr, jsonObject));
      updateCnt++;
    }
    logger.info( "updated rotation info in corp couchbase: " + updateCnt);

    esMetrics.meter("rotation.dump.xdcr.total", updateCnt);
    esMetrics.flush();
    corp_cb_client.returnClient(cacheClient);
  }

  private static void close() {
    if (corp_cb_client != null) {
      corp_cb_client.shutdown();
    }
    if (choco_bucket != null) {
      choco_bucket.close();
    }
    System.exit(0);
  }
}
