package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.dukes.CacheClient;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

public class RotationSnapshotToHadoop {
  static Logger logger = LoggerFactory.getLogger(RotationSnapshotToHadoop.class);
  private static Properties couchbasePros;
  private static CorpRotationCouchbaseClient corp_cb_client;
  private static Bucket corp_bucket;

  public static void main(String args[]) throws Exception {

    String configFilePath = (args != null && args.length > 0) ? args[0] : null;
    if (StringUtils.isEmpty(configFilePath)) {
      logger.error("No configFilePath was defined. please set configFilePath for rotation jobs");
    }
    String outputFilePath = (args != null && args.length > 1) ? args[1] : null;
    String startKey = (args != null && args.length > 2) ? args[2] : null;
    Long startKeyLong = 0L;
    if (StringUtils.isEmpty(startKey)) {
      Calendar c = Calendar.getInstance();
      c.set(Calendar.YEAR, 2018);
      c.set(Calendar.MONTH, 1);
      startKeyLong = c.getTimeInMillis();
    } else {
      startKeyLong = Long.valueOf(startKey);
    }

    init(configFilePath);

    try {
      corp_cb_client = new CorpRotationCouchbaseClient(couchbasePros);
      CacheClient cacheClient = corp_cb_client.getCacheClient();
      corp_bucket = corp_cb_client.getBuctet(cacheClient);
      // dump snapshot
      dumpSnapshot(outputFilePath, startKeyLong);
      corp_cb_client.returnClient(cacheClient);
    } finally {
      close();
    }
  }


  private static void init(String configFilePath) throws IOException {
    couchbasePros = new Properties();
    InputStream in = new FileInputStream(configFilePath);
    couchbasePros.load(in);
  }

  public static void dumpSnapshot(String outputFilePath, Long startKey) throws IOException {
    ESMetrics.init("batch-metrics-", couchbasePros.getProperty("chocolate.elasticsearch.url"));
    ESMetrics esMetrics = ESMetrics.getInstance();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    if (outputFilePath == null) {
      outputFilePath = couchbasePros.getProperty("job.dumpRotationFiles.outputFilePath");
    }
    outputFilePath = outputFilePath + "rotation-" + sdf.format(System.currentTimeMillis()) + ".txt";
    Boolean compress = (couchbasePros.getProperty("job.dumpRotationFiles.compressed") == null) ? Boolean.valueOf(couchbasePros.getProperty("job.dumpRotationFiles.compressed")) : Boolean.FALSE;

    OutputStream out = null;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(outputFilePath + ".gz"), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(outputFilePath));
      }
      ViewQuery query = ViewQuery.from(couchbasePros.getProperty("couchbase.corp.rotation.designName"),
          couchbasePros.getProperty("couchbase.corp.rotation.viewName"));
      query.startKey(Long.valueOf(startKey));

      ViewResult result = corp_bucket.query(query);
      String rotationInfo;
      for (ViewRow row : result) {
        rotationInfo = row.value().toString();
        if (rotationInfo == null) continue;
        out.write(String.valueOf(rotationInfo).getBytes());
        out.write(RotationConstant.RECORD_SEPARATOR);
        out.flush();
        count++;
      }
    } catch (IOException e) {
      esMetrics.meter("rotation.dump.dumpSnapshot.error");
      System.out.println("Error happened when write couchbase data to chocolate file");
      throw e;
    } finally {
      if (out != null) {
        out.close();
      }
    }
    esMetrics.meter("rotation.dump.dumpSnapshot.success", count);
    esMetrics.flushMetrics();
    System.out.println("Successfully dump " + count + " records into chocolate file: " + outputFilePath);
  }

  private static void close() {
    if (corp_cb_client != null) {
      corp_cb_client.shutdown();
    }
    if (corp_bucket != null) {
      corp_bucket.close();
    }
    System.exit(0);
  }
}
