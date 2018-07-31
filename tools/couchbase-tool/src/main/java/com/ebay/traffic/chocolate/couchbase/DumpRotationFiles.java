package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;


public class DumpRotationFiles {
  private static Cluster cluster;
  private static Bucket bucket;
  private static Properties couchbasePros;
  static Logger logger = LoggerFactory.getLogger(DumpLegacyRotationFiles.class);

  public static void main(String args[]) throws IOException {
    String configFilePath = (args != null && args.length > 0) ? args[0] : null;
    if(StringUtils.isEmpty(configFilePath)) logger.error("No configFilePath was defined. please set configFilePath for rotation jobs");

    String lastUpdateTime = (args != null && args.length > 1) ? args[1] : null;
    if(StringUtils.isEmpty(lastUpdateTime)) logger.error("No lastUpdateTime was defined. please set lastUpdateTime for rotation jobs");

    String outputFilePath = (args != null && args.length > 2) ? args[2] : null;

    init(configFilePath);

    try {
      connect();
      dumpFileFromCouchbase(lastUpdateTime, outputFilePath);
    } finally {
      close();
    }
  }

  private static void init(String configFilePath) throws IOException {
    couchbasePros = new Properties();
    InputStream in = new FileInputStream(configFilePath);
    couchbasePros.load(in);
  }

  private static void connect() {
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
        .connectTimeout(Long.valueOf(couchbasePros.getProperty("couchbase.timeout")))
        .queryTimeout(Long.valueOf(couchbasePros.getProperty("couchbase.timeout"))).build();
    cluster = CouchbaseCluster.create(env, couchbasePros.getProperty("couchbase.cluster.rotation"));

    cluster.authenticate(couchbasePros.getProperty("couchbase.user.rotation"), couchbasePros.getProperty("couchbase.password.rotation"));
    bucket = cluster.openBucket(couchbasePros.getProperty("couchbase.bucket.rotation"), Long.valueOf(couchbasePros.getProperty("couchbase.timeout")), TimeUnit.SECONDS);
  }

  private static void dumpFileFromCouchbase(String lastUpdateTime, String outputFilePath) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    if(outputFilePath == null){
      outputFilePath = couchbasePros.getProperty("job.dumpRotationFiles.outputFilePath");
    }
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(Long.valueOf(lastUpdateTime));
    outputFilePath = outputFilePath + "rotation-" + sdf.format(c.getTime()) + ".txt";
    Boolean compress = (couchbasePros.getProperty("job.dumpRotationFiles.compressed") == null) ?  Boolean.valueOf(couchbasePros.getProperty("job.dumpRotationFiles.compressed")) : Boolean.FALSE;

    OutputStream out = null;
    Integer count = 0 ;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(outputFilePath +  ".gz"), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(outputFilePath));
      }

      String queryStr = couchbasePros.getProperty("job.dumpRotationFiles.n1ql");
      queryStr = String.format(queryStr, lastUpdateTime);

      N1qlQueryResult result = bucket.query(N1qlQuery.simple(queryStr));
      JsonObject rotationInfo;
      for (N1qlQueryRow row : result) {
        rotationInfo = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
        if(rotationInfo == null) continue;
        out.write(String.valueOf(rotationInfo).getBytes());
        out.write(RotationConstant.RECORD_SEPARATOR);
        out.flush();
        count++;
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to chocolate file");
      throw e;
    } finally {
      if(out != null){
        out.close();
      }
    }
    System.out.println("Successfully dump " + count + " records into chocolate file: " + outputFilePath);
  }

  private static void close() {
    if(bucket != null){
      bucket.close();
    }
    if(cluster != null){
      cluster.disconnect();
    }
  }
}
