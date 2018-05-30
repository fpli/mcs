package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;


import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;


public class DumpRotationFiles {
  private static final String CB_QUERY_STATEMENT_BY_TIME = "SELECT * FROM `rotation_info` WHERE last_update_time >= ";
  private static Cluster cluster;
  private static Bucket bucket;
  private static Properties couchbasePros;

  public static void main(String args[]) throws IOException {
    String env = args[0].toLowerCase(), output = args[1], lastUpdateTime = args[2], compress = args[3];
    init(env);
    try {
      connect();
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
      String fileName = output + "roatation-" + sdf.format(new Date()) + ".txt";
      dumpFileFromCouchbase(fileName, Long.valueOf(lastUpdateTime), Boolean.valueOf(compress));
    } finally {
      close();
    }
  }

  private static void init(String env) throws IOException {
    couchbasePros = new Properties();
    InputStream in = Object.class.getResourceAsStream("/" + env + "/couchbase.properties");
    couchbasePros.load(in);
  }

  private static void connect() {
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
        .connectTimeout(10000).queryTimeout(5000).build();
    cluster = CouchbaseCluster.create(env, couchbasePros.getProperty("couchbase.cluster.rotation"));
    cluster.authenticate(couchbasePros.getProperty("couchbase.user.rotation"), couchbasePros.getProperty("couchbase.password.rotation"));
    bucket = cluster.openBucket(couchbasePros.getProperty("couchbase.bucket.rotation"), 300, TimeUnit.SECONDS);
  }

  private static void dumpFileFromCouchbase(String output, Long lastUpdateTime, boolean compress) throws IOException {

    OutputStream out = null;
    Integer count = 0 ;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(output +  ".gz"), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(output));
      }

      N1qlQueryResult result = bucket.query(N1qlQuery.simple(CB_QUERY_STATEMENT_BY_TIME + lastUpdateTime));
      for (N1qlQueryRow row : result) {
        out.write(row.byteValue());
        out.write(RotationConstant.RECORD_SEPARATOR);
        out.flush();
        count++;
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to chocolate file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into chocolate file!");
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
