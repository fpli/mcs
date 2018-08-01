package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import org.apache.commons.lang3.StringUtils;


import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;


public class DeleteAllDataInBucket {
  private static final String CB_QUERY_STATEMENT_BY_TIME = "SELECT * FROM `rotation_info`";

  private static Cluster cluster;
  private static Bucket bucket;
  private static Properties couchbasePros;


  public static void main(String args[]) throws IOException {

    String env = (args == null || args.length <1) ? "qa" : args[0].toLowerCase();
    init(env);
    try {
      connect();
      deleteCouchbase();
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
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().connectTimeout(1000000).queryTimeout(5000000).build();
    cluster = CouchbaseCluster.create(env, couchbasePros.getProperty("couchbase.cluster.rotation"));
    cluster.authenticate(couchbasePros.getProperty("couchbase.user.rotation"), couchbasePros.getProperty("couchbase.password.rotation"));
    bucket = cluster.openBucket(couchbasePros.getProperty("couchbase.bucket.rotation"), 3000000, TimeUnit.SECONDS);
  }

  private static void deleteCouchbase(){
    N1qlQueryResult result = bucket.query(N1qlQuery.simple(CB_QUERY_STATEMENT_BY_TIME));
    for(N1qlQueryRow row : result.allRows()){
      JsonObject  rotationInfo = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
      String key = rotationInfo.getString(RotationConstant.FIELD_ROTATION_STRING);
      key = StringUtils.isNotEmpty(key) ? key : rotationInfo.getString(RotationConstant.FIELD_ROTATION_ID);
      bucket.remove(key);
    }

  }

  private static void close() {
    bucket.close();
    cluster.disconnect();
  }

}
