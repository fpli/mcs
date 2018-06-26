package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.ebay.app.raptor.chocolate.constant.MPLXClientEnum;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;


public class TestCBPerformance {
  private static Cluster cluster;
  private static Bucket bucket;
  private static Properties couchbasePros;

  static Logger logger = LoggerFactory.getLogger(TestCBPerformance.class);

  public static void main(String args[]) throws IOException{
    String env = (args != null && args.length >= 1) ?  args[0].toLowerCase() : "qa";
    if(StringUtils.isEmpty(env)) logger.error("No environment was defined. please set qa or prod");

    init(env);
    connect();
    loadDataToCouchbase();
    close();
  }

  private static void init(String env) throws IOException {
    couchbasePros = new Properties();
    InputStream in = Object.class.getResourceAsStream("/" + env + "/couchbase.properties");
    couchbasePros.load(in);
  }

  private static void connect() {
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().connectTimeout(100000000L).queryTimeout(5000000000L).build();
    cluster = CouchbaseCluster.create(env, couchbasePros.getProperty("couchbase.cluster.campaign"));
    cluster.authenticate(couchbasePros.getProperty("couchbase.user.campaign"), couchbasePros.getProperty("couchbase.password.campaign"));
    bucket = cluster.openBucket(couchbasePros.getProperty("couchbase.bucket.campaign"), 3000000000000L, TimeUnit.SECONDS);
  }

  private static final String HYPEN = "-";
  private static final String DATE_FORMAT = "yyyyMMdd";
  private static final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

  private static void loadDataToCouchbase() throws FileNotFoundException {
    ReportPojo rp = null;
    Calendar calendar = Calendar.getInstance();
    Random random = new Random();
    String key = null;
    Gson gson = new Gson();

    for(int i=0; i < 500; i++){
      rp = new ReportPojo();
      key = ThreadLocalRandom.current().nextLong(1000000000L, 7000000000L) + HYPEN + calendar.getTimeInMillis();
      rp.setTimestamp(calendar.getTimeInMillis());
      rp.setDay(Integer.valueOf(sdf.format(calendar.getTime())));
      rp.setPublisher_id(6526542356L);
      rp.setClickCnt(ThreadLocalRandom.current().nextLong(1000000,9999999));
      rp.setValidClickCnt(ThreadLocalRandom.current().nextLong(1000000,9999999));
      rp.setImpCnt(ThreadLocalRandom.current().nextLong(1000000,9999999));
      rp.setValidImpCnt(ThreadLocalRandom.current().nextLong(1000000,9999999));
      rp.setViewImpCnt(ThreadLocalRandom.current().nextLong(1000000,9999999));
      rp.setValidViewImpCnt(ThreadLocalRandom.current().nextLong(1000000,9999999));
      rp.setMobileClickCnt(ThreadLocalRandom.current().nextLong(1000000,9999999));
      rp.setMobileImpCnt(ThreadLocalRandom.current().nextLong(1000000,9999999));
      rp.setMobileValidClickCnt(ThreadLocalRandom.current().nextLong(1000000,9999999));
      rp.setMobileValidImpCnt(ThreadLocalRandom.current().nextLong(1000000,9999999));
      bucket.upsert(StringDocument.create(key, gson.toJson(rp)));
      calendar.add(Calendar.MINUTE, 5);
    }
  }

  private static void close() {
    bucket.close();
    cluster.disconnect();
  }
}
