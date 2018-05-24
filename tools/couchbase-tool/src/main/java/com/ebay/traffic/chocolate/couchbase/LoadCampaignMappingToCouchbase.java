package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;



public class LoadCampaignMappingToCouchbase {
  private static Cluster cluster;
  private static Bucket bucket;
  private static Properties couchbasePros;

  public static void main(String args[]) throws IOException{
    init();
    connect();
    loadFileToCouchbase();
    close();
  }

  private static void init() throws IOException{
    couchbasePros = new Properties();
    InputStream in = Object.class.getResourceAsStream("/prod/couchbase.properties");
    couchbasePros.load(in);
  }

  private static void connect() {
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
        .connectTimeout(10000).queryTimeout(5000).build();
    cluster = CouchbaseCluster.create(env, couchbasePros.getProperty("couchbase.cluster"));
    cluster.authenticate(couchbasePros.getProperty("couchbase.user"),
        couchbasePros.getProperty("couchbase.password"));
    bucket = cluster.openBucket(couchbasePros.getProperty("couchbase.bucket"),
        300, TimeUnit.SECONDS);
  }

  private static void loadFileToCouchbase() {
    InputStream in = Object.class.getResourceAsStream("/" + couchbasePros.getProperty("mapping.file"));
    int count = 0;
    try {
        InputStreamReader isr = new InputStreamReader(in, "GBK");
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        while((line = br.readLine()) != null) {
          String[] result = line.split(",");
          if (!bucket.exists(result[0])) {
            bucket.upsert(StringDocument.create(result[0], result[1]));
            count++;
          }
        }
        br.close();
        isr.close();
    } catch (IOException e) {
      System.out.println("Error reading campaign publisher mapping file found");
    }
    System.out.println("Successfully load " + count + " records into couchbase!");
  }

  private static void close() {
    bucket.close();
    cluster.disconnect();
  }

}
