package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class LoadRotationInfoIntoCB {
  private static Cluster cluster;
  private static Bucket bucket;
  private static Properties couchbasePros;

  public static void main(String args[]) throws IOException{
    String env = args[0].toLowerCase(), existRotationIds_filePath = args[1];
//    String test = "﻿1185738442241";
//    System.out.println("\"" + test + "\"");
//    System.out.println("\"" + test.replaceAll("^﻿\\s+|\\s+$", "") + "\"");

    init(env);
    connect();
    loadFileToCouchbase(existRotationIds_filePath);
    close();
  }

  private static void init(String env) throws IOException {
    couchbasePros = new Properties();
    InputStream in = Object.class.getResourceAsStream("/" + env + "/couchbase.properties");
    couchbasePros.load(in);
  }

  private static void connect() {
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().connectTimeout(100000000L).queryTimeout(5000000000L).build();
    cluster = CouchbaseCluster.create(env, couchbasePros.getProperty("couchbase.cluster.rotation"));
    cluster.authenticate(couchbasePros.getProperty("couchbase.user.rotation"), couchbasePros.getProperty("couchbase.password.rotation"));
    bucket = cluster.openBucket(couchbasePros.getProperty("couchbase.bucket.rotation"), 3000000000000L, TimeUnit.SECONDS);
  }

  private static void loadFileToCouchbase(String fileName) throws FileNotFoundException {
    OutputStream out = new BufferedOutputStream(new FileOutputStream(fileName));
    InputStream in = Object.class.getResourceAsStream("/rotation_info.csv");
    int count = 0;
    try {
        InputStreamReader isr = new InputStreamReader(in, "UTF-8");
        BufferedReader br = new BufferedReader(isr);
        String line = null;
        RotationInfo rotationInfo = null;
        Gson gson = new Gson();
        while((line = br.readLine()) != null) {
          rotationInfo = getRotationInfo(line);
          if (!bucket.exists(rotationInfo.getRotation_string())) {
            bucket.upsert(StringDocument.create(rotationInfo.getRotation_string(), gson.toJson(rotationInfo)));
            count++;
          }else{
            try {
              out.write(rotationInfo.getRotation_string().getBytes());
              out.write(RotationConstant.RECORD_SEPARATOR);
              out.flush();
            }catch (IOException e){
                System.out.println("duplicate rotation_string");
            }
          }
        }
        br.close();
        isr.close();
        out.close();
    } catch (IOException e) {
      System.out.println("Error reading campaign publisher mapping file found");
    }
    System.out.println("Successfully load " + count + " records into couchbase!");
  }

  private static void close() {
    bucket.close();
    cluster.disconnect();
  }

  private static RotationInfo getRotationInfo(String row){
    String[] fields = row.split("\\|");
    RotationInfo rotationInfo = new RotationInfo();
    if(StringUtils.isNotEmpty(fields[0])){
      rotationInfo.setRotation_id(Long.valueOf(fields[0]));
    }
    if(StringUtils.isNotEmpty(fields[1])){
      rotationInfo.setRotation_string(strSpaceStrip(fields[1]));
    }
    if(StringUtils.isNotEmpty(fields[2])){
      rotationInfo.setRotation_name(strSpaceStrip(fields[2]));
    }
    if(StringUtils.isNotEmpty(fields[3])){
      rotationInfo.setCampaign_id(Long.valueOf(strSpaceStrip(fields[3])));
    }
    if(StringUtils.isNotEmpty(fields[5])){
      rotationInfo.setVendor_id(Integer.valueOf(strSpaceStrip(fields[5])));
    }
    if(StringUtils.isNotEmpty(fields[6])){
      rotationInfo.setVendor_name(strSpaceStrip(fields[6]));
    }
    if(StringUtils.isNotEmpty(fields[9])){
      rotationInfo.setRotation_description(strSpaceStrip(fields[9]));
    }
    if(StringUtils.isNotEmpty(fields[11])){
      rotationInfo.setChannel_id(Integer.valueOf(strSpaceStrip(fields[11])));
    }

    Map<String, String> rotationTag = new HashMap<String, String>();

    if(StringUtils.isNotEmpty(fields[7])){
      rotationTag.put(RotationConstant.FIELD_CLIENT_ID, strSpaceStrip(fields[7]));
    }
    if(StringUtils.isNotEmpty(fields[8])){
      rotationTag.put(RotationConstant.FIELD_CLIENT_NAME, strSpaceStrip(fields[8]));
    }
    if(StringUtils.isNotEmpty(fields[10])){
      rotationTag.put(RotationConstant.FIELD_PLACEMENT_ID, strSpaceStrip(fields[10]));
    }
    if(StringUtils.isNotEmpty(fields[12])){
      rotationTag.put(RotationConstant.FIELD_TAG_CHANNEL_NAME, strSpaceStrip(fields[12]));
    }
    if(StringUtils.isNotEmpty(fields[13])){
      rotationTag.put("portl_bkt_id", strSpaceStrip(fields[13]));
    }
    if(StringUtils.isNotEmpty(fields[14])){
      rotationTag.put("portl_sub_bkt_id", strSpaceStrip(fields[14]));
    }
    if(StringUtils.isNotEmpty(fields[13])){
      rotationTag.put("portl_bkt_id", strSpaceStrip(fields[13]));
    }
    if(StringUtils.isNotEmpty(fields[15])){
      rotationTag.put(RotationConstant.FIELD_ROTATION_START_DATE, strSpaceStrip(fields[15]).replaceAll("-", ""));
    }
    if(StringUtils.isNotEmpty(fields[16])){
      rotationTag.put(RotationConstant.FIELD_ROTATION_END_DATE, strSpaceStrip(fields[16]).replaceAll("-", ""));
    }
    if(StringUtils.isNotEmpty(fields[17])){
      rotationTag.put("rotation_sts_name", strSpaceStrip(fields[17]));
    }
    if(StringUtils.isNotEmpty(fields[18])){
      rotationTag.put("upd_date", strSpaceStrip(fields[18]));
    }
    if(StringUtils.isNotEmpty(fields[19])){
      rotationTag.put("cre_date", strSpaceStrip(fields[19]));
    }
    rotationInfo.setRotation_tag(rotationTag);
    return rotationInfo;
  }

  private static String strSpaceStrip(String field){
    return field.replaceAll("^\\s+|\\s+$", "");
  }
}
