package com.ebay.traffic.chocolate;

import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.ebay.app.raptor.chocolate.common.Hostname;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SeedJob {
  private static final Logger logger = LoggerFactory.getLogger(SeedJob.class);
  private static final String ES_PREFIX_PROP = "seed.elasticsearch.prefix";
  private static final String ES_URL_PROP = "seed.elasticsearch.url";
  private static final String PJ_URL_PROP = "seed.purchasejourney.url";
  private static final String N1QL_QUERY = "seed.couchbase.n1ql.query";
  private static final byte RECORD_SEPARATOR = '\n';
  private static final String EMPTY_JSON_RECORD = "{}";
  private static final String EMPTY = "";
  private static Properties seedPros;
  private static Gson gson = new Gson();

  public static void main(String[] args)
      throws Exception {
    String configFilePath = (args != null) && (args.length > 0) ? args[0] : null;
    if (StringUtils.isEmpty(configFilePath)) {
      logger.error("No configFilePath was defined. please set configFilePath for seed job.");
    }
    Long time = (args != null) && (args.length > 1) ? Long.valueOf(args[1]) : null;
    if (time == null) {
      logger.error("No time was defined. please set time for seed job.");
    }
    Long window = (args != null) && (args.length > 2) ? Long.valueOf(args[2]) : null;
    if (window == null) {
      logger.error("No window was defined. please set window for seed job.");
    }
    String outputPath = (args != null) && (args.length > 3) ? args[3] : null;
    if (StringUtils.isEmpty(outputPath)) {
      logger.error("No outputPath was defined. please set outputPath for seed job.");
    }
    init(configFilePath);

    run(configFilePath, time, window, outputPath);

    System.exit(0);
  }

  private static void init(String configFilePath)
      throws IOException {
    seedPros = new Properties();
    Properties log4jProps = new Properties();
    try {
      seedPros.load(new FileInputStream(configFilePath + "seed.properties"));
      log4jProps.load(new FileInputStream(configFilePath + "log4j.properties"));
      PropertyConfigurator.configure(log4jProps);
    } catch (IOException e) {
      logger.error("Can't load seed properties");
      throw e;
    }
  }

  private static void run(String confPath, Long time, Long window, String outputPath)
      throws Exception {
    ESMetrics.init(seedPros.getProperty(ES_PREFIX_PROP), seedPros.getProperty(ES_URL_PROP));
    ESMetrics esMetrics = ESMetrics.getInstance();

    CouchbaseClient cbClient = CouchbaseClient.getInstance(confPath);

    long endTime = System.currentTimeMillis() - time.longValue() * 1000L;
    long startTime = endTime - window.longValue() * 1000L;
    String queryString = String.format(seedPros.getProperty(N1QL_QUERY), startTime, endTime);
    logger.info("Couchbase QueryString is: " + queryString);
    System.out.println("Couchbase QueryString is: " + queryString);
    List<N1qlQueryRow> userList = null;
    try {
      userList = cbClient.query(queryString);
    } catch (Exception e) {
      logger.error("Error happened when query user_info with N1qlQuery: " + queryString);
      throw e;
    } finally {
      CouchbaseClient.close();
    }
    if ((userList == null) || (userList.size() == 0)) {
      logger.info("There is no userInfo in selected time with query string: " + queryString);
      esMetrics.meter("SeedWithLatency", 0L);
      return;
    }
    logger.info("User List Size in Last " + time.longValue() / 60L + " minutes: " + userList.size());

    String filePath = outputPath + Hostname.HOSTNAME + "_" + endTime + ".json";

    final CountDownLatch begin = new CountDownLatch(1);
    final CountDownLatch end = new CountDownLatch(userList.size());

    String pjURLTemplate = seedPros.getProperty(PJ_URL_PROP);
    ExecutorService exec = Executors.newFixedThreadPool(20);
    for (N1qlQueryRow row : userList) {
      Runnable run = () -> {
        try {
          begin.await();
          SeedJob.writeResult(row,pjURLTemplate,filePath);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }finally {
          end.countDown();
        }
      };
      exec.submit(run);
    }
    begin.countDown();
    end.await();
    exec.shutdown();

    logger.info("Successfully generate " + userList.size() + " records to " + filePath);
    esMetrics.flushMetrics();
  }

  private static void writeResult(N1qlQueryRow row, String pjURLTemplate, String filePath){
    BufferedWriter out = null;
    try {
      ESMetrics esMetrics = ESMetrics.getInstance();
      out = new BufferedWriter(new FileWriter(new File(filePath), true));
      JsonObject userInfo = row.value().getObject("user_info");
      String userId = userInfo.getString("user_id");
      String pjURL = String.format(pjURLTemplate, userId);
      String pjResp = RestClient.get(pjURL);
      String temp = StringUtils.isEmpty(pjResp) ? null : pjResp.replace(EMPTY_JSON_RECORD, EMPTY);
      if (StringUtils.isNotEmpty(temp)) {
        out.write(pjResp);
        out.write(RECORD_SEPARATOR);
        out.flush();
        esMetrics.meter("SeedWithLatency-Seed", 1L);
        JsonArray groups = JsonObject.fromJson(pjResp).getArray("groups");
        if ((groups == null) || (groups.size() == 0)) {
          esMetrics.meter("SeedWithLatency-GroupEmpty", 1L);
          logger.info("[EmptyGroups-1] : " + pjURL);
        } else {
          esMetrics.mean("SeedWithLatency-AvgGroupSize", groups.size());
          sendMetricsFor2Groups(groups);
        }
      } else {
        esMetrics.meter("SeedWithLatency-Empty", 1L);
        logger.info("[EmptyGroups-2] : " + pjURL);
      }
    } catch (IOException e) {
      logger.error("error happened when write PJ result to files.");
    } finally {
      try {
        if(out != null) out.close();
      } catch (IOException e) {
        logger.error("error happened when close output stream to file.");
      }
    }
  }

  private static void sendMetricsFor2Groups(JsonArray groups) {
    ESMetrics esMetrics = ESMetrics.getInstance();
    Group[] twoGroups = get1st2ndGroup(groups);
    if (twoGroups[0] != null) {
      esMetrics.mean("SeedWithLatency-Avg1stItemSize", twoGroups[0].getItems().length);
      esMetrics.mean("SeedWithLatency-Avg1stScore", (long) (twoGroups[0].getScore().floatValue() * 10000));
      if (twoGroups[0].getItems().length >= 1) {
        esMetrics.meter("SeedWithLatency-1stItemSizeLarger1", 1L);
      }
    } else {
      esMetrics.meter("SeedWithLatency-Empty", 1L);
    }
    if (twoGroups[1] != null) {
      esMetrics.mean("SeedWithLatency-2ndItemSize", twoGroups[1].getItems().length);
      esMetrics.mean("SeedWithLatency-Avg2ndScore", (long) (twoGroups[1].getScore().floatValue() * 10000));
      if (twoGroups[1].getItems().length >= 1) {
        esMetrics.meter("SeedWithLatency-2ndItemSizeLarger2", 1L);
      }
    } else {
      esMetrics.meter("SeedWithLatency-2ndEmpty", 1L);
    }
  }

  private static Group[] get1st2ndGroup(JsonArray groups) {
    float maxScore = 0.0F;
    float secondScore = 0.0F;
    float tempScore = 0.0F;
    Group[] twoGroup = new Group[2];
    for (int i = 0; i < groups.size(); i++) {
      Group group = gson.fromJson(groups.getObject(i).toString(), Group.class);
      tempScore = group.getScore().floatValue();
      if (tempScore > maxScore) {
        maxScore = tempScore;
        twoGroup[0] = group;
      }
      if ((tempScore >= secondScore) && (tempScore <= maxScore) && (i > 0)) {
        secondScore = tempScore;
        twoGroup[1] = group;
      }
    }
    return twoGroup;
  }
}
