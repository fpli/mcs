package com.ebay.traffic.chocolate;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.N1qlQueryRow;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DeleteUsers {
  private static final Logger logger = LoggerFactory.getLogger(DeleteUsers.class);
  private static final String N1QL_DELETE = "seed.couchbase.n1ql.query.delete";
  private static Properties seedPros;

  public static void main(String[] args)
      throws Exception {
    String configFilePath = (args != null) && (args.length > 0) ? args[0] : null;
    if (StringUtils.isEmpty(configFilePath)) {
      logger.error("No configFilePath was defined. please set configFilePath for seed job.");
      return;
    }
    Long time = (args != null) && (args.length > 1) ? Long.valueOf(args[1]) : null;
    if (time == null) {
      logger.error("No time was defined. please set time for seed job.");
      return;
    }

    init(configFilePath);

    run(configFilePath, time);

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

  private static void run(String configFilePath, Long time) throws IOException {
    CouchbaseClient cbClient = CouchbaseClient.getInstance(configFilePath);
    String queryString = String.format(seedPros.getProperty(N1QL_DELETE), time);
    logger.info("[DeleteUsers] - Couchbase QueryString is: " + queryString);
    System.out.println("[DeleteUsers] - Couchbase QueryString is: " + queryString);
    List<N1qlQueryRow> userList = null;
    try {
      userList = cbClient.query(queryString);
    } catch (Exception e) {
      logger.error("[DeleteUsers] - Error happened when query user_info with N1qlQuery: " + queryString);
      throw e;
    } finally {
      CouchbaseClient.close();
    }
    if ((userList == null) || (userList.size() == 0)) {
      logger.info("[DeleteUsers] - There is no userInfo in selected time with query string: " + queryString);
      return;
    }
    logger.info("[DeleteUsers] - Successfully deleted user number =" + userList.size());
  }
}
