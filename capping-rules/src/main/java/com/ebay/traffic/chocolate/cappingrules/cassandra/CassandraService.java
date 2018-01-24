package com.ebay.traffic.chocolate.cappingrules.cassandra;

import com.ebay.traffic.chocolate.cappingrules.constant.ReportType;
import com.ebay.traffic.chocolate.report.cassandra.RawReportRecord;
import com.ebay.traffic.chocolate.report.constant.Env;
import com.google.gson.Gson;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

public class CassandraService {
  private static final String OAUTH_SVC_PATH =
      "/idauth/site/token?client_id=urn:ebay-marketplace-consumerid:c81d6f26-6600-4e61-ac56-e1987122efc5&client_secret=d1e63a54-a36d-42f3-ab5f-f366982881da&grant_type=client_credentials&scope=https://api.ebay.com/oauth/scope/@public%20https://api.ebay.com/oauth/scope/core@application";
  private static final String CHOCO_SVC_PATH = "/marketing/tracking/v1/report/gen?reporttype=";
  private static Logger logger = LoggerFactory.getLogger(CassandraService.class);
  private static CassandraService cassandraService = new CassandraService();

  public static CassandraService getInstance(){ return cassandraService; }

  public static String getOauthToken(URL oauthURL) throws IOException {
    HttpURLConnection conn = null;
    String chocoAuth = null;
    try {
      //URL oauthUrl = new URL(oauthURL);
      conn = (HttpURLConnection) oauthURL.openConnection();
      conn.setRequestMethod("GET");
      conn.setRequestProperty("Accept", "application/json");

      if (conn.getResponseCode() != 200) {
        throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
      }

      String output = IOUtils.toString(conn.getInputStream());
      logger.info(output);
      chocoAuth = output.substring(output.indexOf("\"access_token\": ") + 18);
      chocoAuth = chocoAuth.substring(0, chocoAuth.indexOf("\""));
      logger.info(chocoAuth);
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
    }
    return chocoAuth;
  }

  public static URL getCassandraSvcEndPoint(ApplicationOptions applicationOptions, ReportType reportType)
      throws IOException {
    String endpoint = applicationOptions.getByNameString(ApplicationOptions.CHOCO_CASSANDRA_SVC_END_POINT) +
        CHOCO_SVC_PATH + reportType.name();
    return new URL(endpoint);
  }

  public static URL getOauthSvcEndPoint(ApplicationOptions applicationOptions)
      throws IOException {
    String endpoint = applicationOptions.getByNameString(ApplicationOptions.CHOCO_OAUTH_SVC_END_POINT) + OAUTH_SVC_PATH;
    return new URL(endpoint);
  }

  public static void main(String[] arg) throws Exception {
    String env = null;
    if (arg != null && arg.length > 0) {
      env = arg[0];
    } else {
      env = Env.QA.name();
    }

    ApplicationOptions.init("cassandra.properties", env);
    ApplicationOptions applicationOptions = ApplicationOptions.getInstance();

    URL oauthSvcURL = CassandraService.getOauthSvcEndPoint(applicationOptions);
    String oauthToken = CassandraService.getOauthToken(oauthSvcURL);

    CassandraService cassandraService = new CassandraService();
    //CampaignReport
    URL campaignURL = CassandraService.getCassandraSvcEndPoint(applicationOptions, ReportType.CAMPAIGN);
    RawReportRecord reportRecord = new RawReportRecord(98765432102l, 201712, 20171212, 98765432100001l,
        698765432100001l, 1, 2, 3, 5, 8, 11, 19, 20, 21, 22);
    cassandraService.saveReportRecord(oauthToken, campaignURL, reportRecord);
    //PartnerReport
    URL partnerURL = CassandraService.getCassandraSvcEndPoint(applicationOptions, ReportType.CAMPAIGN);
    cassandraService.saveReportRecord(oauthToken, partnerURL, reportRecord);
  }

  public void saveReportRecordList(String oauthToken, URL chocorptURL, List<RawReportRecord> reportRecordList) throws
      Exception {
    for (RawReportRecord record : reportRecordList) {
      saveReportRecord(oauthToken, chocorptURL, record);
    }
  }

  public void saveReportRecord(String chocoAuth, URL chocorptURL, RawReportRecord reportRecord) throws IOException {

    HttpURLConnection chocorptConn = null;
    try {
      chocorptConn = (HttpURLConnection) chocorptURL.openConnection();
      chocorptConn.setConnectTimeout(5000);
      chocorptConn.setDoOutput(true);
      chocorptConn.setDoInput(true);
      chocorptConn.setRequestProperty("Content-Type", "application/json");
      chocorptConn.setRequestProperty("Authorization", "bearer " + chocoAuth);
      chocorptConn.setRequestMethod("POST");

      String requestPayload = new Gson().toJson(reportRecord);
      OutputStream os = chocorptConn.getOutputStream();
      os.write(requestPayload.getBytes());
      os.flush();

      if (chocorptConn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        logger.error(chocorptConn.getErrorStream().toString());
        throw new RuntimeException("Failed : HTTP error code : " + chocorptConn.getResponseCode());
      }

      String response = IOUtils.toString(chocorptConn.getInputStream());
      logger.info("cassandraRecord == " + response);
    } catch (IOException e) {
      logger.error(e.getMessage());
      throw e;
    } finally {
      if (chocorptConn != null) {
        chocorptConn.disconnect();
      }
    }
  }
}
