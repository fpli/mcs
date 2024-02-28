package com.ebay.traffic.chocolate.util.metric;

import com.ebay.traffic.chocolate.util.Constants;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class MetricUtil {
  private static final Logger logger = LoggerFactory.getLogger(MetricUtil.class);
  private static final HttpClientBuilder builder = HttpClients.custom();
  public static String getToken() throws InterruptedException {
    String token = null;
    int retry = 0;
    while (retry < 3) {
      try {
        HttpClient httpClient = builder.build();
        HttpPost post = new HttpPost(Constants.OS_IDENTITY_END_POINT);
        post.setHeader("Content-Type", "application/json");
        post.setEntity(new StringEntity("{\"auth\": { \"passwordCredentials\": {\"username\": \"" + Constants.AUTH_USER_NAME + "\", \"password\": \"" + Constants.AUTH_PASSWD + "\"}}}", "UTF-8"));
        HttpResponse httpresponse = httpClient.execute(post);
        String rs = EntityUtils.toString(httpresponse.getEntity(), StandardCharsets.UTF_8);
        JSONObject jsonObject = new JSONObject(rs);
        JSONObject access = (JSONObject) jsonObject.get("access");
        JSONObject tokens = (JSONObject) access.get("token");
        token = tokens.get("id").toString();
        logger.info("token:" + token);
        return token;
      } catch (Exception e) {
        logger.error("get token exception");
        e.printStackTrace();
        logger.error(e.getMessage());
        retry++;
        Thread.sleep(2000);
      }
    }
    logger.info("token:" + token);
    return token;
  }
  
  public static String getSherlockMetric(String queryMetric, Long startTime, Long endTime, Long step, String token) throws InterruptedException {
    int retry = 0;
    while (retry < 3) {
      try {
        HttpClient httpClient = builder.build();
        String query = URLEncoder.encode(queryMetric, "UTF-8");
        String uri = Constants.SHERLOCK_METRIC_END_POINT + "/api/v1/query_range?query=" + query + "&start=" + startTime + "&end=" + endTime + "&step=" + step;
        logger.info("uri:" + uri);
        HttpGet get = new HttpGet(uri);
        get.setHeader("Authorization", "Bearer " + token);
        get.setHeader("Content-Type", "application/json");
        get.setHeader("Accept", "application/json");
        HttpResponse httpresponse = httpClient.execute(get);
        String res = EntityUtils.toString(httpresponse.getEntity(), StandardCharsets.UTF_8);
        logger.info("res:" + res);
        return res;
      } catch (Exception e) {
        logger.error("getSherlockData exception");
        e.printStackTrace();
        logger.error(e.getMessage());
        retry++;
        Thread.sleep(2000);
      }
    }
    return null;
  }
}
