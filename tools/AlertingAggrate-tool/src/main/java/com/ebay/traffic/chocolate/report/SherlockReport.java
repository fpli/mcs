package com.ebay.traffic.chocolate.report;

import com.ebay.traffic.chocolate.pojo.*;
import com.ebay.traffic.chocolate.util.Constants;
import com.ebay.traffic.chocolate.util.NumUtil;
import com.ebay.traffic.chocolate.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;

public class SherlockReport {
    private static final Logger logger = LoggerFactory.getLogger(SherlockReport.class);
    private static final SherlockReport report = new SherlockReport();
    private static final HttpClientBuilder builder = HttpClients.custom();
    private String date;
    private Long startTime;
    private Long endTime;
    private Long step;

    public void init(String runPeriod) {
        this.startTime = System.currentTimeMillis()/1000;
        this.endTime = this.startTime;
        if("daily".equalsIgnoreCase(runPeriod)){
            this.step = 86400L;
            this.date = TimeUtil.getYesterday();
        }else if("hourly".equalsIgnoreCase(runPeriod)){
            this.step = 3600L;
            this.date = TimeUtil.getHour(System.currentTimeMillis());
        }
    }

    public static SherlockReport getInstance() {
        return report;
    }

    public void search(LinkedList<SherlockProject> projectsList){
        String token = null;
        int i = 3;
        while ( i>0 && token == null) {
            token = getToken();
            i--;
        }
        if(token == null){
            return;
        }
        for (SherlockProject pro : projectsList) {
            ArrayList<SherlockMetric> list = pro.getList();
            for (SherlockMetric metric : list){
                try {
                    logger.info("metric:" + metric.toString());
                    String rs = getSherlockData(metric,token);
                    logger.info("sherlock result:" + rs);
                    if(rs == null){
                        continue;
                    }
                    JSONObject jsonObject = new JSONObject(rs);
                    String status = jsonObject.get("status").toString();
                    metric.setDate(this.date);
                    if(!"success".equalsIgnoreCase(status)){
                        continue;
                    }
                    JSONObject data = (JSONObject) jsonObject.get("data");
                    if(data == null){
                        continue;
                    }
                    JSONArray results = (JSONArray) data.get("result");
                    if(results == null || results.isEmpty()){
                        metric.setValue("0");
                    }else {
                        JSONObject result = results.getJSONObject(0);
                        JSONArray values = (JSONArray) result.get("values");
                        if (values == null || values.isEmpty() || values.length() < 1) {
                            metric.setValue("0");
                        } else {
                            JSONArray value = (JSONArray) values.get(0);
                            if (value != null && value.length() > 1) {
                                metric.setValue(removeDecimalPart(value.get(1).toString()));
                            } else {
                                metric.setValue("0");
                            }
                        }
                    }
                    metric.setFlag(getState(metric));
                    logger.info("metric:" + metric.toString());
                }catch (Exception e){
                    logger.error("deal metric exception");
                    e.printStackTrace();
                    logger.error(e.getMessage());
                }
            }
        }
    }

    public static String getToken(){
        String token = null;
        try {
            HttpClient httpClient = builder.build();
            HttpPost post = new HttpPost(Constants.OS_IDENTITY_END_POINT);
            post.setHeader("Content-Type","application/json");
            post.setEntity(new StringEntity("{\"auth\": { \"passwordCredentials\": {\"username\": \"" + Constants.AUTH_USER_NAME + "\", \"password\": \""+ Constants.AUTH_PASSWD +"\"}}}", "UTF-8"));
            HttpResponse httpresponse = httpClient.execute(post);
            String rs = EntityUtils.toString(httpresponse.getEntity(), StandardCharsets.UTF_8);
            JSONObject jsonObject = new JSONObject(rs);
            JSONObject access = (JSONObject) jsonObject.get("access");
            JSONObject tokens = (JSONObject) access.get("token");
            token = tokens.get("id").toString();
        } catch (Exception e) {
            logger.error("get token exception");
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        logger.info("token:" + token);
        return token;
    }

    public String getSherlockData(SherlockMetric metric,String token) {
        try {
            HttpClient httpClient = builder.build();
            String query = URLEncoder.encode(metric.getQuery(),"UTF-8");
            String uri=Constants.SHERLOCK_METRIC_END_POINT+"/api/v1/query_range?query="+query+"&start="+this.startTime+"&end="+this.endTime+"&step="+this.step;
            logger.info("uri:" + uri);
            HttpGet get = new HttpGet(uri);
            get.setHeader("Authorization","Bearer " + token);
            get.setHeader("Content-Type","application/json");
            get.setHeader("Accept","application/json");
            HttpResponse httpresponse = httpClient.execute(get);
            String res = EntityUtils.toString(httpresponse.getEntity(), StandardCharsets.UTF_8);
            logger.info("res:"+res);
            return res;
        } catch (Exception e) {
            logger.error("getSherlockData exception");
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return null;
    }

    public static String getState(SherlockMetric metric) {
        String flag="-1";
        try {
            if(StringUtils.isBlank(metric.getValue())){
                metric.setValue("0");
            }
            long threshold = metric.getThreshold();
            long value = Long.valueOf(metric.getValue());
            double thresholdFactor = metric.getThresholdFactor();
            if (threshold > 0 && metric.getAlert().equalsIgnoreCase("true")) {
                flag = NumUtil.getStateWhenAlertIsTrue(threshold, value, thresholdFactor);
            } else if (threshold > 0 && metric.getAlert().equalsIgnoreCase("false")) {
                flag = NumUtil.getStateWhenAlertIsFalse(threshold, value, thresholdFactor);
            } else if (threshold == 0 && value > 0 && metric.getAlert().equalsIgnoreCase("false")) {
                flag = "1";
            } else {
                flag = "0";
            }
        }catch (Exception e){
            logger.error("sherlock getState error");
            logger.error(e.getMessage());
        }
        return flag;
    }

    public static String removeDecimalPart(String num){
        if(StringUtils.isBlank(num)) {
            return "";
        }
        int dx = num.indexOf(".");
        if (dx > 0) {
            return num.substring(0, dx);
        }else {
            return num;
        }
    }
}
