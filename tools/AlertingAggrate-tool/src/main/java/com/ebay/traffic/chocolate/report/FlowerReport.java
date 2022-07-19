package com.ebay.traffic.chocolate.report;

import com.ebay.traffic.chocolate.pojo.AirflowWorker;
import com.ebay.traffic.chocolate.pojo.FlowerResult;
import com.ebay.traffic.chocolate.util.Constants;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class FlowerReport {
    private static final Logger logger = LoggerFactory.getLogger(FlowerReport.class);

    private static final HttpClientBuilder builder = HttpClients.custom();

    public static LinkedList<AirflowWorker> getAirflowWorkerList(String cluster) {
        String res = getWorkers(cluster);
        FlowerResult flowerResult = new Gson().fromJson(res, FlowerResult.class);
        if (flowerResult == null || flowerResult.getData() == null) {
            return null;
        }
        List<HashMap<String, Object>> workers = flowerResult.getData();
        workers.sort((o1, o2) -> {
            String worker1 = (String) o1.getOrDefault("hostname", "worker");
            String worker2 = (String) o2.getOrDefault("hostname", "worker");
            return StringUtils.compare(worker1, worker2);
        });
        LinkedList<AirflowWorker> ls = new LinkedList<>();
        for (HashMap<String, Object> worker : workers) {
            AirflowWorker aw = new AirflowWorker();
            aw.setWorkerName(worker.get("hostname").toString());
            aw.setActive((Double) worker.getOrDefault("active", (double) 0));
            aw.setSuccess((Double) worker.getOrDefault("task-succeeded", (double) 0));
            aw.setProcessed((Double) worker.getOrDefault("processed", (double) 0));
            aw.setFailed((Double) worker.getOrDefault("task-failed", (double) 0));
            aw.setStatus(worker.get("status").toString());
            ls.add(aw);
        }
        return ls;
    }

    public static String getWorkers(String cluster) {
        int retry = 0;
        while (retry < 3) {
            try {
                logger.info("uri:" + getFlowerDashboardUrl(cluster));
                HttpClient httpClient = builder.build();
                HttpGet get = new HttpGet(getFlowerDashboardUrl(cluster));
                get.setHeader("Accept", "application/json");
                HttpResponse httpresponse = httpClient.execute(get);
                String res = EntityUtils.toString(httpresponse.getEntity(), StandardCharsets.UTF_8);
                logger.info("res:" + res);
                return res;
            } catch (Exception e) {
                logger.error("getWorkers exception");
                e.printStackTrace();
                logger.error(e.getMessage());
                retry++;
            }
        }

        return null;
    }

    public static String getFlowerDashboardUrl(String cluster){
        switch (cluster){
            case "127": return Constants.FLOWER_DASHBOARD_URL_127;
            case "94": return Constants.FLOWER_DASHBOARD_URL_94;
            case "79": return Constants.FLOWER_DASHBOARD_URL_79;
            case "27":;
            default: return Constants.FLOWER_DASHBOARD_URL_27;
        }
    }
}
