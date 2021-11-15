package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.traffic.chocolate.flink.nrt.provider.monitor.DimensionEntity;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * load metrics target from state backend and offer the ability to judge whether a target exists
 *
 * @author yuhxiao
 * @since 2021/11/10
 */

public class MetricsUtil {
    public static HashMap<String, ArrayList<String>> targetMap = new HashMap<>();

    public static void add(DimensionEntity entity) {
        if (!targetMap.containsKey(entity.getDimensionName())) {
            targetMap.put(entity.getDimensionName(), new ArrayList<>());
        }
        targetMap.get(entity.getDimensionName()).add(entity.getDimensionVal());
    }

    public static String checkExist(String dimensionName, String dimensionVal) {
        if (targetMap.get(dimensionName) == null) {
            return "null";
        } else {
            if (targetMap.get(dimensionName).contains(dimensionVal)) {
                return dimensionVal;
            }
        }
        return "null";
    }

    public static void updateCache(String fideliusUrl) {
        CloseableHttpClient closeableHttpClient = HttpClients.custom().disableRedirectHandling().build();
        HttpGet httpGet = new HttpGet(fideliusUrl);
        try (CloseableHttpResponse response = closeableHttpClient.execute(httpGet)) {
            String jsonString = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            List<DimensionEntity> dimensions = mapper.readValue(jsonString, new TypeReference<List<DimensionEntity>>() {
            });
            targetMap.clear();
            for (DimensionEntity dimensionEntity : dimensions) {
                System.out.println(dimensionEntity.getDimensionName());
                add(dimensionEntity);
            }
        } catch (IOException e) {
            SherlockioMetrics.getInstance().meterByGauge("GetDimensionFromFideliusError", 1);
        }
    }

}

