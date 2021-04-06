package com.ebay.app.raptor.chocolate.adservice.component;

import com.ebay.traffic.monitoring.ESMetrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

@Component
@DependsOn("AdserviceService")
public class EsrXidClient {
    Logger logger = LoggerFactory.getLogger(EsrXidClient.class);

    private CloseableHttpClient closeableHttpClient;

    @Value(value = "${esrxid.adservice.endpointUrl}")
    private String esrXidEndpointUrl;

    @Autowired
    private void init() {
        PoolingHttpClientConnectionManager pool = new PoolingHttpClientConnectionManager();
        pool.setDefaultMaxPerRoute(32);
        pool.setMaxTotal(200);
        closeableHttpClient = HttpClients.custom().setConnectionManager(pool).disableRedirectHandling().build();
    }

    public String getUserIdByGuid(String guid) {
        String userId = "0";
        if (StringUtils.isBlank(guid) || guid.length() < 32) {
            return userId;
        }

        ESMetrics.getInstance().meter("totalRequestEsrxidCount");

        //pguid equals guid here
        URI uri = null;
        try {
            uri = new URI(esrXidEndpointUrl + "pguid/" + guid);
        } catch (URISyntaxException e) {
            logger.error("call esrXid error, " + e);
            ESMetrics.getInstance().meter("FailedGetUidFromEsrxid");
        }
        if (uri == null) {
            return userId;
        }
        HttpGet httpGet = new HttpGet(uri);
        try (CloseableHttpResponse response = closeableHttpClient.execute(httpGet)) {
            String jsonString = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            JSONObject jsonObject = new JSONObject(jsonString);
            JSONArray idMap = jsonObject.getJSONArray("idMap");
            if (idMap != null) {
                JSONObject idMapObject = idMap.getJSONObject(0);
                if (!idMapObject.has("accounts")) {
                    return userId;
                }
                JSONArray accounts = idMapObject.getJSONArray("accounts");
                if (accounts != null) {
                    JSONObject accountObj = accounts.getJSONObject(0);
                    if (accountObj != null) {
                        userId = accountObj.getString("id");
                        ESMetrics.getInstance().meter("succeedGetUidFromEsrxid");
                    }
                }
            }
        } catch (IOException e) {
            logger.error("call esrXid error, " + e);
            ESMetrics.getInstance().meter("FailedGetUidFromEsrxid");
        }
        return userId;
    }
}
