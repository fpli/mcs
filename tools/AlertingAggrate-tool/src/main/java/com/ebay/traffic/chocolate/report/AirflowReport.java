package com.ebay.traffic.chocolate.report;

import com.ebay.traffic.chocolate.pojo.AirflowDag;
import com.ebay.traffic.chocolate.pojo.AirflowMetric;
import com.ebay.traffic.chocolate.util.Constants;
import com.ebay.traffic.chocolate.xml.XMLUtil;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.filter.LoggingFilter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class AirflowReport {
    private static final Logger logger = LoggerFactory.getLogger(AirflowReport.class);

    private static Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFilter.class));


    public static HashMap<String, ArrayList<AirflowDag>> getAirflowMap() {
        HashMap<String, ArrayList<AirflowDag>> airflowMap = new HashMap<>();
        try {
            HashMap<String, ArrayList<AirflowMetric>> hm = XMLUtil.readAirflowMap(Constants.AIRFLOW_HOURLY_XML);
            JSONObject allDags = getAllDagStatus();
            if (hm.isEmpty() || allDags.isEmpty()) {
                return null;
            }
            Set<String> set = hm.keySet();

            for (String subjectArea : set) {
                ArrayList<AirflowMetric> mertrics = hm.get(subjectArea);
                ArrayList<AirflowDag> dags = new ArrayList<>();
                for (AirflowMetric mertic : mertrics) {
                    JSONArray status;
                    try {
                        status = allDags.getJSONArray(mertic.getDagName());
                        if (status.isEmpty()) {
                            continue;
                        }
                        AirflowDag dag = new AirflowDag();
                        dag.setDagName(mertic.getDagName());
                        dag.setThreshold(mertic.getThreshold());
                        dag.setSuccess(String.valueOf(status.getJSONObject(0).getInt("count")));
                        dag.setRunning(String.valueOf(status.getJSONObject(1).getInt("count")));
                        dag.setFailed(String.valueOf(status.getJSONObject(2).getInt("count")));
                        dags.add(dag);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                if (dags.size() > 0) {
                    airflowMap.put(subjectArea, dags);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return airflowMap;
    }

    public static JSONObject getAllDagStatus() {
        String res = null;
        for (int i = 0; i < 3; i++) {
            try {
                res = get(Constants.AIRFLOW_GET_DAG_RUNS_API_URL);
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (res == null) {
            return null;
        }
        JSONObject obj = new JSONObject(res);
        return obj;
    }

    public static String get(String endPoint) {
        if (endPoint == null || endPoint.length() == 0) {
            return null;
        }
        logger.info("Rest url: " + endPoint);
        WebTarget webTarget = client.target(endPoint);

        MediaType mediaType = new MediaType("application", "x-www-form-urlencoded");
        Invocation.Builder invocationBuilder = webTarget.request();
        Response response = invocationBuilder
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("X-Requested-With", "XMLHttpRequest")
                .get();

        String resp = response.readEntity(String.class);

        logger.info("Rest response : " + resp);
        return resp;
    }
}
