package com.ebay.traffic.chocolate.report;

import com.ebay.traffic.chocolate.pojo.AirflowDag;
import com.ebay.traffic.chocolate.pojo.DagProject;
import com.ebay.traffic.chocolate.pojo.DagRun;
import com.ebay.traffic.chocolate.pojo.DagRunsResult;
import com.ebay.traffic.chocolate.util.Constants;
import com.ebay.traffic.chocolate.xml.XMLUtil;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class AirflowReport {
    private static final Logger logger = LoggerFactory.getLogger(AirflowReport.class);

    private static final HttpClientBuilder builder = HttpClients.custom();

    public static LinkedList<DagProject> getAirflowList(String cluster) {
        LinkedList<DagProject> projects = XMLUtil.readAirflowMap(Constants.AIRFLOW_HOURLY_XML);
        try {
            for (DagProject pro : projects) {
                ArrayList<AirflowDag> list = pro.getList();
                for (AirflowDag dag : list) {
                    try {
                        logger.info("dag:" + dag.toString());
                        String rs = getDagRuns(dag,cluster);
                        logger.info("Airflow result:" + rs);
                        if (rs == null) {
                            continue;
                        }
                        DagRunsResult drr = new Gson().fromJson(rs, DagRunsResult.class);
                        if (drr == null || drr.getTotal_entries() == null || drr.getDag_runs().size() == 0) {
                            continue;
                        }
                        dag.setTotal(drr.getTotal_entries());
                        for (DagRun dr : drr.getDag_runs()) {
                            switch (dr.getState()) {
                                case "success":
                                    dag.setSuccess(dag.getSuccess() + 1);
                                    break;
                                case "running":
                                    dag.setRunning(dag.getRunning() + 1);
                                    break;
                                case "failed":
                                    dag.setFailed(dag.getFailed() + 1);
                                    break;
                                default:
                                    break;
                            }
                        }
                        DagRun last = drr.getDag_runs().get(drr.getDag_runs().size() - 1);
                        dag.setLastStartDate(getLastStartDate(last));
                        dag.setLastRunningTime(getLastRuningTime(last));
                    } catch (Exception e) {
                        logger.error("deal dag exception");
                        e.printStackTrace();
                        logger.error(e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            logger.error(Constants.AIRFLOW_HOURLY_XML + "not found");
            e.printStackTrace();
        }
        return projects;
    }

    public static String getAirflowGetDagApiUrl(String cluster){
        switch (cluster){
            case "127": return Constants.AIRFLOW_GET_DAG_API_URL_127;
            case "94": return Constants.AIRFLOW_GET_DAG_API_URL_94;
            case "79": return Constants.AIRFLOW_GET_DAG_API_URL_79;
            case "27":;
            default: return Constants.AIRFLOW_GET_DAG_API_URL_27;
        }
    }

    public static String getDagRuns(AirflowDag dag,String cluster) {
        if (dag == null || StringUtils.isBlank(dag.getId())) {
            return null;
        }
        int retry = 0;
        while (retry < 3) {
            try {
                String start_date_gte = URLEncoder.encode(getStartDateGte(dag), "UTF-8");
                String uri = getAirflowGetDagApiUrl(cluster) + "/" + dag.getId() + "/dagRuns?limit=100&start_date_gte=" + start_date_gte;
                logger.info("uri:" + uri);
                CredentialsProvider provider = new BasicCredentialsProvider();
                provider.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials("admin", "admin")
                );
                HttpClient httpClient = builder.setDefaultCredentialsProvider(provider).build();
                HttpGet get = new HttpGet(uri);
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
            }
        }

        return null;
    }

    public static String getStartDateGte(AirflowDag dag) {
        String startDateGte;
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR, 7);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:000000+00:00");
        if ("daily".equalsIgnoreCase(dag.getType())) {
            calendar.add(Calendar.DATE, -1);
        } else {
            calendar.add(Calendar.HOUR, -1);
        }
        startDateGte = df.format(calendar.getTime());
        return startDateGte;
    }

    public static String getLastStartDate(DagRun dagRun) {
        if (StringUtils.isBlank(dagRun.getStart_date())) {
            return "";
        }
        String startDates = dagRun.getStart_date().substring(0, 19);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Calendar startDate = Calendar.getInstance();
        try {
            startDate.setTime(format.parse(startDates));
            startDate.add(Calendar.HOUR, -7);
            return format.format(startDate.getTime());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public static long getLastRuningTime(DagRun dagRun) {
        if (StringUtils.isBlank(dagRun.getStart_date())) {
            return -1;
        }
        String endDates = "";
        if (!StringUtils.isBlank(dagRun.getEnd_date())) {
            endDates = dagRun.getEnd_date().substring(0, 19);
        }
        String startDates = dagRun.getStart_date().substring(0, 19);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        Calendar endDate = Calendar.getInstance();
        Calendar startDate = Calendar.getInstance();
        try {
            if ("running".equalsIgnoreCase(dagRun.getState())) {
                endDate.setTime(new Date());
                endDate.add(Calendar.HOUR, 7);
            } else {
                endDate.setTime(format.parse(endDates));
            }
            startDate.setTime(format.parse(startDates));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return TimeUnit.MILLISECONDS.toSeconds(endDate.getTimeInMillis() - startDate.getTimeInMillis());
    }
}
