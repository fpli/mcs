package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.parse.AirflowHTMLParse;
import com.ebay.traffic.chocolate.pojo.AirflowDag;
import com.ebay.traffic.chocolate.report.AirflowReport;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class AirflowUtil {

    public static String getAirflowReportHtml() {
        HashMap<String, ArrayList<AirflowDag>> map = AirflowReport.getAirflowMap();
        return AirflowHTMLParse.parse(map);
    }

    public static void main(String[] args) throws IOException {
        HttpClientBuilder builder = HttpClients.custom();
        try {
            HttpHost proxyHost = HttpHost.create("c2sproxy.vip.ebay.com:8080");
            builder.setProxy(proxyHost);
            HttpClient httpClient = builder.build();
            HttpGet get = new HttpGet("http://airflowprod-web.mrkttech-tracking-ns.svc.27.tess.io:8080/admin/airflow/dag_stats");
            get.setHeader("Proxy-Authorization", "Basic eWxpMTk6MTk5NTAyMjd2dnZ2Y2N0YmRmdnJ2Y2p1YnVuZGxqZmxlcnRybmJjamVlYnRmdmpibHZjaQ==");
            HttpResponse httpresponse = httpClient.execute(get);
            Scanner sc = new Scanner(httpresponse.getEntity().getContent());

            //Printing the status line
            System.out.println(httpresponse.getStatusLine());
            while(sc.hasNext()){
                System.out.println(sc.nextLine());
            }
        } catch (Exception e) {
            throw e;
        }
    }
}
