package com.ebay.traffic.chocolate.report;

import com.ebay.traffic.chocolate.html.FlowerTable;
import com.ebay.traffic.chocolate.html.HourlyEmailHtml;
import junit.framework.TestCase;
import org.apache.http.HttpHost;
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
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class AirflowReportTest extends TestCase {

    @Test
    public void testGetDag() {
        HttpClientBuilder builder = HttpClients.custom();
        try {
            HttpHost proxyHost = HttpHost.create("c2sproxy.vip.ebay.com:8080");
            builder.setProxy(proxyHost);
            CredentialsProvider provider = new BasicCredentialsProvider();
            provider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials("admin", "admin")
            );

            HttpClient httpClient = builder.setDefaultCredentialsProvider(provider).build();
            HttpGet get = new HttpGet("http://airflowprod-web.mrkttech-tracking-ns.svc.27.tess.io:8080/api/v1/dags/dedupe_epn_v2/dagRuns?limit=100&start_date_gte=2021-08-16T22%3A00%3A00");
            get.setHeader("Proxy-Authorization", "Basic eWxpMTk6MTk5NTAyMjd2dnZ2Y2N0YmRmdnJmaW50dmt2cnVrdGpnZXRrZmt1ZWNiaWdnbmdya3Vkdg==");
            get.setHeader("Accept", "application/json");
            HttpResponse httpresponse = httpClient.execute(get);
            System.out.println(EntityUtils.toString(httpresponse.getEntity(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test2() {
        System.out.println(HourlyEmailHtml.getAirflowReportHtml("27"));
    }

    @Test
    public void test3() {
        System.out.println(FlowerTable.parse(FlowerReport.getAirflowWorkerList("27")));
    }
}