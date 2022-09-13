package com.ebay.traffic.chocolate.report;

import com.ebay.traffic.chocolate.html.HourlyEmailHtml;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import junit.framework.TestCase;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ebay.traffic.chocolate.report.Uc4Report.getUc4SessionID;

public class uc4ReportTest {


    @Test
    public void test1() {
        System.out.println(HourlyEmailHtml.getUc4ReportHtml());
    }

    @Test
    public void test3GetUc4job() {
        HttpClientBuilder builder = HttpClients.custom();
        try {

            CredentialsProvider provider = new BasicCredentialsProvider();
            provider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials("_PaaS_Provisioning", "Q2xvdWQyMDE4I0UyRWViYXlAMTIz")
            );

            HttpClient httpClient = builder.setDefaultCredentialsProvider(provider).build();
            HttpPost post = new HttpPost("https://identity.altus.vip.ebay.com/identityservice/v1/authenticate/DEV/_PaaS_Provisioning");
            post.setHeader("Accept", "application/json");
            post.setHeader("Authorization", "Q2xvdWQyMDE4I0UyRWViYXlAMTIz");
            HttpResponse httpresponse = httpClient.execute(post);
            String res = EntityUtils.toString(httpresponse.getEntity(), StandardCharsets.UTF_8);

//            System.out.println(res);
            JsonParser parser = new JsonParser();
            JsonElement element = parser.parse(res);
            JsonObject root = element.getAsJsonObject();
            System.out.println(root.get("data").getAsJsonObject().get("sessionID"));
            JsonElement sessionID = root.get("data").getAsJsonObject().get("sessionID");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetUc4job() {
        // 01 authenticate
        HttpClientBuilder builder = HttpClients.custom();
        HashMap<String, String> map = new HashMap<>();
        String url2 = null;

        try {

            CredentialsProvider provider = new BasicCredentialsProvider();

            HttpClient httpClient = builder.setDefaultCredentialsProvider(provider).build();
            HttpPost post = new HttpPost("https://auth.muse.vip.ebay.com/api/v2/authenticate");
            post.setHeader("content-type", "application/json");
            String requestBody = "{\"username\":\"_PaaS_Provisioning\",\"password\":\"Q2xvdWQyMDE4I0UyRWViYXlAMTIz\"}";
            ByteArrayEntity entity = null;
            entity = new ByteArrayEntity(requestBody.getBytes("UTF-8"));
            post.setEntity(entity);

            HttpResponse httpresponse = httpClient.execute(post);
            String res = EntityUtils.toString(httpresponse.getEntity(), StandardCharsets.UTF_8);
            System.out.println(res);
            Header[] allHeaders = httpresponse.getAllHeaders();
            for (Header allHeader : allHeaders) {

                if (allHeader.getName().contains("Set-Cookie")) {

                    String[] split = allHeader.getValue().split(";");
                    System.out.println(split[0]);

                    map.put("MUSE-AUTH-SID", split[0]);
                    System.out.println(map.get("MUSE-AUTH-SID"));

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 02 Login
        HttpClientBuilder builder2 = HttpClients.custom();
        try {

            CredentialsProvider provider2 = new BasicCredentialsProvider();

            HttpClient httpClient2 = builder2.setDefaultCredentialsProvider(provider2).build();
            HttpGet get = new HttpGet("https://auth.muse.vip.ebay.com/v2/login?referer=https%3A%2F%2Fdw.batchadminconsole.muse.qa.ebay.com%2Fs3%2Fhistory%3Fname%3DUTP_EVENT_HOURLY_DONE");
            get.setHeader("accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9");
            String url = "s=CgAD4ACBjBzL8Y2VmOGNjZDgxODIwYWQ3MDQyNjA2ZWNjZmZmZmYwYjbD6rpY; ebay=%5Esbf%3D%23000000%5E; nonsession=CgADKACBmyEwJY2VmOGNjZDgxODIwYWQ3MDQyNjA2ZWNjZmZmZmYwYjYAywACYwXsETE4Z2oJBA**; "
                    + map.get("MUSE-AUTH-SID") + "'";
            System.out.println(url);
            get.setHeader("cookie", url);

            System.out.println("1=============");

            HttpClientContext context = HttpClientContext.create();
            CloseableHttpResponse response = (CloseableHttpResponse) httpClient2.execute(get, context);
            System.out.println("2--------------------");
            context.getCookieStore().getCookies().forEach(System.out::println);

            System.out.println("3-----------------------");
            HttpResponse httpresponse2 = httpClient2.execute(get);
            String res1 = EntityUtils.toString(httpresponse2.getEntity(), StandardCharsets.UTF_8);

            Header[] allHeaders = httpresponse2.getAllHeaders();
            System.out.println("4==============");
            for (Header allHeader : allHeaders) {
                System.out.println(allHeader);
            }


        } catch (Exception e) {
            e.printStackTrace();
        }


        try {

            String[] cmd = new String[]{"/bin/sh", "-c", "curl 'https://auth.muse.vip.ebay.com/v2/login?referer=https%3A%2F%2Fdw.batchadminconsole.muse.qa.ebay.com%2Fs3%2Fhistory%3Fname%3DUTP_EVENT_HOURLY_DONE'" +
                    "-H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' " +
                    "-H 'cookie: " + map.get("MUSE-AUTH-SID") + "'"};

            Process ps = Runtime.getRuntime().exec(cmd);

            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));

            StringBuffer sb = new StringBuffer();

            String line;

            while ((line = br.readLine()) != null) {

                sb.append(line).append("\n");

            }

            String result = sb.toString();

            System.out.println("re========" + result);// output return value

            // Intercept the TITLE and URL in the link in the webpage, and the URL must start with HTTP or HTTPS
            String mode = "\\s*Redirecting to (?='?http|https)([^>]*)";
            Pattern p = Pattern.compile(mode);
            Matcher m = p.matcher(result);
            while (m.find()) {
                System.out.println("find...");
                url2 = m.group(1);
                System.out.println("src:" + url2);
            }
        } catch (IOException e) {

            e.printStackTrace();

        }

        // 03 authed?code
        HttpClientBuilder builder3 = HttpClients.custom();

        HashMap<String, String> map3 = new HashMap<>();

        try {

            CredentialsProvider provider3 = new BasicCredentialsProvider();

            HttpClient httpClient3 = builder3.setDefaultCredentialsProvider(provider3).build();
            HttpGet get3 = new HttpGet(url2.trim());
            get3.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9");
            get3.setHeader("Referer", "https://auth.muse.vip.ebay.com/");


            HttpClientContext context = HttpClientContext.create();
            CloseableHttpResponse response = (CloseableHttpResponse) httpClient3.execute(get3, context);
            System.out.println(">>>>>>cookies:");
            List<Cookie> cookies = context.getCookieStore().getCookies();
            for (Cookie cookie : cookies) {
                String name = cookie.getName();
                System.out.println(name);
                if (name.contains("muse.sessionid")) {
                    map3.put("muse.sessionid", cookie.getValue());
                }

            }
            System.out.println("================");
            System.out.println(map3);
            System.out.println(map3.get("muse.sessionid"));
            System.out.println("================");


        } catch (Exception e) {
            e.printStackTrace();
        }

        try {

            String[] cmd1 = new String[]{"/bin/sh", "-c", "curl 'https://batchadminconsolesvc78.vip.ebay.com/api/dw/batchadm/job-history/UTP_EVENT_HOURLY_DONE?limit=120&from=&to=&loadVariables=false'" +
                    "-H 'sessionid: " + map3.get("muse.sessionid") + " " +
                    "-H 'user: " + "_PaaS_Provisioning" + "'"};

            Process ps1 = Runtime.getRuntime().exec(cmd1);

            BufferedReader br1 = new BufferedReader(new InputStreamReader(ps1.getInputStream()));

            StringBuffer sb1 = new StringBuffer();

            String line;

            while ((line = br1.readLine()) != null) {

                sb1.append(line).append("\n");

            }

            String result1 = sb1.toString();

            System.out.println("re========" + result1);


        } catch (IOException e) {

            e.printStackTrace();

        }
    }

    @Test
    public void test6() {
        String uc4SessionID = getUc4SessionID();
        Uc4Report.getUc4PlanJobs(uc4SessionID, "UTP_EVENT_HOURLY_DONE");
    }

    @Test
    public void test7() {
        HourlyEmailHtml.getUc4ReportHtml();
    }

    @Test
    public void test8() throws IOException {
        Properties pps = new Properties();
        InputStream in = new BufferedInputStream(new FileInputStream("src/test/resources/uc4.properties"));
        pps.load(in);
        Enumeration en = pps.propertyNames(); //get the name of the configuration file

        String delete_meta = pps.getProperty("DELETE_META");
        System.out.println("==" + delete_meta);

        while (en.hasMoreElements()) {
            String strKey = (String) en.nextElement();
            String strValue = pps.getProperty(strKey);
            System.out.println(strKey + "=" + strValue);


        }
    }

}