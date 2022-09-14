package com.ebay.traffic.chocolate.report;

import com.ebay.traffic.chocolate.pojo.*;
import com.ebay.traffic.chocolate.util.Constants;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FileUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.*;
import org.apache.http.util.EntityUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Uc4Report {
    private static final Logger logger = LoggerFactory.getLogger(Uc4Report.class);

    /**
     * 0101 Get sessionID based on public account_PaaS_Provisioning
     * sessionID is universal and only needs to be obtained once
     *
     * @return sessionID
     */
    public static String getUc4SessionID() {
        String muse_sessionid = "";

        // 01 authenticate
        String muse_auth_sid = "";
        HttpClientBuilder builder = HttpClients.custom();
        HashMap<String, String> map = new HashMap<>();
        try {

            CredentialsProvider provider = new BasicCredentialsProvider();

            HttpClient httpClient = builder.setDefaultCredentialsProvider(provider).build();
            HttpPost post = new HttpPost("https://auth.muse.vip.ebay.com/api/v2/authenticate");
            post.setHeader("content-type", "application/json");
            String requestBody = "{\"username\":" + "\"" + Constants.AUTH_USER_NAME + "\"" +
                    ",\"password\":" + "\"" + Constants.AUTH_PASSWD_BASE64 + "\"" + "}";

            ByteArrayEntity entity = null;
            entity = new ByteArrayEntity(requestBody.getBytes("UTF-8"));
            post.setEntity(entity);

            HttpResponse httpresponse = httpClient.execute(post);
            String res = EntityUtils.toString(httpresponse.getEntity(), StandardCharsets.UTF_8);

            Header[] allHeaders = httpresponse.getAllHeaders();
            for (Header allHeader : allHeaders) {

                if (allHeader.getName().contains("Set-Cookie")) {

                    String[] split = allHeader.getValue().split(";");
                    map.put("MUSE-AUTH-SID", split[0]);
                    muse_auth_sid = map.get("MUSE-AUTH-SID");
                }
            }
            logger.info("01 authenticate MUSE-AUTH-SID is " + muse_auth_sid);

        } catch (Exception e) {
            logger.error("01 authenticate exception");
            e.printStackTrace();
            logger.error(e.getMessage());

        }
        // 02 Login
        String redirectURL = "";
        try {
            String[] cmd = new String[]{"/bin/sh", "-c", "curl 'https://auth.muse.vip.ebay.com/v2/login?referer=https%3A%2F%2Fdw.batchadminconsole.muse.vip.ebay.com%2Fs3%2Fhistory%3Fname%3DUTP_EVENT_HOURLY_DONE'" +
                    "-H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' " +
                    "-H 'cookie: " + muse_auth_sid + "'"};

            Process ps = Runtime.getRuntime().exec(cmd);
            BufferedReader br = new BufferedReader(new InputStreamReader(ps.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            String result = sb.toString();
            logger.info("02 Login response is " + result);

            // Intercept the TITLE and URL in the link in the webpage, and the URL must start with HTTP or HTTPS
            String content = result;
            String mode = "\\s*Redirecting to (?='?http|https)([^>]*)";
            Pattern pattern = Pattern.compile(mode);
            Matcher matcher = pattern.matcher(content);

            while (matcher.find()) {
                logger.info("02 Login find Success...");
                redirectURL = matcher.group(1).trim();
                logger.info("Old-Redirecting-URL:" + redirectURL);
            }

            String mode1 = "\\s*(?='?/__api__/v2)([^>]*)";
            Pattern pattern1 = Pattern.compile(mode1);
            Matcher matcher1 = pattern1.matcher(redirectURL);
            while (matcher1.find()) {
                logger.info("02 Login find Start!!!...");
                String redirectURL1 = matcher1.group(1).trim();
                redirectURL = "https://boc.muse.vip.ebay.com" + redirectURL1;
                logger.info("New-Redirecting-URL:" + redirectURL);
            }
        } catch (IOException e) {
            logger.error("02 Login exception");
            e.printStackTrace();
            logger.error(e.getMessage());

        }

        // 03 authed?code
        HashMap<String, String> map3 = new HashMap<>();
        CloseableHttpClient httpclient3 = HttpClients.createDefault();
        try {
            // Create a local cookie store
            CookieStore cookieStore = new BasicCookieStore();

            // Create local HTTP request context HttpClientContext
            HttpClientContext localContext = HttpClientContext.create();
            // bind cookieStore to localContext
            localContext.setCookieStore(cookieStore);

            logger.info("01.+++++++++++++++++++++");
            HttpGet httpget = new HttpGet(redirectURL);
            httpget.setHeader("authority", "boc.muse.vip.ebay.com");
            httpget.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9");
            httpget.setHeader("Referer", "https://auth.muse.vip.ebay.com/");

            logger.info("02.+++++++++++++++++++++");
            // Get Coolie info
            try {
                // set TimeOut
                RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(1000)
                        .setSocketTimeout(1000).setConnectTimeout(1000).build();
                httpget.setConfig(requestConfig);
                logger.info("url-before:" + redirectURL);

                httpclient3.execute(httpget, localContext).close();
                logger.info("url-after:" + redirectURL);

            } catch (Exception e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }

            String src = Constants.LOG4J_DIR;
            File file = new File(src);
            String str = FileUtils.readFileToString(file, "utf-8");
            Pattern pattern2 = Pattern.compile("(?<=org.apache.http.client.protocol.ResponseProcessCookies - Cookie rejected \\[muse.sessionid=\").*?(?=\")");
            Matcher matcher2 = pattern2.matcher(str);
            logger.info("logger=====:" + str);

            while (matcher2.find()) {
                String sessionid = matcher2.group(0).trim();
                logger.info("03 muse.sessionid find Start!!! : " + sessionid);
                map3.put("muse.sessionid", sessionid);
            }


            muse_sessionid = map3.get("muse.sessionid");
            logger.info("03 authed?code muse.sessionid is  Success..." + "(muse.sessionid:" + muse_sessionid + ")");

        } catch (Exception e) {
            logger.error("03 authed?code exception");
            e.printStackTrace();
            logger.error(e.getMessage());

        }
        return muse_sessionid;
    }

    /**
     * 0201 GetJson
     *
     * @param planName
     * @param uc4SessionID
     * @return
     */
    public static String getJson(String planName, String uc4SessionID) {
        String jsonStr = "";
        try {
            String[] cmd1 = new String[]{"/bin/sh", "-c", "curl 'https://batchadminconsolesvc78.vip.ebay.com/api/dw/batchadm/job-history/"
                    + planName
                    + "?limit=24&from=&to=&loadVariables=false'"
                    + "-H 'Accept: application/json' "
                    + "-H 'sessionid: "
                    + uc4SessionID
                    + "' "
                    + "-H 'user: " + Constants.AUTH_USER_NAME + "'"};

            logger.info("01 get-json:start!!!");
            Process ps1 = Runtime.getRuntime().exec(cmd1);

            BufferedReader br1 = new BufferedReader(new InputStreamReader(ps1.getInputStream()));

            StringBuffer sb1 = new StringBuffer();

            String line;

            while ((line = br1.readLine()) != null) {

                sb1.append(line).append("\n");

            }
            jsonStr = sb1.toString();
            logger.info("02 "
                    + planName +
                    " GetJson is success");

            return jsonStr;

        } catch (IOException e) {
            logger.error("02 "
                    + planName +
                    "GetJson authed?code exception");
            e.printStackTrace();
            logger.error(e.getMessage());

        }
        return jsonStr;
    }

    /**
     * 0301 From GetJson to LinkedList<JobHistory>
     *
     * @param planNames
     * @return
     */
    @Nullable
    static LinkedList<JobHistory> getUc4PlanJobs(String planNames, String Uc4SessionID) {
        String planName = planNames;
        LinkedList<JobHistory> JobHistory = null;
        Gson gson = new Gson();

        try {
            String json = getJson(planName, Uc4SessionID);

            // To parse json as an object, replace the trailing percent sign "%"
            if (json.length() > 0) {
                json = json.replace("}]%", "}]");
            }
            logger.info("json.length:" + json.length());

            JobHistory = gson.fromJson(json, new TypeToken<LinkedList<JobHistory>>() {
            }.getType());

            logger.info("JobHistory.length:" + JobHistory.toArray().length);
            return JobHistory;
        } catch (Exception e) {
            logger.error(planName + " JobHistory-to-Json exception");
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        return JobHistory;
    }

    public static LinkedList<JobPlan> getUc4JobPlanList() {

        String uc4SessionID = "";
        LinkedList<JobPlan> jobPlans = new LinkedList<>();
        LinkedList<JobHistory> jobHistoryList;
        String type;
        String alert;
        String id;


        SAXReader reader = new SAXReader();
        try {
            Document document = reader.read(new File(Constants.Uc4_DIR));
            logger.info("00.Uc4_DIR:========" + Constants.Uc4_DIR);

            Element projects = document.getRootElement();
            Iterator it = projects.elementIterator();
            ArrayList<JobPlan> list = new ArrayList<>();

            uc4SessionID = getUc4SessionID();
            String projectName = null;
            while (it.hasNext()) {
                Element project = (Element) it.next();
                projectName = project.attribute("name").getValue();
                List<Element> ls = project.elements("uc4");
                for (Element elem : ls) {
                    JobPlan jp = new JobPlan();
                    jp.setProjectName(projectName);
                    jp.setId(elem.attribute("id").getValue());
                    jp.setType(elem.attribute("type").getValue());
                    jp.setAlert(elem.attribute("alert").getValue());
                    list.add(jp);
                }
            }
            logger.info("01.projectName:========" + projectName);
            for (JobPlan jobPlan : list) {
                logger.info("02.uc4SessionID:========" + uc4SessionID);
                logger.info("02.planName:========" + jobPlan.getId());

                jobHistoryList = getUc4PlanJobs(jobPlan.getId(), uc4SessionID);

                type = jobPlan.getType();
                alert = jobPlan.getAlert();
                id = jobPlan.getProjectName();


                addJobPlan(jobPlans, jobHistoryList, type, alert, id);
            }
        } catch (DocumentException e) {
            e.printStackTrace();
        }
        return jobPlans;
    }

    /**
     * addJobPlan
     *
     * @param jobPlans
     * @param jobHistoryList
     * @param type
     */
    private static void addJobPlan(LinkedList<JobPlan> jobPlans, LinkedList<JobHistory> jobHistoryList, String type, String alert, String id) {
        JobPlan jobPlan = new JobPlan();

        jobPlan.setProjectName(jobHistoryList.get(0).getName());
        jobPlan.setType(type);
        jobPlan.setAlert(alert);
        jobPlan.setId(id);

        long currentTimeMillis = System.currentTimeMillis();
        logger.info("currentTimeMillis:=====:"+currentTimeMillis);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


        if (type.contains("hourly")) {
            long oneHourBefore = currentTimeMillis - 4200*1000;
            logger.info("oneHourBefore:=====:"+oneHourBefore);


            Stream<JobHistory> successStream = jobHistoryList.stream()
                    .filter(s -> {
                        try {
                            logger.info("jobStartTime:======:"+sdf.parse(s.getStartTime()).getTime());
                            return (sdf.parse(s.getStartTime()).getTime() > oneHourBefore)
                                    && (s.getStatusInfo().contains("ENDED_OK - ended normally"));
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    });

            int successNum = successStream.collect(Collectors.toList()).size();
            jobPlan.setSuccess(successNum);

            Stream<JobHistory> running = jobHistoryList.stream()
                    .filter(s ->
                    {
                        try {
                            logger.info("jobStartTime:======:"+sdf.parse(s.getStartTime()).getTime());
                            return ((sdf.parse(s.getStartTime()).getTime() > oneHourBefore)
                                    && (s.getStatus().contains("Running")));
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    });

            int runningNum = running.collect(Collectors.toList()).size();

            Stream<JobHistory> total = jobHistoryList.stream()
                    .filter(s -> {
                        try {
                            logger.info("jobStartTime:======:"+sdf.parse(s.getStartTime()).getTime());
                            return (sdf.parse(s.getStartTime()).getTime() > oneHourBefore);
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    });

            int totalNum = total.collect(Collectors.toList()).size();

            jobPlan.setRunning(runningNum);

            jobPlan.setFailed(totalNum - successNum - runningNum);

            jobPlan.setTotal(totalNum);


            jobPlan.setLastStartDate(jobHistoryList.get(0).getStartTime());

            //duration: "00:00:40"
            jobPlan.setLastRunningTime(jobHistoryList.get(1).getDuration());

            jobPlans.add(jobPlan);
        } else {
            long dailyBefore = currentTimeMillis - 129600*1000;
            logger.info("dailyBefore:===== "+dailyBefore);

            Stream<JobHistory> successStream = jobHistoryList.stream()
                    .filter(s -> {
                        try {
                            logger.info("jobStartTime:======:"+sdf.parse(s.getStartTime()).getTime());
                            return (sdf.parse(s.getStartTime()).getTime()>dailyBefore)
                                    && (s.getStatusInfo().contains("ENDED_OK - ended normally"));
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    });

            int successNum = successStream.collect(Collectors.toList()).size();
            jobPlan.setSuccess(successNum);

            Stream<JobHistory> running = jobHistoryList.stream()
                    .filter(s -> {
                        try {
                            logger.info("jobStartTime:======:"+sdf.parse(s.getStartTime()).getTime());
                            return (sdf.parse(s.getStartTime()).getTime()>dailyBefore)
                            &&(s.getStatus().contains("Running"));
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    });

            int runningNum = running.collect(Collectors.toList()).size();

            Stream<JobHistory> total = jobHistoryList.stream()
                    .filter(s -> {
                        try {
                            logger.info("jobStartTime:======:"+sdf.parse(s.getStartTime()).getTime());
                            return (sdf.parse(s.getStartTime()).getTime() > dailyBefore);
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                    });

            int totalNum = total.collect(Collectors.toList()).size();

            jobPlan.setRunning(runningNum);

            jobPlan.setFailed(totalNum - successNum - runningNum);

            jobPlan.setLastStartDate(jobHistoryList.get(0).getStartTime());

            jobPlan.setTotal(totalNum);

            //duration: "00:00:40"
            jobPlan.setLastRunningTime(jobHistoryList.get(1).getDuration());

            jobPlans.add(jobPlan);
        }
    }
}

