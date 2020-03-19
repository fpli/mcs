package com.ebay.traffic.chocolate.report;

import com.ebay.traffic.chocolate.pojo.Azkaban;
import com.ebay.traffic.chocolate.pojo.AzkabanFlow;
import com.ebay.traffic.chocolate.util.rest.AzkabanRestHelper;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class AzkabanReport {

  private static final Logger logger = LoggerFactory.getLogger(AzkabanReport.class);
  private static AzkabanReport azkabanReport = new AzkabanReport();

  /*
   * Singleton
   */
  private AzkabanReport() {
  }

  public void init() {

  }

  public static AzkabanReport getInstance() {
    return azkabanReport;
  }

  public HashMap<String, ArrayList<AzkabanFlow>> getAzkabanFlowMap(HashMap<String, ArrayList<Azkaban>> azkabanmap) {

    HashMap<String, ArrayList<AzkabanFlow>> azkabanFlowMap = new HashMap<>();
    Set<String> set = azkabanmap.keySet();

    for (String subjectArea : set) {
      ArrayList<Azkaban> azkabans = azkabanmap.get(subjectArea);
      azkabanFlowMap.put(subjectArea, getAzkabanFlowList(azkabans));
    }

    return azkabanFlowMap;
  }

  public ArrayList<AzkabanFlow> getAzkabanFlowList(ArrayList<Azkaban> azkabans) {
    ArrayList<AzkabanFlow> azkabanFlowList = new ArrayList<>();

    for (Azkaban azkaban : azkabans) {
      azkabanFlowList.add(getAzkabanFlow(azkaban));
    }

    return azkabanFlowList;
  }

  public AzkabanFlow getAzkabanFlow(Azkaban azkaban) {
    AzkabanFlow azkabanFlow = new AzkabanFlow();
    String sessionId = getSessionId();

    azkabanFlow.setProjectName(azkaban.getProjectName());
    azkabanFlow.setFlowName(azkaban.getFlowName());
    azkabanFlow.setTatal(azkaban.getTotal());
    azkabanFlow.setThreshold(azkaban.getThreshold());

    HashMap<String, String> managerInfoMap = getManagerInfo(azkaban, sessionId);
    azkabanFlow.setSuccess(managerInfoMap.getOrDefault("success", "0"));
    azkabanFlow.setFailed(managerInfoMap.getOrDefault("failed", "0"));

    //curl -k --get --data "session.id=XXXXXXXXXXXXXX&ajax=slaInfo&scheduleId=1" http://localhost:8081/schedule"
//curl -k --get --data "session.id=XXXXXXXXXXXXXX&ajax=fetchSchedule&projectId=1&flowId=test" http://localhost:8081/schedule
    String scheduleId = getScheduleId(sessionId, azkaban, managerInfoMap);
    azkabanFlow.setSla(getSla(scheduleId, sessionId));
    azkabanFlow.setRunningTime(managerInfoMap.getOrDefault("runningTime", "0s"));

//    azkabanFlow.setStatus(getStatus(azkaban, sessionId));

    return azkabanFlow;
  }

  private String getSla(String scheduleId, String sessionId) {
    if (scheduleId.equalsIgnoreCase("")) {
      return "";
    }

    String schedulerUrl = "http://chocolate-azkb-web.stratus.lvs.ebay.com:8081/schedule?";
    logger.info("Azkaban url for getting scheduler: " + schedulerUrl);

    StringBuffer requestBody = new StringBuffer();
    requestBody.append("session.id=")
      .append(sessionId)
      .append("&")
      .append("ajax=slaInfo&")
      .append("scheduleId=")
      .append(scheduleId);

    String res = null;
    try {
      res = AzkabanRestHelper.get(schedulerUrl + requestBody.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }

    if (res == null) {
      return "";
    }
    JSONObject obj = new JSONObject(res);

    if (obj.isNull("settings")) {
      return "";
    }
    JSONArray settings = obj.getJSONArray("settings");

    StringBuffer sla = new StringBuffer();
    if (settings.length() == 0) {
      return "";
    }

    for (int i = 0; i < settings.length(); i++) {
      JSONObject setting = settings.getJSONObject(i);
      if (setting.isNull("duration")) {
        continue;
      }
      String duration = setting.getString("duration");

      sla.append(",").append(duration);
    }

    return sla.toString();
  }

  private String getScheduleId(String sessionId, Azkaban azkaban, HashMap<String, String> managerInfoMap) {
    if (managerInfoMap.getOrDefault("projectId", "").equalsIgnoreCase("") || managerInfoMap.getOrDefault("flowId", "").equalsIgnoreCase("")) {
      return "";
    }

    String schedulerUrl = "http://chocolate-azkb-web.stratus.lvs.ebay.com:8081/schedule?";
    logger.info("Azkaban url for getting scheduler: " + schedulerUrl);

    StringBuffer requestBody = new StringBuffer();
    requestBody.append("session.id=")
      .append(sessionId)
      .append("&")
      .append("ajax=fetchSchedule&")
      .append("projectId=")
      .append(managerInfoMap.get("projectId"))
      .append("&")
      .append("flowId=")
      .append(managerInfoMap.get("flowId"));

    String res = null;
    try {
      res = AzkabanRestHelper.get(schedulerUrl + requestBody.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }

    if (res == null) {
      return "";
    }
    JSONObject obj = new JSONObject(res);

    if (obj.isNull("schedule")) {
      return "";
    }
    JSONObject schedule = obj.getJSONObject("schedule");

    if (schedule.isNull("scheduleId")) {
      return "";
    }

    return Integer.toString(schedule.getInt("scheduleId"));
  }

  public HashMap<String, String> getManagerInfo(Azkaban azkaban, String sessionId) {
    HashMap<String, String> map = new HashMap<>();
    String managerUrl = "http://chocolate-azkb-web.stratus.lvs.ebay.com:8081/manager?";
    logger.info("Azkaban url for getting manager: " + managerUrl);

    StringBuffer requestBody = new StringBuffer();
    requestBody.append("session.id=")
      .append(sessionId)
      .append("&")
      .append("ajax=fetchFlowExecutions&")
      .append("project=")
      .append(azkaban.getProjectName())
      .append("&")
      .append("flow=")
      .append(azkaban.getFlowName())
      .append("&")
      .append("start=0&")
      .append("length=")
      .append(azkaban.getTotal());

    String res = null;
    try {
      res = AzkabanRestHelper.get(managerUrl + requestBody.toString());
    } catch (Exception e) {
      e.printStackTrace();
    }

    setMangerInfo(map, res);

    return map;
  }

  private void setMangerInfo(HashMap<String, String> map, String res) {
    if (res == null) {
      return;
    }
    JSONObject obj = new JSONObject(res);

    if (!obj.isNull("length")) {
      map.put("length", Integer.toString(obj.getInt("length")));
    }

    if (!obj.isNull("projectId")) {
      map.put("projectId", Integer.toString(obj.getInt("projectId")));
    }

    if (obj.isNull("executions")) {
      return;
    }

    JSONArray executions = obj.getJSONArray("executions");

    int success = 0;
    int failed = 0;
    for (int i = 0; i < executions.length(); i++) {
      JSONObject execution = (JSONObject) executions.get(i);
      if (execution.isNull("status")) {
        continue;
      }

      String status = execution.getString("status");
      if (status.equalsIgnoreCase("SUCCEEDED")) {
        success++;
      } else if (status.equalsIgnoreCase("FAILED")) {
        failed++;
      }
    }

    if (executions.length() > 0) {
      JSONObject execution = (JSONObject) executions.get(0);
      long startTime = execution.getLong("startTime");
      long endTime = execution.getLong("endTime");
      if (endTime == -1) {
        endTime = System.currentTimeMillis();
      }
      logger.info(map.get("projectId") + ":startTime" + startTime);
      logger.info(map.get("projectId") + ":endTime" + endTime);

      long runningTime = TimeUnit.MILLISECONDS.toSeconds(endTime - startTime);
      if (runningTime > 60) {
        map.put("runningTime", Long.toString(TimeUnit.SECONDS.toMinutes(runningTime)) + "min");
      } else {
        map.put("runningTime", Long.toString(runningTime) + "s");
      }

      map.put("flowId", execution.getString("flowId"));
    }

    map.put("success", Integer.toString(success));
    map.put("failed", Integer.toString(failed));
  }

  public String getSessionId() {
    String loginUrl = "http://chocolate-azkb-web.stratus.lvs.ebay.com:8081/?action=login";
    String requestBody = "username=azadmin&password=azadmin";

//    String requestBody = "username=azkaban&password=azkaban";
//    String loginUrl = "http://localhost:8081/?action=login";
    logger.info("Azkaban url for getting session: " + loginUrl);
    String res = null;
    try {
      res = AzkabanRestHelper.post(loginUrl, requestBody);
    } catch (Exception e) {
      e.printStackTrace();
    }

    logger.info("get Azkaban SessionId: " + res);
    JSONObject jsonObject = new JSONObject(res);

    return jsonObject.getString("session.id");
  }


}
