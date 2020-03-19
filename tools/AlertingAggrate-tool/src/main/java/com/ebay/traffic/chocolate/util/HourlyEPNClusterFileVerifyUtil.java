package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.HourlyEPNClusterFileVerifyInfo;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HourlyEPNClusterFileVerifyUtil {
  private static final Logger logger = LoggerFactory.getLogger(HourlyEPNClusterFileVerifyUtil.class);

  private static String PREFIX_PATH = "/datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_monitor_epn/";

  public static ArrayList<HourlyEPNClusterFileVerifyInfo> getHourlyEPNClusterFileVerifyInfos() {
    ArrayList<HourlyEPNClusterFileVerifyInfo> list = new ArrayList<>();
    list.add(getHourlyEPNClusterFileVerifyInfo("apollorno"));
    list.add(getHourlyEPNClusterFileVerifyInfo("herculeslvs"));

    return list;
  }

  private static HourlyEPNClusterFileVerifyInfo getHourlyEPNClusterFileVerifyInfo(String clusterName) {
    HourlyEPNClusterFileVerifyInfo hourlyEPNClusterFileVerifyInfo = new HourlyEPNClusterFileVerifyInfo();

    HashMap<String, String> map = getHourlyDoneFileMap(clusterName);
    String newestDoneFile = map.get("hourlyDoneFile");
    String dateSegement = map.get("dateSegement");
    String hour = Integer.toString(getHour(newestDoneFile));
    logger.info("HourlyEPNClusterFileVerifyUtil getHourlyDoneFile: " + newestDoneFile);
    logger.info("HourlyEPNClusterFileVerifyUtil dateSegement: " + dateSegement);
    logger.info("HourlyEPNClusterFileVerifyUtil hour: " + hour);

    HashMap<String, String> map1 = getIdentifiedFileMap(clusterName, Integer.parseInt(hour), dateSegement);
    int newestIdentifiedFileSequence = Integer.parseInt(map1.get("newestIdentifiedFileSequence"));
    int allIdentifiedFileNum = Integer.parseInt(map1.get("allIdentifiedFileNum"));
    logger.info("HourlyEPNClusterFileVerifyUtil newestIdentifiedFileSequence: " + newestIdentifiedFileSequence);
    logger.info("HourlyEPNClusterFileVerifyUtil allIdentifiedFileNum: " + allIdentifiedFileNum);

    hourlyEPNClusterFileVerifyInfo.setClusterName(clusterName);
    hourlyEPNClusterFileVerifyInfo.setNewestDoneFile(newestDoneFile);
    hourlyEPNClusterFileVerifyInfo.setNewestIdentifiedFileSequence(String.valueOf(newestIdentifiedFileSequence));
    hourlyEPNClusterFileVerifyInfo.setAllIdentifiedFileNum(String.valueOf(allIdentifiedFileNum));
    hourlyEPNClusterFileVerifyInfo.setDiff(String.valueOf(Math.abs(newestIdentifiedFileSequence - allIdentifiedFileNum)));

    return hourlyEPNClusterFileVerifyInfo;
  }

  public static HashMap<String, String> getIdentifiedFileMap(String clusterName, int hour, String dateSegement) {
    HashMap<Integer, String> map = new HashMap<>();
    List<CSVRecord> csvRecordList = CSVUtil.readCSV(getPath(clusterName, dateSegement), ' ');
    for (CSVRecord csvRecord : csvRecordList) {
      logger.info("getIdentifiedFileMap 1 : " + csvRecord.get(0));
      if (csvRecord.size() > 1) {
        if(clusterName.equalsIgnoreCase("apollorno")) {
          logger.info("getIdentifiedFileMap: " + csvRecord.get(0) + csvRecord.get(1));
          map.put(getSeqenceNumFromApollo(csvRecord.get(1)), csvRecord.get(0));
        }else {
          logger.info("getIdentifiedFileMap: " + csvRecord.get(0) + csvRecord.get(1));
          map.put(getSeqenceNumFromHercules(csvRecord.get(1)), csvRecord.get(0));
        }
      }
    }

    Iterator iter = map.entrySet().iterator();
    ArrayList<Integer> list = new ArrayList<>();
    while (iter.hasNext()) {
      Map.Entry entry = (Map.Entry) iter.next();
      Integer fileSeqence = (Integer) entry.getKey();
      String time = (String) entry.getValue();
      if (verifyFileByTime(hour, time)) {
        list.add(fileSeqence);
      }
    }

    HashMap<String, String> map1 = new HashMap<>();
    map1.put("newestIdentifiedFileSequence", String.valueOf(Collections.max(list) + 1));
    map1.put("allIdentifiedFileNum", String.valueOf(list.size()));

    return map1;
  }

  private static boolean verifyFileByTime(int hour, String time) {
    String[] arr = time.split(":");
    if (arr.length < 2) {
      return false;
    }

    if (Integer.parseInt(arr[0]) <= hour) {
      return true;
    }

    return false;
  }

  public static Integer getSeqenceNumFromHercules(String file) {
    String[] arr = file.split("/");
    if (arr.length < 12) {
      return -1;
    }

    String fileName = arr[11];
    String[] arr1 = fileName.split("\\.");
    if (arr1.length < 4) {
      return -1;
    }

    String appfileName = arr1[1];
    String[] arr2 = appfileName.split("_");
    if (arr2.length < 8) {
      return -1;
    }

    return Integer.parseInt(arr2[7]);
  }

  public static Integer getSeqenceNumFromApollo(String file) {
    String[] arr = file.split("/");
    if (arr.length < 10) {
      return -1;
    }

    String fileName = arr[9];
    String[] arr1 = fileName.split("\\.");
    if (arr1.length < 4) {
      return -1;
    }

    String appfileName = arr1[1];
    String[] arr2 = appfileName.split("_");
    if (arr2.length < 8) {
      return -1;
    }

    return Integer.parseInt(arr2[7]);
  }

  private static HashMap<String, String> getHourlyDoneFileMap(String clusterName) {
    HashMap<String, String> map = new HashMap<>();
    List<CSVRecord> todayCsvRecordList = verifyNull(CSVUtil.readCSV(getDonePath(clusterName, "today"), ' '));
    List<CSVRecord> yesterdayCsvRecordList = verifyNull(CSVUtil.readCSV(getDonePath(clusterName, "yesterday"), ' '));

    if (todayCsvRecordList != null && todayCsvRecordList.size() > 0) {
      map.put("hourlyDoneFile", getNewestHourlyDoneFile(todayCsvRecordList));
      map.put("dateSegement", "today");
      return map;
    }

    if (yesterdayCsvRecordList != null && yesterdayCsvRecordList.size() > 0) {
      map.put("hourlyDoneFile", getNewestHourlyDoneFile(yesterdayCsvRecordList));
      map.put("dateSegement", "yesterday");
      return map;
    }

    return map;
  }

  private static List<CSVRecord> verifyNull(List<CSVRecord> readCSVlist) {
    if (readCSVlist.size() == 0) {
      return null;
    }

    ArrayList<String> list = new ArrayList<>();
    for (CSVRecord csvRecord : readCSVlist) {
      if(!csvRecord.get(0).trim().equalsIgnoreCase("")){
        list.add(csvRecord.get(0));
      }
    }

    if(list.size() == 0){
      return null;
    }

    return readCSVlist;
  }

  private static String getNewestHourlyDoneFile(List<CSVRecord> todayCsvRecordList) {
    int maxHour = 0;
    String donfile = "";
    for (CSVRecord csvRecord : todayCsvRecordList) {
      donfile = csvRecord.get(0);
      int hour = getHour(donfile);
      if (maxHour > hour) {
        maxHour = hour;
      }
    }

    return donfile;
  }

  public static int getHour(String donfile) {
    String[] arr = donfile.split("/");
    if (arr.length < 8) {
      return 0;
    }

    String donefileName = arr[7];
    String[] arr1 = donefileName.split("\\.");
    if (arr1.length < 3) {
      return 0;
    }

    String donefileSequence = arr1[2];

    return Integer.parseInt(donefileSequence.substring(8, 10));
  }

  public static String getDonePath(String clusterName, String dateSegement) {
    return PREFIX_PATH + dateSegement + "/donefile-" + clusterName + "-list.csv";
  }

  public static String getPath(String clusterName, String dateSegement) {
    return PREFIX_PATH + dateSegement + "/file-" + clusterName + "-list.csv";
  }

}
