package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.client.ApolloHdfsClient;
import com.ebay.traffic.chocolate.client.HerculesHdfsClient;
import com.ebay.traffic.chocolate.pojo.DoneFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

public class DoneFileUtil {

  private static final Logger logger = LoggerFactory.getLogger(DoneFileUtil.class);

  public static DoneFile getImkEventDoneFileDetail(String clusterName, String path) {
    DoneFile doneFile = getDalayDelayInfo("imk_rvr_trckng_event_hourly", path);
    doneFile.setClusterName(clusterName);
    return doneFile;
  }

  public static DoneFile getAmsClickDoneFileDetail(String clusterName, String path) {
    DoneFile doneFile = getDalayDelayInfo("ams_click_hourly", path);
    doneFile.setClusterName(clusterName);
    return doneFile;
  }

  public static DoneFile getAmsImpressionDoneFileDetail(String clusterName, String path) {
    DoneFile doneFile = getDalayDelayInfo("ams_imprsn_hourly", path);
    doneFile.setClusterName(clusterName);
    return doneFile;
  }

  public static DoneFile getImkEventDoneFileDetailV2(String clusterName, String path) {
    DoneFile doneFile = getDalayDelayInfo("imk_rvr_trckng_event_hourly", path);
    doneFile.setDataSource("imk_rvr_trckng_event_v2_hourly");
    doneFile.setClusterName(clusterName);
    return doneFile;
  }

  public static ArrayList<String> getParams(String pattern, String path) {
    List<String> list = getFileList(pattern, path);
    Collections.sort(list, Collections.reverseOrder());

    ArrayList<String> retList = new ArrayList<>();
    int delay = 0;

    if (list == null || list.size() == 0) {
      int h = LocalDateTime.now().getHour();
      delay = -1;
    }

    String donefile = "";
    String max_time = "";
    if (list.size() >= 1) {
      donefile = list.get(0);
      String[] arr = donefile.split("\\.");
      if (arr.length == 3) {
        donefile = arr[2];
      }
      logger.info("log: donefile ----> " + donefile);
      System.out.println("console: donefile ----> " + donefile);
      if (donefile.length() > 10) {
        max_time = donefile.substring(0, 10);
        delay = getDelay(max_time);
      }
      logger.info("log: max_time ----> " + max_time);
      System.out.println("console: max_time ----> " + max_time);
    }

    retList.add(new Integer(delay).toString());
    retList.add(list.get(0));

    return retList;
  }

  private static List<String> getFileList(String pattern, String path) {
    if (path.contains("apollo")) {
      return DoneFileReadUtil.getDoneFileList(Constants.APOLLO_DONE_FILES, pattern);
    } else if (path.contains("hercules")) {
      return DoneFileReadUtil.getDoneFileList(Constants.HERCULES_DONE_FILES, pattern);
    } else {
      return null;
    }
  }

  public static int getDelay(String max_time) {
    int year = Integer.parseInt(max_time.substring(0, 4));
    int month = Integer.parseInt(max_time.substring(4, 6));
    int day = Integer.parseInt(max_time.substring(6, 8));
    int hour = Integer.parseInt(max_time.substring(8, 10));

    Calendar c = Calendar.getInstance();
    c.set(year, month - 1, day, hour, 0);
    long delay = 0;
    try {
      long t = c.getTimeInMillis();
      long current = System.currentTimeMillis();
      delay = (current - t) / 3600000l;
    } catch (Exception e) {
      e.printStackTrace();
    }

    return (int) delay;
  }

  public static DoneFile getDalayDelayInfo(String pattern, String path) {
    ArrayList<String> list = getParams(pattern, path);

    int delay_hour = Integer.parseInt(list.get(0));
    String donefile = list.get(1);

    DoneFile doneFile = new DoneFile();
    int delay = 0;
    String status = "Ok";

    if (delay_hour >= 3) {
      delay = delay_hour - 2;
    }

    int waring_delay_max_value = 12;
    if (path.contains("hercules")){
      waring_delay_max_value = 5;
    }

    if (delay > 0 && delay <= waring_delay_max_value) {
      status = "Warning";
    } else if (delay > waring_delay_max_value) {
      status = "Critical";
    }

    doneFile.setDataSource(pattern);
    doneFile.setStatus(status);
    doneFile.setDelay(delay);
    doneFile.setCurrentDoneFile(donefile);

    return doneFile;
  }

  public static ArrayList<DoneFile> getDoneFileInfos() {
    ArrayList<DoneFile> list = new ArrayList<>();
    list.add(getImkEventDoneFileDetailV2("apollo-rno", "viewfs://apollo-rno/apps/b_marketing_tracking/watch-imk"));
    list.add(getImkEventDoneFileDetail("apollo-rno", "viewfs://apollo-rno/apps/b_marketing_tracking/watch"));
    list.add(getAmsClickDoneFileDetail("apollo-rno", "viewfs://apollo-rno/apps/b_marketing_tracking/watch"));
    list.add(getAmsImpressionDoneFileDetail("apollo-rno", "viewfs://apollo-rno/apps/b_marketing_tracking/watch"));
    list.add(getImkEventDoneFileDetailV2("hercules-lvs", "hdfs://hercules/apps/b_marketing_tracking/watch-imk"));
    list.add(getImkEventDoneFileDetail("hercules-lvs", "hdfs://hercules/apps/b_marketing_tracking/watch"));
    list.add(getAmsClickDoneFileDetail("hercules-lvs", "hdfs://hercules/apps/b_marketing_tracking/watch"));
    list.add(getAmsImpressionDoneFileDetail("hercules-lvs", "hdfs://hercules/apps/b_marketing_tracking/watch"));

    return list;
  }

}
