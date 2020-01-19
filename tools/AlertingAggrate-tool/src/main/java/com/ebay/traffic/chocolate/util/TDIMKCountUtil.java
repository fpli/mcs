package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.TDIMKInfo;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TDIMKCountUtil {

  private static final Logger logger = LoggerFactory.getLogger(TDIMKCountUtil.class);

  public static ArrayList<TDIMKInfo> getTDIMKInfos() {
    ArrayList<TDIMKInfo> list = new ArrayList<>();
    HashMap<Integer, Integer> mozartMap = getCountMap(getPath("imk_rvr_trckng_event", "mozart"));
    HashMap<Integer, Integer> hopperMap = getCountMap(getPath("imk_rvr_trckng_event", "hopper"));

    for (ChannelType e : ChannelType.values()) {
      TDIMKInfo tdimkInfo = new TDIMKInfo();
      tdimkInfo.setChannelName(e.getName());
      tdimkInfo.setMozartcount(Integer.toString(mozartMap.getOrDefault(e.getIndex(), 0)));
      tdimkInfo.setHopperCount(Integer.toString(hopperMap.getOrDefault(e.getIndex(), 0)));
      tdimkInfo.setDiff(ToolsUtil.getDiff(tdimkInfo.getMozartcount(), tdimkInfo.getHopperCount()));

      list.add(tdimkInfo);
    }

    list.add(getInfosByName("imk_rvr_trckng_event_dtl", "imk dtl"));
    list.add(getInfosByName("ams_click", "epn click"));
    list.add(getInfosByName("ams_imprsn", "epn impression"));

    return list;
  }

  private static TDIMKInfo getInfosByName(String tableName, String type) {
    TDIMKInfo tdimkInfo = new TDIMKInfo();
    tdimkInfo.setChannelName(type);
    tdimkInfo.setMozartcount(getCount(getPath(tableName, "mozart")));
    tdimkInfo.setHopperCount(getCount(getPath(tableName, "hopper")));
    tdimkInfo.setDiff(ToolsUtil.getDiff(tdimkInfo.getMozartcount(), tdimkInfo.getHopperCount()));

    return tdimkInfo;
  }

  public static HashMap<Integer, Integer> getCountMap(String path) {
    List<CSVRecord> csvRecordList = CSVUtil.readCSV(path, '\011');
    HashMap<Integer, Integer> map = new HashMap<>();

    if (csvRecordList == null || csvRecordList.equals("") || csvRecordList.size() < 1) {
      return map;
    }

    for (CSVRecord csvRecord : csvRecordList) {
      logger.info("getCountMap: " + csvRecord.get(0) + "--------" + csvRecord.get(1));
      map.put(Integer.parseInt(csvRecord.get(0)), Integer.parseInt(csvRecord.get(1)));
    }

    return map;
  }

  public static String getCount(String path) {
    List<CSVRecord> csvRecordList = CSVUtil.readCSV(path, '\011');
    if (csvRecordList == null || csvRecordList.equals("") || csvRecordList.size() < 1) {
      return "0";
    }

    return csvRecordList.get(0).get(0);
  }

  private static String getPath(String tableName, String TDType) {
    logger.info("TDIMKCountUtil getPath: " + Constants.PREFIX_DIR + tableName + "_" + TDType + "_merge");

    return Constants.PREFIX_DIR + tableName + "_" + TDType + "_merge";
  }

}