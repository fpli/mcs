package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.TDRotationInfo;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class TDRotationCountUtil {

  private static final Logger logger = LoggerFactory.getLogger(TDRotationCountUtil.class);

  public static ArrayList<TDRotationInfo> getTDRotationInfos() {
    ArrayList<TDRotationInfo> list = new ArrayList<>();

    setTDRotationInfo(list, "dw_mpx_rotations");
    setTDRotationInfo(list, "dw_mpx_campaigns");
    setTDRotationInfo(list, "dw_mpx_clients");
    setTDRotationInfo(list, "dw_mpx_vendors");

    return list;
  }

  private static void setTDRotationInfo(ArrayList<TDRotationInfo> list, String tableName) {
    TDRotationInfo tdRotationInfo = new TDRotationInfo();
    tdRotationInfo.setTableName(tableName);
    tdRotationInfo.setMozartcount(getCount(getPath(tableName, "mozart")));

    list.add(tdRotationInfo);
  }



  public static String getCount(String path){
    List<CSVRecord> csvRecordList = CSVUtil.readCSV(path, '\011');
    if(csvRecordList == null || csvRecordList.equals("") || csvRecordList.size() < 1){
      return "0";
    }

    return csvRecordList.get(0).get(0);
  }

  private static String getPath(String tableName, String TDType){
    logger.info("TDRotationCountUtil getPath: " + Constants.TD_DIR + tableName+ "_" + TDType + "_merge");

    return Constants.TD_DIR + tableName+ "_" + TDType + "_merge";
  }

}
