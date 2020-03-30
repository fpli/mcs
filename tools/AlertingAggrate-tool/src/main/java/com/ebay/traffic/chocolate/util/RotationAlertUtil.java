package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.RotationAlert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

public class RotationAlertUtil {
  private static final Logger logger = LoggerFactory.getLogger(RotationAlertUtil.class);

  public static ArrayList<RotationAlert> getRotationAlertInfos() {
    String path = "/home/_choco_admin/rotation/";

    ArrayList<RotationAlert> list = new ArrayList<>();

    list.add(getRotationAlert(path, "rotations", "apollorno"));
    list.add(getRotationAlert(path, "campaigns", "apollorno"));
    list.add(getRotationAlert(path, "clients", "apollorno"));
    list.add(getRotationAlert(path, "vendors", "apollorno"));

    list.add(getRotationAlert(path, "rotations", "herculeslvs"));
    list.add(getRotationAlert(path, "campaigns", "herculeslvs"));
    list.add(getRotationAlert(path, "clients", "herculeslvs"));
    list.add(getRotationAlert(path, "vendors", "herculeslvs"));

    return list;
  }

  private static RotationAlert getRotationAlert(String path, String tableName, String clusterName) {
    HashMap<String, String> map = getRotationAlertMap(path, tableName, clusterName);
    RotationAlert rotationAlert = new RotationAlert();
    rotationAlert.setClusterName(clusterName);
    rotationAlert.setTableName(tableName);
    rotationAlert.setCount(map.getOrDefault("count", "0"));
    rotationAlert.setDistinctCount(map.getOrDefault("distinctCount", "0"));
    rotationAlert.setDiff(Integer.toString(Integer.parseInt(rotationAlert.getCount()) - Integer.parseInt(rotationAlert.getDistinctCount())));

    return rotationAlert;
  }

  private static HashMap<String, String> getRotationAlertMap(String path, String tableName, String clusterName) {
    String dirName = path + clusterName + "/" + tableName;

    return FileReadUtil.getRotationAlertMap(dirName);
  }

}
