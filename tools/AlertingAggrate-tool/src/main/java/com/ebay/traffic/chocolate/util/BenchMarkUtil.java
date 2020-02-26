package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.client.KylinClient;
import com.ebay.traffic.chocolate.pojo.BenchMarkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class BenchMarkUtil {

  private static final Logger logger = LoggerFactory.getLogger(BenchMarkUtil.class);

  public static List<BenchMarkInfo> getBenchMarkInfos() {
    initKylinClient();

    ArrayList<BenchMarkInfo> onedayList = getBenchMarkInfoTimeRange(1);
    ArrayList<BenchMarkInfo> manydayList = getBenchMarkInfoTimeRange(30);

    List<BenchMarkInfo> mergedList = getMergedList(onedayList, manydayList);

    return mergedList;
  }

  private static List<BenchMarkInfo> getMergedList(ArrayList<BenchMarkInfo> onedayList, ArrayList<BenchMarkInfo> manydayList) {
    HashMap<String, String> map = new HashMap<>();
    ArrayList<BenchMarkInfo> mergedList = new ArrayList<>();
    for (BenchMarkInfo benchMarkInfo : manydayList) {
      map.put(benchMarkInfo.getChannelType() + "_" + benchMarkInfo.getActionType(), benchMarkInfo.getAvg());
    }

    for (BenchMarkInfo onedayBenchMarkInfo : onedayList) {
      onedayBenchMarkInfo.setAvg(map.getOrDefault(onedayBenchMarkInfo.getChannelType() + "_" + onedayBenchMarkInfo.getActionType(), "0"));
      manydayList.add(onedayBenchMarkInfo);
    }

    return sort(mergedList);
  }

  private static List<BenchMarkInfo> sort(ArrayList<BenchMarkInfo> mergedList) {
    Comparator<BenchMarkInfo> by_channel_type = Comparator.comparing(BenchMarkInfo::getChannelType);
    Comparator<BenchMarkInfo> by_action_type = Comparator.comparing(BenchMarkInfo::getActionType);
    Comparator<BenchMarkInfo> unionComparator = by_channel_type.thenComparing(by_action_type);

    List<BenchMarkInfo> list = mergedList.stream().sorted(unionComparator).collect(Collectors.toList());

    return list;
  }

  private static ArrayList<BenchMarkInfo> getBenchMarkInfoTimeRange(int daynums) {
    ArrayList<BenchMarkInfo> benchMarkInfoArrayList = new ArrayList();

    setIMK(benchMarkInfoArrayList, daynums);
    setEpnClick(benchMarkInfoArrayList, daynums);
    setEpnImpression(benchMarkInfoArrayList, daynums);

    return benchMarkInfoArrayList;
  }

  private static void setEpnImpression(ArrayList<BenchMarkInfo> benchMarkInfoArrayList, int daynums) {
    String sql = "select count(*) from choco_data.ams_imprsn where imprsn_dt %s";
    String formatSql = String.format(sql, getTimeRange(daynums));

    ResultSet resultSet = KylinClient.getInstance().getResultSet(formatSql);

    try {
      while (resultSet.next()) {
        BenchMarkInfo benchMarkInfo = new BenchMarkInfo();
        benchMarkInfo.setChannelType("1");
        benchMarkInfo.setActionType("7");
        setBenchMarkInfo(benchMarkInfo, resultSet.getString(1), daynums);
        benchMarkInfoArrayList.add(benchMarkInfo);
      }
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
  }

  private static void setEpnClick(ArrayList<BenchMarkInfo> benchMarkInfoArrayList, int daynums) {
    String sql = "select count(*) from choco_data.ams_click where click_dt %s";
    String formatSql = String.format(sql, getTimeRange(daynums));

    ResultSet resultSet = KylinClient.getInstance().getResultSet(formatSql);

    try {
      while (resultSet.next()) {
        BenchMarkInfo benchMarkInfo = new BenchMarkInfo();
        benchMarkInfo.setChannelType("1");
        benchMarkInfo.setActionType("1");
        setBenchMarkInfo(benchMarkInfo, resultSet.getString(1), daynums);
        benchMarkInfoArrayList.add(benchMarkInfo);
      }
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
  }

  private static void setIMK(ArrayList<BenchMarkInfo> benchMarkInfoArrayList, int daynums) {
    String sql = "select rvr_chnl_type_cd, rvr_cmnd_type_cd, count(*) from choco_data.imk_rvr_trckng_event where dt %s group by rvr_chnl_type_cd, rvr_cmnd_type_cd order by rvr_chnl_type_cd, rvr_cmnd_type_cd";
    String formatSql = String.format(sql, getTimeRange(daynums));

    ResultSet resultSet = KylinClient.getInstance().getResultSet(formatSql);

    try {
      while (resultSet.next()) {
        BenchMarkInfo benchMarkInfo = new BenchMarkInfo();
        benchMarkInfo.setChannelType(resultSet.getString(1));
        benchMarkInfo.setActionType(resultSet.getString(2));
        setBenchMarkInfo(benchMarkInfo, resultSet.getString(3), daynums);
        benchMarkInfoArrayList.add(benchMarkInfo);
      }
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
  }

  private static String getTimeRange(int daynums) {
    LocalDate yesterday = LocalDate.now().minus(1, ChronoUnit.DAYS);
    LocalDate startDay = LocalDate.now().minus(daynums, ChronoUnit.DAYS);

    if (daynums == 1) {
      return "= '" + yesterday + "'";
    } else if (daynums > 1) {
      return "between '" + startDay + "' and '" + yesterday + "'";
    } else {
      return "= '1970-01-01'";
    }
  }

  private static void setBenchMarkInfo(BenchMarkInfo benchMarkInfo, String count, int daynums) {
    if (daynums == 1) {
      benchMarkInfo.setOnedayCount(count);
    } else if (daynums > 1) {
      benchMarkInfo.setAvg(String.valueOf(Long.parseLong(count) / daynums));
    }
  }

  private static void initKylinClient() {
    String config_path = "/datashare/mkttracking/tools/AlertingAggrate-tool/conf/kylin-client.properties";
    InputStream inStream = null;

    try {
      inStream = new FileInputStream(new File(config_path));
      Properties prop = new Properties();
      prop.load(inStream);

      String username = prop.getProperty("username");
      String password = prop.getProperty("password");
      String url = prop.getProperty("url");

      KylinClient.getInstance()
        .setUserName(username)
        .setPassWord(password)
        .setUrl(url);

    } catch (Exception e) {
      if (inStream != null) {
        try {
          inStream.close();
        } catch (IOException ioException) {
          logger.info(ioException.getMessage());
        }
      }

      logger.info(e.getMessage());
    }
  }

}
