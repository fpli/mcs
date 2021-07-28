package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.client.ApolloHdfsClient;
import com.ebay.traffic.chocolate.client.ChocolateHdfsClient;
import com.ebay.traffic.chocolate.client.HerculesHdfsClient;
import com.ebay.traffic.chocolate.pojo.HdfsFileNumberCompare;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.ArrayList;

public class HdfsCompareUtil {

  private static final Logger logger = LoggerFactory.getLogger(HdfsCompareUtil.class);

  public static HdfsFileNumberCompare getHdfsFiles(String tableName, Boolean hasChoco) {

    int chocolate = 0;
    if(hasChoco) {
      chocolate = FileReadUtil.getFileList(Constants.PREFIX_TMP_DIR + "chocolate_files/" + tableName + ".txt").size();
    }
    int apollo = FileReadUtil.getFileList(Constants.PREFIX_TMP_DIR + "apollo_files/" + tableName + ".txt").size();
    int hercules = FileReadUtil.getFileList(Constants.PREFIX_TMP_DIR + "hercules_files/" + tableName + ".txt").size();

    String status_chocolate_apollo = "-";
    if(hasChoco) {
      status_chocolate_apollo = getStatus(chocolate, apollo);
    }
    String status_apollo_hercules = getStatus(apollo, hercules);

    HdfsFileNumberCompare hdfsFileNumberCompare = new HdfsFileNumberCompare();
    hdfsFileNumberCompare.setData(tableName);
    hdfsFileNumberCompare.setChocolate_cluster(chocolate);
    hdfsFileNumberCompare.setApollo_rno_cluster(apollo);
    hdfsFileNumberCompare.setHercules_lvs_cluster(hercules);
    hdfsFileNumberCompare.setChocolate_minus_apollo_rno(status_chocolate_apollo);
    hdfsFileNumberCompare.setApollo_rno_cluster_minus_hercules_lvs(status_apollo_hercules);

    return hdfsFileNumberCompare;
  }

  private static String getStatus(int chocolate, int apollo) {
    if (chocolate - apollo == 0) {
      return "Ok";
    } else {
      return "Critical";
    }
  }

  public static ArrayList<HdfsFileNumberCompare> getHdfsFileNumberCompares() {
    ArrayList<HdfsFileNumberCompare> list = new ArrayList<>();
    list.add(getHdfsFiles("imk_rvr_trckng_event_v2",false));
    list.add(getHdfsFiles("ams_click",false));
    list.add(getHdfsFiles("ams_impression",false));
    return list;
  }

}
