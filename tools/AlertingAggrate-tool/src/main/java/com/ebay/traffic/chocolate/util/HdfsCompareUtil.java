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

  public static HdfsFileNumberCompare getImkEvent() {
    String date = LocalDate.now().minusDays(1).toString();

    int apollo = ApolloHdfsClient.getFileList("viewfs://apollo-rno/apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event/dt=" + date).size();
    int hercules = HerculesHdfsClient.getFileList("/datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/imk_rvr_trckng_event.txt").size();

    String status_apollo_hercules = getStatus(apollo, hercules);

    HdfsFileNumberCompare hdfsFileNumberCompare = new HdfsFileNumberCompare();
    hdfsFileNumberCompare.setData("imk_rvr_trckng_event");
    hdfsFileNumberCompare.setChocolate_cluster(0);
    hdfsFileNumberCompare.setApollo_rno_cluster(apollo);
    hdfsFileNumberCompare.setHercules_lvs_cluster(hercules);
    hdfsFileNumberCompare.setChocolate_minus_apollo_rno("-");
    hdfsFileNumberCompare.setApollo_rno_cluster_minus_hercules_lvs(status_apollo_hercules);

    return hdfsFileNumberCompare;
  }

  public static HdfsFileNumberCompare getImkEventDtl() {
    String date = LocalDate.now().minusDays(1).toString();

    int apollo = ApolloHdfsClient.getFileList("viewfs://apollo-rno/apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl/dt=" + date).size();
    int hercules = HerculesHdfsClient.getFileList("/datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/imk_rvr_trckng_event_dtl.txt").size();

    String status_apollo_hercules = getStatus(apollo, hercules);

    HdfsFileNumberCompare hdfsFileNumberCompare = new HdfsFileNumberCompare();
    hdfsFileNumberCompare.setData("imk_rvr_trckng_event_dtl");
    hdfsFileNumberCompare.setChocolate_cluster(0);
    hdfsFileNumberCompare.setApollo_rno_cluster(apollo);
    hdfsFileNumberCompare.setHercules_lvs_cluster(hercules);
    hdfsFileNumberCompare.setChocolate_minus_apollo_rno("-");
    hdfsFileNumberCompare.setApollo_rno_cluster_minus_hercules_lvs(status_apollo_hercules);

    return hdfsFileNumberCompare;
  }

  public static HdfsFileNumberCompare getAmsClick() {
    String date = LocalDate.now().minusDays(1).toString();

    int chocolate = ChocolateHdfsClient.getFileList("/datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/choco_ams_click.txt").size();
    int apollo = ApolloHdfsClient.getFileList("viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt/click/click_dt=" + date).size();
    int hercules = HerculesHdfsClient.getFileList("/datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/ams_click.txt").size();

    String status_chocolate_apollo = getStatus(chocolate, apollo);
    String status_apollo_hercules = getStatus(apollo, hercules);

    HdfsFileNumberCompare hdfsFileNumberCompare = new HdfsFileNumberCompare();
    hdfsFileNumberCompare.setData("ams_click");
    hdfsFileNumberCompare.setChocolate_cluster(chocolate);
    hdfsFileNumberCompare.setApollo_rno_cluster(apollo);
    hdfsFileNumberCompare.setHercules_lvs_cluster(hercules);
    hdfsFileNumberCompare.setChocolate_minus_apollo_rno(status_chocolate_apollo);
    hdfsFileNumberCompare.setApollo_rno_cluster_minus_hercules_lvs(status_apollo_hercules);

    return hdfsFileNumberCompare;
  }

  public static HdfsFileNumberCompare getAmsImpression() {
    String date = LocalDate.now().minusDays(1).toString();

    int chocolate = ChocolateHdfsClient.getFileList("/datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/choco_ams_impression.txt").size();
    int apollo = ApolloHdfsClient.getFileList("viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt/imp/imprsn_dt=" + date).size();
    int hercules = HerculesHdfsClient.getFileList("/datashare/mkttracking/tools/AlertingAggrate-tool/temp/hercules_files/ams_impression.txt").size();

    String status_chocolate_apollo = getStatus(chocolate, apollo);
    String status_apollo_hercules = getStatus(apollo, hercules);

    HdfsFileNumberCompare hdfsFileNumberCompare = new HdfsFileNumberCompare();
    hdfsFileNumberCompare.setData("ams_imprsn");
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
    list.add(getImkEvent());
    list.add(getImkEventDtl());
    list.add(getAmsClick());
    list.add(getAmsImpression());

    return list;
  }

}
