package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.OracleAndCouchbaseCountInfo;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OracleAndCouchbaseCountUtil {

  private static final Logger logger = LoggerFactory.getLogger(OracleAndCouchbaseCountUtil.class);

  public static List<OracleAndCouchbaseCountInfo> getOracleAndCouchbaseCountInfos() {
    ArrayList<OracleAndCouchbaseCountInfo> list = new ArrayList<>();

    List<CSVRecord> csvRecordList = CSVUtil.readCSV("/datashare/mkttracking/tools/AlertingAggrate-tool/temp/oracle/epn_campaign_publisher_mapping.csv", '\011');
    for (CSVRecord csvRecord : csvRecordList){
      OracleAndCouchbaseCountInfo oracleAndCouchbaseCountInfo = new OracleAndCouchbaseCountInfo();
      oracleAndCouchbaseCountInfo.setTableName(csvRecord.get(0));
      oracleAndCouchbaseCountInfo.setOnedayCountInOracle(csvRecord.get(1));
      oracleAndCouchbaseCountInfo.setAlldayCountInOracle(csvRecord.get(2));
      oracleAndCouchbaseCountInfo.setOnedayCountInCouchbase(csvRecord.get(3));
      oracleAndCouchbaseCountInfo.setAlldayCountInCouchbase(csvRecord.get(4));
      oracleAndCouchbaseCountInfo.setTableType(csvRecord.get(5));

      list.add(oracleAndCouchbaseCountInfo);
    }

    return list;
  }

}
