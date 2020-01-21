package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.map.ChocomapApplication;
import com.ebay.traffic.chocolate.map.entity.DataCountInfo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class FileUtils {

  private static Logger logger = LoggerFactory.getLogger(ChocomapApplication.class);

  /**
   * write csv
   *
   * @param dataCountInfoArrayList
   * @param path
   */
  public static void writeCSV(ArrayList<DataCountInfo> dataCountInfoArrayList, String path) {
    String[] fileHeader = {"tableName", "countOnedayInOracle", "countAlldaysInOracle", "countOnedayInCouchbase", "countAlldaysInCouchbase", "tableType"};

    try {
      BufferedWriter writer = Files.newBufferedWriter(Paths.get(path));
      CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(fileHeader).withFirstRecordAsHeader());
      for (DataCountInfo dataCountInfo : dataCountInfoArrayList) {
        csvPrinter.printRecord(
          dataCountInfo.getTableName() + Character.toString('\011')
            + dataCountInfo.getOnedayCountInOracle() + Character.toString('\011')
            + dataCountInfo.getAlldayCountInOracle() + Character.toString('\011')
            + dataCountInfo.getOnedayCountInCouchbase() + Character.toString('\011')
            + dataCountInfo.getAlldayCountInCouchbase() + Character.toString('\011')
            + dataCountInfo.getTableType()
        );
      }

      csvPrinter.flush();
      logger.info("==== write CSV File successfully, and the file path ：" + path + " ====");
    } catch (Exception e) {
      logger.info(e.getMessage());
    }

  }

}
