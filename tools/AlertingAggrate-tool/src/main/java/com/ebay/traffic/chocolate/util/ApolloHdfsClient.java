package com.ebay.traffic.chocolate.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;

public class ApolloHdfsClient {

  private static final Logger logger = LoggerFactory.getLogger(ApolloHdfsClient.class);

  /**
   * The file system
   */
  private static FileSystem fs;

  static {
    try {
      Configuration hadoopConf = new Configuration();
      hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
      hadoopConf.addResource(new Path("/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/etc/hadoop/core-site.xml"));
      hadoopConf.addResource(new Path("/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/etc/hadoop/hdfs-site.xml"));
      UserGroupInformation.setConfiguration(hadoopConf);

      // Wait for some period for ticket cache fetched
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      fs = FileSystem.get(hadoopConf);
    } catch (Exception e) {
      logger.info(e.getMessage());
      e.printStackTrace();
    }
  }

  public static ArrayList<String> getDoneFileList(String path, String date, String pattern) {
    int n = 0;
    try {
      System.out.println(path);
      System.out.println(date.replaceAll("-", ""));
      System.out.println(pattern);
      date = date.replaceAll("-", "");
      ArrayList<String> list = new ArrayList();
      while (!fs.exists(new Path(path + "/" + date)) || list.size() == 0) {
        logger.info("getFileList start: " + pattern);
        System.out.println("getFileList start: " + pattern);
        if (fs.exists(new Path(path + "/" + date))) {
          FileStatus[] fileStatuses = fs.listStatus(new Path(path + "/" + date));

          for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath().getName());
            String str = fileStatus.getPath().getName();
            if (str.contains(pattern)) {
              list.add(str);
              System.out.println(str);
              logger.info(str);
            }
          }
        }

        n++;
        date = getYesterdayPath(n);
        System.out.println(date);
        logger.info(date);
      }

      logger.info("getFileList end: " + pattern);

      return list;
    } catch (IOException e) {
      logger.error(e.getMessage());
      e.printStackTrace();
    }

    return null;
  }

  public static ArrayList<String> getFileList(String path) {
    try {
      FileStatus[] fileStatuses = fs.listStatus(new Path(path));
      ArrayList<String> list = new ArrayList();

      for (FileStatus fileStatus : fileStatuses) {
        System.out.println(fileStatus.getPath().getName());
        String str = fileStatus.getPath().getName();
        list.add(str);
      }

      return list;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  private static String getYesterdayPath(int n) {
    return LocalDate.now().minusDays(n).toString().replaceAll("-", "");
  }

}
