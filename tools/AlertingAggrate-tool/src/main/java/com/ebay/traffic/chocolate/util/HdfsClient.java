package com.ebay.traffic.chocolate.util;

import com.ebay.hadoop.kite2.client.KiteClient;
import com.ebay.hadoop.kite2.client.KiteClientFactory;
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
import java.util.Properties;
import java.util.concurrent.Future;

public class HdfsClient {

  private static final Logger logger = LoggerFactory.getLogger(HdfsClient.class);


  /**
   * The file system
   */
  private static FileSystem fs;

  static {
    try {
      Properties config = new Properties();
      config.setProperty("keystone.api.key", "4d4808ef2ab04958a330a4591776341b");
      config.setProperty("keystone.api.secret", "X2CsOyPaN9F8lE6GC8AzG9yH8TqplYzRyygAmf5WTiH7V5dbDD32Nr2fvzgMMFMg");
      config.setProperty("kite.krb5.conf.location", "/tmp/b_kite4_test_krb5.conf");
      config.setProperty("kite.user.principal", "b_marketing_tracking@PROD.EBAY.COM");
      KiteClient kiteClient = KiteClientFactory.newBuilder()
        .build()
        .createClient(config);
      Future<Void> future = kiteClient.start();


      // To block until credential has been fetched for the first time.
      future.get();

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

  public static ArrayList<String> getFileList(String path, String date, String pattern) {
    int n = 0;
    try {
      System.out.println(path);
      System.out.println(date.replaceAll("-", ""));
      System.out.println(pattern);
      date = date.replaceAll("-", "");
      while (!fs.exists(new Path(path + "/" + date))) {
        n++;
        date = getYesterdayPath(n);
        System.out.println(date);
        logger.info(date);
      }

      logger.info("getFileList start: " + pattern);
      System.out.println("getFileList start: " + pattern);
      FileStatus[] fileStatuses = fs.listStatus(new Path(path + "/" + date));
      ArrayList<String> list = new ArrayList();

      for (FileStatus fileStatus : fileStatuses) {
        System.out.println(fileStatus.getPath().getName());
        String str = fileStatus.getPath().getName();
        if (str.contains(pattern)) {
          list.add(str);
          System.out.println(str);
          logger.info(str);
        }
      }

      logger.info("getFileList end: " + pattern);

      return list;
    } catch (IOException e) {
      logger.error(e.getMessage());
      e.printStackTrace();
    }

    return null;
  }

  private static String getYesterdayPath(int n) {
    return LocalDate.now().minusDays(n).toString().replaceAll("-", "");
  }

}
