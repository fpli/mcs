package com.ebay.traffic.chocolate;


import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * Created by huiclu on 5/17/19.
 */
public class EPNScp {

  private static Properties properties;

  private static FileSystem fs;

  private static Metrics metrics;

  private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();

  public static void main(String[] args) throws Exception {
    assert args != null;
    if(args.length != 1) {
      System.out.println("UAGES: java -classpath cho-epn-scp-*.jar com.ebay.traffic.chocolate.EPNScp [env]");
      throw new Exception("init error");
    }
    String env = "prod".equalsIgnoreCase(args[0]) ? "prod" : "qa";
    try {
      init(env);
      run();
    } catch (Exception e) {
      e.printStackTrace();
      throw new Exception(e);
    } finally {
      shutdown();
    }
  }

  private static void run() throws Exception{
    System.out.println("start SCP EPN click data");
    RemoteIterator<LocatedFileStatus> ite = fs.listFiles(new Path(properties.getProperty("epn.hdfs.click.metaDir")), true);
    while (ite.hasNext()) {
      LocatedFileStatus status = ite.next();
      if (status.isFile() && status.getPath().getName().endsWith("meta.epnnrt")) {
        HashMap<String, List<String>> maps = readMetaFile(status.getPath().toString());
        maps.forEach((k, v) -> {
          getFileFromHdfs(k, v, "click");
          scpToETL(k, v, "click");
          //scpToApollo(k, v, "click");
          deleteLocalFile(k, v);
          touchProcessedFile(k, "click");
        });
        fs.delete(new Path(status.getPath().toString()), true);
      }
    }

    System.out.println("start SCP EPN impression data");
    RemoteIterator<LocatedFileStatus> itei = fs.listFiles(new Path(properties.getProperty("epn.hdfs.imp.metaDir")), true);
    while (itei.hasNext()) {
      LocatedFileStatus status = itei.next();
      if (status.isFile() && status.getPath().getName().endsWith("meta.epnnrt")) {
        HashMap<String, List<String>> maps = readMetaFile(status.getPath().toString());
        maps.forEach((k, v) -> {
          getFileFromHdfs(k, v, "imp");
          scpToETL(k, v, "imp");
          //scpToApollo(k, v, "imp");
          deleteLocalFile(k, v);
          touchProcessedFile(k, "imp");
        });
        fs.delete(new Path(status.getPath().toString()), true);
      }
    }

  }

  private static void touchProcessedFile(String date, String eventType) {
    String file = properties.getProperty("epn.local.processDir") + date +"." + eventType + ".processed";
    try {
      if (!new File(file).exists())
        Files.touch(new File(file));
    } catch (Exception e) {
      System.out.println("failed to touch processed file of " + date);
      e.printStackTrace();
    }
  }

  /**
   * for unit test
   * @param props properties
   */
  public static void injectProperties(Properties props) {
    properties = props;
  }


  /**
   * shutdown job
   */
  private static void shutdown() throws IOException {
    if (metrics != null) {
      metrics.flush();
      metrics.close();
    }
    fs.close();
    System.out.println("shutdown success");
  }

  private static void init(String env) throws IOException{
    // load priperties
    if(properties == null) {
      properties = new Properties();
      System.out.println("start read props");
      InputStream in = Object.class.getResourceAsStream("/" + env + "/epn.properties");
      properties.load(in);
      System.out.println("finish read props");
    }
    // init hdfs filesystem
    Configuration conf = new Configuration();
    conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
    if ("prod".equals(env)) {
      conf.addResource(Object.class.getResourceAsStream("/prod/core-site.xml"));
      conf.addResource(Object.class.getResourceAsStream("/prod/hdfs-site.xml"));
    }
    System.setProperty("HADOOP_USER_NAME", properties.getProperty("epn.hdfs.username"));
    fs = FileSystem.get(URI.create(properties.getProperty("epn.hdfs.uri")), conf);

    ESMetrics.init(properties.getProperty("epn.es.prefix"), properties.getProperty("epn.es.url"));
    metrics = ESMetrics.getInstance();
    System.out.println("init success");
  }


  /**
   * get one data file from chocolate hdfs to local and touch one done file to local
   * @throws Exception fast throw any exception
   */
  private static void getFileFromHdfs(String date, List<String> files, String eventType) {
    files.forEach(k -> {
      String fileName = k.substring(k.lastIndexOf("/") + 1);
      try {
        String removeDataCmd = "rm -f " + properties.getProperty("epn.local.workDir") + "/" + k;
        Process removeDataProcess = Runtime.getRuntime().exec(removeDataCmd);
        if (removeDataProcess.waitFor() != 0 ) {
          throw new Exception("rmCmdException");
        }
        fs.copyToLocalFile(new Path(k), new Path(properties.getProperty("epn.local.workDir") + "/" + fileName));
        metrics.meter("EPNGetFromHDFS", 1, Field.of("channelAction", eventType));
      } catch (Exception e) {
        metrics.meter("EPNGetFromHDFSError", 1, Field.of("channelAction", eventType));
        System.out.println("failed get file from hdfs: " + fileName);
        e.printStackTrace();
      }
    });
  }


  /**
   * read meta file in chocolate hdfs
   * @param path full path of meta file
   * @return list of data files
   * @throws IOException IOException
   */
  private static HashMap<String, List<String>> readMetaFile(String path) throws IOException {
    HashMap<String, List<String>> maps = new HashMap<>();
    FSDataInputStream inputStream = fs.open(new Path(path));
    String out= IOUtils.toString(inputStream, "UTF-8");
    inputStream.close();
    MetaFiles metaFiles = gson.fromJson(out, MetaFiles.class);
    for (DateFiles dateFiles : metaFiles.getMetaFiles()) {
      String date = dateFiles.getDate();
      if (maps.containsKey(date)) {
        maps.get(date).addAll(Arrays.asList(dateFiles.getFiles()));
      } else {
        maps.put(date, Arrays.asList(dateFiles.getFiles()));
      }
    }
    return maps;
  }

  /**
   * scp one local data file to etl, scp one done file to etl
   * @param files the file list
   * @throws Exception fast throw any exception
   */
  private static void scpToETL(String date, List<String> files, String eventType) {
    files.forEach(k -> {
      String fileName = k.substring(k.lastIndexOf("/") + 1);
      String localDataFilePath = properties.getProperty("epn.local.workDir") + "/" + fileName;
      try {
        String scpDataCmd = String.format(properties.getProperty("epn.etl.command"), localDataFilePath, fileName);
        Process scpDataProcess = Runtime.getRuntime().exec(scpDataCmd);
        if (scpDataProcess.waitFor() != 0) {
          throw new Exception("scpCmdException");
        }
        metrics.meter("EPNToETL", 1, Field.of("channelAction", eventType));
      } catch (Exception e) {
        metrics.meter("EPNToETLError", 1, Field.of("channelAction", eventType));
        System.out.println("failed scp " + eventType + " file to etl: " + fileName);
        e.printStackTrace();
       // throw new Exception(e);
      }
    });
  }

  private static void scpToApollo(String date, List<String> files, String eventType) {
    try {
      String initCmd = properties.getProperty("epn.reno.init.command");
      Process initProcess = Runtime.getRuntime().exec(initCmd);
      if (initProcess.waitFor() != 0)
        throw new Exception("keytabInitCmdException");
    } catch (Exception e) {
      System.out.println("failed to init reno keytab");
      e.printStackTrace();
    }

    files.forEach(k -> {
      String fileName = k.substring(k.lastIndexOf("/") + 1);
      String localDataFilePath = properties.getProperty("epn.local.workDir") + "/" + fileName;
      String apolloPath = properties.getProperty("epn.apollo.path") + eventType + "/" + date + "/" + fileName;
      try {
        if (!fs.exists(new Path(properties.getProperty("epn.apollo.path") + eventType + "/" + date))) {
          fs.mkdirs(new Path(properties.getProperty("epn.apollo.path") + eventType + "/" + date));
        }
        fs.copyFromLocalFile(new Path(localDataFilePath), new Path(apolloPath));
        metrics.meter("EPNToReno", 1, Field.of("channelAction", eventType));
      } catch (Exception e) {
        metrics.meter("EPNToRenoError", 1, Field.of("channelAction", eventType));
        System.out.println("failed scp " + eventType +" file to Apollo reno: " + fileName);
        e.printStackTrace();
      }
    });
  }

  /**
   * delete local data and done file if scp success
   * @param files the file name
   */
  private static void deleteLocalFile(String date, List<String> files){
    files.forEach(k -> {
      String fileName = k.substring(k.lastIndexOf("/") + 1);
      String localDataFilePath = properties.getProperty("epn.local.workDir") +  "/" + fileName;
      try {
        String removeDataCmd = "rm -f " + localDataFilePath;
        Process removeDataProcess = Runtime.getRuntime().exec(removeDataCmd);
        if (removeDataProcess.waitFor() != 0 ) {
          throw new Exception("rmCmdException");
        }
      } catch (Exception e) {
        System.out.println("failed delete local file: " + fileName);
        e.printStackTrace();
      }
    });
  }
}

