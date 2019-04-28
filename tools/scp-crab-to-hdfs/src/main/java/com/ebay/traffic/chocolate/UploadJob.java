package com.ebay.traffic.chocolate;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.net.URI;

/**
 * Created by ganghuang on 12/3/18.
 */
public class UploadJob {

    private static FileSystem fs;

    private static String srcPath;
    private static String fileName;
    private static String middlePath;
    private static String destPath;

    public static void main(String args[]) throws Exception {
        assert args != null;
        if(args.length != 4) {
            System.out.println("UAGES: java -classpath crab-scp-hdfs*.jar com.ebay.traffic.chocolate.UploadJob");
            throw new Exception("init error");
        }
        srcPath = args[0];
        fileName = args[1];
        middlePath = args[2];
        destPath = args[3];
        try {
            init();
            runJob();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        } finally {
            shutdown();
        }
    }

    /**
     * init for the job
     * @throws IOException fast fail
     */
    private static void init() throws IOException {
        // init hdfs filesystem
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.addResource(Object.class.getResourceAsStream("/core-site.xml"));
        conf.addResource(Object.class.getResourceAsStream("/hdfs-site.xml"));

        System.setProperty("HADOOP_USER_NAME", "chocolate");
        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("chocolate"));

        fs = FileSystem.get(URI.create(destPath), conf);
    }

    /**
     * shutdown job
     */
    private static void shutdown() throws IOException {
        fs.close();
        System.out.println("shutdown success");
    }

    private static void runJob() throws Exception {
        fs.copyFromLocalFile(new Path(srcPath), new Path(middlePath + "/" + fileName));
        fs.rename(new Path(middlePath + "/" + fileName), new Path(destPath + "/" + fileName));
    }

}
