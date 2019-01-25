package com.ebay.traffic.chocolate;

import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.curator.shaded.com.google.common.io.Files;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by ganghuang on 12/3/18.
 */
public class ScpJob {

    private static Properties properties;

    private static FileSystem fs;

    private static Metrics metrics;

    private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();

    public static void main(String args[]) throws Exception {
        assert args != null;
        if(args.length != 1) {
            System.out.println("UAGES: java -classpath cho-panda-etl-scp-*.jar com.ebay.traffic.chocolate.ScpJob [env]");
            throw new Exception("init error");
        }
        String env = "prod".equalsIgnoreCase(args[0]) ? "prod" : "qa";
        try {
            init(env);
            runJob();
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e);
        } finally {
            shutdown();
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
     * init for the job
     * @param env qa or prod
     * @throws IOException fast fail
     */
    private static void init(String env) throws IOException {
        System.out.println("start init");
        // load priperties
        if(properties == null) {
            properties = new Properties();
            InputStream in = Object.class.getResourceAsStream("/" + env + "/imkscp.properties");
            properties.load(in);
        }

        // init hdfs filesystem
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        if ("prod".equals(env)) {
            conf.addResource(Object.class.getResourceAsStream("/prod/core-site.xml"));
            conf.addResource(Object.class.getResourceAsStream("/prod/hdfs-site.xml"));
        }
        System.setProperty("HADOOP_USER_NAME", properties.getProperty("imkscp.hdfs.username"));
        fs = FileSystem.get(URI.create(properties.getProperty("imkscp.hdfs.uri")), conf);

        ESMetrics.init(properties.getProperty("imkscp.es.prefix"), properties.getProperty("imkscp.es.url"));
        metrics = ESMetrics.getInstance();
        System.out.println("init success");
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

    /**
     * 1. get data file from hdfs by read meta file, sava the data file to local and touch one done file,
     * 2. scp local data file and local done file to ETL
     * 3. delete local data file and done file
     * 4. delete meta file in hdfs
     * @throws Exception fast fail if any exception
     */
    private static void runJob() throws Exception {
        RemoteIterator<LocatedFileStatus> ite = fs.listFiles(new Path(properties.getProperty("imkscp.hdfs.metaDir")), true);
        while (ite.hasNext()) {
            LocatedFileStatus status = ite.next();
            if (status.isFile() && status.getPath().getName().endsWith("meta.etl")) {
                List<String> dataFiles = readMetaFile(status.getPath().toString());
                for (String dataFile : dataFiles) {
                    String fileName = dataFile.substring(dataFile.lastIndexOf("/") + 1, dataFile.length());
                    getFileInHdfs(dataFile, fileName);
                    scpToETL(fileName);
                    deleteLocalFile(fileName);
                }
                fs.delete(new Path(status.getPath().toString()), true);
            }
        }
    }

    /**
     * read meta file in chocolate hdfs
     * @param path full path of meta file
     * @return list of data files
     * @throws IOException IOException
     */
    private static List<String> readMetaFile(String path) throws IOException {
        List<String> dataFiles = new ArrayList<>();
        FSDataInputStream inputStream = fs.open(new Path(path));
        String out= IOUtils.toString(inputStream, "UTF-8");
        inputStream.close();
        MetaFiles metaFiles = gson.fromJson(out, MetaFiles.class);
        for (DateFiles dateFiles : metaFiles.getMetaFiles()) {
            dataFiles.addAll(Arrays.asList(dateFiles.getFiles()));
        }
        return dataFiles;
    }

    /**
     * get one data file from chocolate hdfs to local and touch one done file to local
     * @param filePath full path of file in hdfs
     * @param fileName file name
     * @throws Exception fast throw any exception
     */
    private static void getFileInHdfs(String filePath, String fileName) throws Exception {
        try {
            fs.copyToLocalFile(new Path(filePath), new Path(properties.getProperty("imkscp.local.workDir") + "/" + fileName));
            Files.touch(new File(properties.getProperty("imkscp.local.workDir") + "/" + fileName + ".done"));
            metrics.meter("imk.dump.count.getHdfs");
        } catch (Exception e) {
            metrics.meter("imk.dump.error.getHdfs");
            System.out.println("failed get file from hdfs: " + fileName);
            e.printStackTrace();
            throw new Exception(e);
        }
    }

    /**
     * scp one local data file to etl, scp one done file to etl
     * @param fileName the file name
     * @throws Exception fast throw any exception
     */
    private static void scpToETL(String fileName) throws Exception {
        String localDataFilePath = properties.getProperty("imkscp.local.workDir") + "/" + fileName;
        String localDoneFilePath = properties.getProperty("imkscp.local.workDir") + "/" + fileName + ".done";

        try {
            String scpDataCmd = String.format(properties.getProperty("imkscp.etl.command"), localDataFilePath, fileName);
            String scpDoneCmd = String.format(properties.getProperty("imkscp.etl.command"), localDoneFilePath, fileName + ".done");
            Process scpDataProcess = Runtime.getRuntime().exec(scpDataCmd);
            if (scpDataProcess.waitFor() != 0) {
                throw new Exception("scpCmdException");
            }
            Process scpDoneProcess = Runtime.getRuntime().exec(scpDoneCmd);
            if (scpDoneProcess.waitFor() != 0) {
                throw new Exception("scpCmdException");
            }
            metrics.meter("imk.dump.count.scpToEtl");
        } catch (Exception e) {
            metrics.meter("imk.dump.error.scpToEtl");
            System.out.println("failed scp file to etl: " + fileName);
            e.printStackTrace();
            throw new Exception(e);
        }
    }

    /**
     * delete local data and done file if scp success
     * @param fileName the file name
     */
    private static void deleteLocalFile(String fileName) throws Exception {
        String localDataFilePath = properties.getProperty("imkscp.local.workDir") + "/" + fileName;
        String localDoneFilePath = properties.getProperty("imkscp.local.workDir") + "/" + fileName + ".done";

        try {
            String removeDataCmd = "rm " + localDataFilePath;
            String removeDoneCmd = "rm " + localDoneFilePath;
            Process removeDataProcess = Runtime.getRuntime().exec(removeDataCmd);
            Process removeDoneProcess = Runtime.getRuntime().exec(removeDoneCmd);
            if (removeDataProcess.waitFor() != 0 || removeDoneProcess.waitFor() != 0) {
                throw new Exception("rmCmdException");
            }
            metrics.meter("imk.dump.count.deleteLocal");
        } catch (Exception e) {
            metrics.meter("imk.dump.error.deleteLocal");
            System.out.println("failed delete local file: " + fileName);
            e.printStackTrace();
            throw new Exception(e);
        }
    }

}
