package com.ebay.traffic.chocolate;

import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import com.google.common.io.Files;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.*;
import java.net.URI;
import java.util.Properties;

public class ScpJob {
    private static Logger logger = LoggerFactory.getLogger(ScpJob.class);

    private static Properties properties;

    private static FileSystem fs;

    private static ESMetrics esMetrics;

    public static void main(String args[]) throws Exception {
        assert args != null;
        if(args.length != 1) logger.error("USAGE: [env]");
        String env = "prod".equalsIgnoreCase(args[0]) ? "prod" : "qa";
        init(env);
        runJob();
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
        // load priperties
        if(properties == null) {
            properties = new Properties();
            InputStream in = Object.class.getResourceAsStream("/" + env + "/imkscp.properties");
            properties.load(in);
        }

        // init hdfs filesystem
        Configuration conf = new Configuration();
        System.setProperty("HADOOP_USER_NAME", properties.getProperty("imkscp.hdfs.username"));
        fs = FileSystem.get(URI.create(properties.getProperty("imkscp.hdfs.uri")), conf);

        ESMetrics.init(properties.getProperty("imkscp.es.prefix"), properties.getProperty("imkscp.es.url"));
        esMetrics = ESMetrics.getInstance();
    }

    /**
     * list all data files in chocolate hdfs.
     * each time:
     * 1. get one data file from hdfs, sava the data file to local and touch one done file,
     * 2. scp one local data file and one local done file to ETL
     * 3. delete one local data file and done file
     * 4. delete the data file in chocolate hdfs
     * @throws Exception fast fail if any exception
     */
    private static void runJob() throws Exception {
        RemoteIterator<LocatedFileStatus> ite = fs.listFiles(new Path(properties.getProperty("imkscp.hdfs.outputDir")), true);
        while (ite.hasNext()) {
            LocatedFileStatus status = ite.next();
            if (status.isFile() && status.getPath().getName().startsWith(properties.getProperty("imkscp.hdfs.fileprefix"))) {
                getFileInHdfs(status.getPath().toString(), status.getPath().getName());
                scpToETL(status.getPath().getName());
                deleteLocalFile(status.getPath().getName());
                deleteFileInHdfs(status.getPath().toString());
            }
        }
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
            esMetrics.meter("imk.dump.count.getHdfs");
        } catch (Exception e) {
            esMetrics.meter("imk.dump.error.getHdfs");
            logger.error("failed get file from hdfs: " + fileName, e);
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
            esMetrics.meter("imk.dump.count.scpToEtl");
        } catch (Exception e) {
            esMetrics.meter("imk.dump.error.scpToEtl");
            logger.error("failed scp file to etl: " + fileName, e);
            throw new Exception(e);
        }
    }

    /**
     * delete local data and done file if scp success
     * not fast fail, ok when failed delete local tmp file
     * @param fileName the file name
     */
    private static void deleteLocalFile(String fileName) {
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
            esMetrics.meter("imk.dump.count.deleteLocal");
        } catch (Exception e) {
            esMetrics.meter("imk.dump.error.deleteLocal");
            logger.error("failed delete local file: " + fileName, e);
        }
    }

    /**
     * delete one data file in chocolate hdfs
     * not fast fail, ok when failed delete data file in hdfs
     * @param filePath full path of file in hdfs
     */
    private static void deleteFileInHdfs(String filePath) {
        try {
            fs.delete(new Path(filePath), false);
            esMetrics.meter("imk.dump.count.deleteHdfs");
        } catch (IOException e) {
            esMetrics.meter("imk.dump.error.deleteHdfs");
            logger.error("failed delete file in Hdfs: " + filePath, e);
        }
    }

}
