package com.ebay.traffic.chocolate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

public class ScpJobTest {

    private static Properties props;
    private static String etlDir;
    private static FileSystem fileSystem;
    private static MiniDFSCluster hdfsCluster;

    @BeforeClass
    public static void init() throws IOException {
        // prepare test dir
        String baseDir = Files.createTempDirectory("test").toString();
        String localWordDir = baseDir + "/" + "chocowork";
        String hdfsBaseDir = baseDir + "/" + "hdfs";
        etlDir = baseDir + "/" + "rvr_test";
        new File(etlDir).mkdir();

        // prepare test data in hdfs
        System.setProperty("HADOOP_USER_NAME", "utuser");
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsBaseDir);
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        fileSystem = hdfsCluster.getFileSystem();
        fileSystem.mkdirs(new Path("/outDir"));
        String hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";

        // prepare properties file
        props =  new Properties();
        props.setProperty("imkscp.local.workDir", localWordDir);
        props.setProperty("imkscp.hdfs.outputDir", "/outDir");
        props.setProperty("imkscp.hdfs.uri", hdfsURI);
        props.setProperty("imkscp.hdfs.username", "utuser");
//        props.setProperty("imkscp.etl.command", "scp -i ~/.ssh/id_rsa_stack %s stack@choco-cent-2218410.lvs02.dev.ebayc3.com:/home/stack/%s");
        props.setProperty("imkscp.es.url", "http://10.148.181.34:9200");
        props.setProperty("imkscp.es.prefix", "chocolate-metrics-");
        props.setProperty("imkscp.hdfs.fileprefix", "imk_rvr_trckng_");

    }

    @Test
    public void testJob() throws Exception {
        String testDataFileName = "imk_rvr_trckng_20181211_183858.V4.HostName1.PAID_SEARCH.dat.gz";
        fileSystem.copyFromLocalFile(new Path(new File("src/test/resources/testData.dat.gz").getAbsolutePath()),
          new Path("/outDir/" + testDataFileName));
        props.setProperty("imkscp.etl.command", "cp %s " + etlDir + "/%s");

        Assert.assertTrue(fileSystem.exists(new Path("/outDir/" + testDataFileName)));
        Assert.assertTrue(!(new File(etlDir + "/" + testDataFileName)).exists());

        ScpJob.injectProperties(props);
        String[] args = {"qa"};
        ScpJob.main(args);

        Assert.assertTrue(!fileSystem.exists(new Path("/outDir/" + testDataFileName)));
        Assert.assertTrue((new File(etlDir + "/" + testDataFileName)).exists());
    }

    @Test
    public void testFastFail() throws Exception {
        String testDataFileName = "imk_rvr_trckng_20181211_183858.V4.HostName2.PAID_SEARCH.dat.gz";
        fileSystem.copyFromLocalFile(new Path(new File("src/test/resources/testData.dat.gz").getAbsolutePath()),
          new Path("/outDir/" + testDataFileName));
        props.setProperty("imkscp.etl.command", "ut %s " + etlDir + "/%s");

        Assert.assertTrue(fileSystem.exists(new Path("/outDir/" + testDataFileName)));
        Assert.assertTrue(!(new File(etlDir + "/" + testDataFileName)).exists());

        ScpJob.injectProperties(props);
        String[] args = {"qa"};
        boolean jobResult = true;
        try {
            ScpJob.main(args);
        } catch (Exception e) {
            jobResult = false;
        }
        Assert.assertFalse(jobResult);

        Assert.assertTrue(fileSystem.exists(new Path("/outDir/" + testDataFileName)));
    }



    @AfterClass
    public static void shutdown() {
        hdfsCluster.shutdown();
    }

}
