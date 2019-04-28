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

/**
 * Created by ganghuang on 12/3/18.
 */
public class ScpJobTest {

    private static Properties props;
    private static String etlDir;
    private static FileSystem fileSystem;
    private static MiniDFSCluster hdfsCluster;

    @BeforeClass
    public static void init() throws IOException {
        // prepare test dir
        String baseDir = Files.createTempDirectory("test").toString();
        String localWorkDir = baseDir + "/" + "chocowork";
        String hdfsBaseDir = baseDir + "/";
        etlDir = baseDir + "/" + "rvr_test";
        new File(etlDir).mkdir();

        // prepare test data in hdfs
        System.setProperty("HADOOP_USER_NAME", "utuser");
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsBaseDir);
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        fileSystem = hdfsCluster.getFileSystem();
        fileSystem.mkdirs(new Path("/metaDir"));
        String hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";

        // prepare properties file
        props =  new Properties();
        props.setProperty("imkscp.hdfs.metaDir", "/metaDir");
        props.setProperty("imkscp.hdfs.uri", hdfsURI);
        props.setProperty("imkscp.hdfs.username", "utuser");
        props.setProperty("imkscp.local.workDir", localWorkDir);
//        props.setProperty("imkscp.etl.command", "scp -i ~/.ssh/id_rsa_stack %s stack@choco-cent-2218410.lvs02.dev.ebayc3.com:/home/stack/%s");
        props.setProperty("imkscp.es.url", "http://10.148.181.34:9200");
        props.setProperty("imkscp.es.prefix", "chocolate-metrics-");
        props.setProperty("imkscp.hdfs.fileprefix", "imk_rvr_trckng_");

    }

    @Test
    public void testJob() throws Exception {

        String testDataFileName = "testData.dat.gz";

        fileSystem.copyFromLocalFile(new Path(new File("src/test/resources/ut.meta.etl").getAbsolutePath()),
          new Path("/metaDir/ut.meta.etl"));
        fileSystem.copyFromLocalFile(new Path(new File("src/test/resources/testData.dat.gz").getAbsolutePath()),
          new Path("/outDir/" + testDataFileName));

        props.setProperty("imkscp.etl.command", "cp %s " + etlDir + "/%s");

        Assert.assertTrue(fileSystem.exists(new Path("/outDir/" + testDataFileName)));
        Assert.assertTrue(!(new File(etlDir + "/" + testDataFileName)).exists());

        ScpJob.injectProperties(props);
        String[] args = {"qa"};
        ScpJob.main(args);

        Assert.assertTrue((new File(etlDir + "/" + testDataFileName)).exists());
    }

    @AfterClass
    public static void shutdown() {
        hdfsCluster.shutdown();
    }

}
