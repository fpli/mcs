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
 * Created by huiclu on 12/3/18.
 */
public class EPNScpTest {

    private static Properties props;
    private static String etlDir;
    private static String hdfsBaseDir;
    private static FileSystem fileSystem;
    private static MiniDFSCluster hdfsCluster;

    @BeforeClass
    public static void init() throws IOException {
        // prepare test dir
        String baseDir = Files.createTempDirectory("test").toString();
        String localWorkDir = baseDir + "/" + "epn-nrt";
        hdfsBaseDir = baseDir + "/";
        etlDir = baseDir + "/" + "epn_scp_test";
        new File(etlDir).mkdir();

        // prepare test data in hdfs
        System.setProperty("HADOOP_USER_NAME", "utuser");
        Configuration conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsBaseDir);
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        fileSystem = hdfsCluster.getFileSystem();
        fileSystem.mkdirs(new Path("/metaDir_click"));
        fileSystem.mkdirs(new Path("/metaDir_imp"));
        fileSystem.mkdirs(new Path("/epnnrt/click"));
        fileSystem.mkdirs(new Path("/epnnrt/imp"));
        String hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";

        // prepare properties file
        props =  new Properties();
        props.setProperty("epn.hdfs.click.metaDir", "/metaDir_click");
        props.setProperty("epn.hdfs.imp.metaDir", "/metaDir_imp");
        props.setProperty("epn.hdfs.uri", hdfsURI);
        props.setProperty("epn.hdfs.username", "utuser");
        props.setProperty("epn.local.workDir", localWorkDir);
        props.setProperty("epn.es.url", "http://10.148.181.34:9200");
        props.setProperty("epn.es.prefix", "chocolate-metrics-");
        props.setProperty("epn.hdfs.fileprefix", "epnnrt");
        props.setProperty("epn.apollo.path", "/epnnrt/");

    }

    @Test
    public void testJob() throws Exception {
        fileSystem.copyFromLocalFile(new Path(new File("src/test/resources/ut.click.meta.epnnrt").getAbsolutePath()),
          new Path("/metaDir_click/ut.click.meta.epnnrt"));
        fileSystem.copyFromLocalFile(new Path(new File("src/test/resources/testData.dat.gz").getAbsolutePath()),
          new Path("/outDir/" + "testData.dat.gz"));
        fileSystem.copyFromLocalFile(new Path(new File("src/test/resources/testData.dat3.gz").getAbsolutePath()),
            new Path("/outDir/" + "testData.dat3.gz"));
        fileSystem.copyFromLocalFile(new Path(new File("src/test/resources/testData.dat2.gz").getAbsolutePath()),
            new Path("/outDir/" + "testData.dat2.gz"));
        fileSystem.copyFromLocalFile(new Path(new File("src/test/resources/ut.imp.meta.epnnrt").getAbsolutePath()),
            new Path("/metaDir_imp/ut.imp.meta.epnnrt"));
        fileSystem.copyFromLocalFile(new Path(new File("src/test/resources/testData.dat1.gz").getAbsolutePath()),
            new Path("/outDir/" + "testData.dat1.gz"));

        props.setProperty("epn.etl.command", "cp %s " + etlDir + "/%s");

        Assert.assertTrue(fileSystem.exists(new Path("/outDir/testData.dat.gz")));
        Assert.assertTrue(!(new File(etlDir + "/testData.dat.gz")).exists());
        Assert.assertTrue(!(new File(etlDir + "/testData.dat1.gz")).exists());
        Assert.assertTrue(!(new File(etlDir + "/testData.dat2.gz")).exists());
        Assert.assertTrue(!(new File(etlDir + "/testData.dat3.gz")).exists());

        EPNScp.injectProperties(props);
        String[] args = {"qa"};
        EPNScp.main(args);

        Assert.assertTrue((new File(etlDir + "/testData.dat.gz")).exists());
        Assert.assertTrue((new File(etlDir + "/testData.dat1.gz")).exists());
        Assert.assertTrue((new File(etlDir + "/testData.dat2.gz")).exists());
        Assert.assertTrue((new File(etlDir + "/testData.dat3.gz")).exists());
    }

    @AfterClass
    public static void shutdown() {
        hdfsCluster.shutdown();
    }

}
