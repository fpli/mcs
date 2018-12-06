package com.ebay.traffic.chocolate.job

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.util.Parameter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

class CheckJobTest extends BaseFunSuite {

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  val tmpPath = createTempPath()
  val outputDir = tmpPath + "/checkJob/"


  test("test check files job") {
    val parameter = new Parameter("checkJob",
      "local",
      outputDir + "countFileDir",
      "1543477214789l",
      getTestResourcePath("testCheck"),
      "http://10.148.181.34:9200");
    val job = new CheckJob(parameter);

    job.run()
    val df = job.spark.read.csv(outputDir + "countFileDir");
    assert(df.count() == 1);
    job.stop()
  }
}
