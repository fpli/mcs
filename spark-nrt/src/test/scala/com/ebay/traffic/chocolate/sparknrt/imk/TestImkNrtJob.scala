/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.imk


import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.imk.ImkNrtJob
import com.ebay.traffic.chocolate.sparknrt.imk.Parameter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TestImkNrtJob extends BaseFunSuite{

  private val tmpPath = createTempDir()
  private val deltaDir = tmpPath + "/apps/delta/tracking-events"
  private val outPutDir = tmpPath + "/apps/tracking-events"

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {
    fs.mkdirs(new Path(deltaDir))
    fs.mkdirs(new Path(outPutDir))
  }

  test("test imk etl job for parquet output") {
    val job = new ImkNrtJob(Parameter(Array(
      "--mode", "local[8]",
      "--deltaDir", deltaDir,
      "--outPutDir", outPutDir,
      "--partitions", "1"
    )))

    job.run()
    job.stop()
  }
}
