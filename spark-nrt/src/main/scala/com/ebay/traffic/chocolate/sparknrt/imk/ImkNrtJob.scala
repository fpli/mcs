/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.imk

import java.io.File

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.basenrt.BaseNrtJob
import com.ebay.traffic.chocolate.sparknrt.imkETL.Parameter
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._

object ImkNrtJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkNrtJob(params)

    job.run()
    job.stop()
  }
}

class ImkNrtJob(params: Parameter) extends BaseNrtJob(params.appName, params.mode) {

  lazy val imkDeltaDir = params.deltaDir + "imk"

  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to run the spark job.
    */
  override def run(): Unit = {

    val file = new File("/tmp/delta")

    val path = file.getCanonicalPath
    println(path)
    val data = spark.range(0,5)
    data.printSchema()
    data.toDF()

    saveDFToFiles(data.toDF(), path, outputFormat = "delta")

    val imkDelta = DeltaTable.forPath(spark, path)
    imkDelta.toDF.show()
//    val dfImk = imkDelta
//      .update(
//        col("eventType") === "click",
//        Map("eventType" -> lit("click")))
  }
}
