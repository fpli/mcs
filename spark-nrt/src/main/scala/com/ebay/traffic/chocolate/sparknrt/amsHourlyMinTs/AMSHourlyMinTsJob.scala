package com.ebay.traffic.chocolate.sparknrt.amsHourlyMinTs

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.sql.functions._

/**
  * Created by jialili1 on 06/21/19.
  */
object AMSHourlyMinTsJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)
    val job = new AMSHourlyMinTsJob(params)

    job.run()
    job.stop()
  }
}

class AMSHourlyMinTsJob(params: Parameter) extends
  BaseSparkNrtJob(params.appName, params.mode) {

  @transient lazy val metadata = {
    Metadata(params.workDir, params.channel, MetadataEnum.convertToMetadataEnum(params.usage))
  }

  @transient lazy val schema_epn_click = TableSchema("df_epn_click.json")

  override def run(): Unit = {
    var epnnrtResult = metadata.readDedupeOutputMeta(params.metaSuffix)

    var minTsArray : Array[Long] = new Array[Long](0)

    // get minimum timestamp
    epnnrtResult.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2

      datesFiles.foreach(datesFile => {
        val df = readFilesAsDFEx(datesFile._2, schema_epn_click.dfSchema, "csv", "tab", false)
          .select("CLICK_TS")

        val head = df.take(1)
        if (head.length == 0) {
          logger.info("No data!")
        } else {
          var minTsInThisFile = df.agg(min(df.col("CLICK_TS"))).head().getLong(0)
          minTsArray :+ minTsInThisFile
        }
      })
    })

    // write minimum timestamp to hdfs file
    var outputStream: FSDataOutputStream = null
    try {
      outputStream = fs.create(new Path(params.outputDir))
      outputStream.writeLong(minTsArray.min)
      outputStream.flush()
    } finally {
      if (outputStream != null) {
        outputStream.close()
      }
    }
  }

}
