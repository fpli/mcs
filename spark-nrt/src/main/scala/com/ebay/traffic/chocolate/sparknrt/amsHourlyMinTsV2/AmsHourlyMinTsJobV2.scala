package com.ebay.traffic.chocolate.sparknrt.amsHourlyMinTsV2

import java.text.SimpleDateFormat

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.sql.functions._


object AmsHourlyMinTsJobV2 extends App {
  override def main(args: Array[String]): Unit = {
    val params = ParameterV2(args)
    val job = new AmsHourlyMinTsJobV2(params)

    job.run()
    job.stop()
  }
}

class AmsHourlyMinTsJobV2(params: ParameterV2) extends
  BaseSparkNrtJob(params.appName, params.mode) {

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  @transient lazy val metadata = {
    Metadata(params.workDir, params.channel, MetadataEnum.convertToMetadataEnum(params.usage))
  }

  var ts_col = ""
  if (params.usage == "epnnrt_scp_click") {
    ts_col = "CLICK_TS"
  }
  else if (params.usage == "epnnrt_scp_imp") {
    ts_col = "IMPRSN_TS"
  }
  else
    logger.warn("Wrong usage!")

  override def run(): Unit = {
    val epnnrtResult = metadata.readDedupeOutputMeta(params.metaSuffix)

    var minTsArray : Array[Long] = new Array[Long](0)

    // get minimum timestamps from all files
    if (epnnrtResult.length > 0) {
      epnnrtResult.foreach(metaIter => {
        val datesFiles = metaIter._2

        datesFiles.foreach(datesFile => {
          val df = readFilesAsDFEx(datesFile._2)
            .select(ts_col)

          val head = df.take(1)
          if (head.length == 0) {
            logger.info("No data!")
          } else {
            val minTsInThisFile = getTimestamp(df.agg(min(df.col(ts_col))).head().getTimestamp(0).toString)
            minTsArray :+= minTsInThisFile
          }
        })
      })

      // write the minimum timestamp to hdfs file
      if (minTsArray.length > 0) {
        var outputStream: FSDataOutputStream = null
        try {
          outputStream = fs.create(new Path(params.outputDir))
          outputStream.writeChars(minTsArray.min.toString)
          outputStream.flush()
        } finally {
          if (outputStream != null) {
            outputStream.close()
          }
        }
      }
    }
  }

  def getTimestamp(date: String): Long = {
    sdf.parse(date).getTime
  }

}