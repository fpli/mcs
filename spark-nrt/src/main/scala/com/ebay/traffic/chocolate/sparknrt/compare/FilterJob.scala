package com.ebay.traffic.chocolate.sparknrt.compare

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.common.schema.TableSchema
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.sql.functions.udf

class FilterJob(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode) {
  import spark.implicits._
  override def run(): Unit = {
    val schema = TableSchema("df_epn_click.json")

    val compareTimeUdf = udf(compareTime(_: String))

    var raw_df = readFilesAsDF(params.click_source, schema.dfSchema, "csv", "tab", false)

    val beforeCount = raw_df.count()

    raw_df = raw_df.filter(compareTimeUdf($"CLICK_TS"))

    raw_df.repartition(3)
    saveDFToFiles(raw_df, params.click_source, "gzip", "csv", "tab")

    val afterCount = raw_df.count()

    var outputStream: FSDataOutputStream = null

    try {

      outputStream = fs.create(new Path(params.click_outputPath))
      outputStream.writeChars(s"Raw count: $beforeCount \n")
      outputStream.writeChars(s"After filter count: $afterCount \n")


      outputStream.flush()
    } finally {
      if(outputStream != null) {
        outputStream.close()
      }
    }

  }

  def compareTime(str: String): Boolean = {
    false
  }

}
