package com.ebay.traffic.chocolate.sparknrt.compare

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.common.schema.TableSchema
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.sql.functions.{col, udf, when}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


class TestCompareJob(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode) {
  import spark.implicits._

  override def run(): Unit = {

    @transient lazy val click_columns: Array[String] = {
      val column_list = Array("ROVER_URL", "LND_PAGE_URL_NAME","y_LND_PAGE_URL_NAME", "c_LND_PAGE_URL_NAME")
    //  val column_list = Array("ROVER_URL", "LND_PAGE_URL_NAME","y_LND_PAGE_URL_NAME", "AMS_PAGE_TYPE_MAP_ID", "y_AMS_PAGE_TYPE_MAP_ID")

      column_list
    }

    val amsClickSchema: StructType = StructType(
      Seq(
        StructField("ROVER_URL", StringType, nullable = true),
        StructField("LND_PAGE_URL_NAME", StringType, nullable = true),
        StructField("AMS_PAGE_TYPE_MAP_ID", StringType, nullable = true),
        StructField("y_AMS_PAGE_TYPE_MAP_ID", StringType, nullable = true)
      )
    )

    val schema = TableSchema("df_epn_click.json")
    val your_schema = TableSchema("df_your_epn_click.json")

    val removeParamsUdf = udf(removeParams(_: String))
    val normalizeUrlUdf = udf((roverUrl: String) => normalizeUrl(roverUrl))

    val testDf = readFilesAsDF("/Users/huiclu/tmp_1/page1.csv", amsClickSchema, "csv", "tab", false)

    val ourDf = readFilesAsDF(params.click_source, schema.dfSchema, "csv", "tab", false)
      .withColumn("my_uri", removeParamsUdf($"ROVER_URL"))

    val yourDf = readFilesAsDF(params.click_dest, your_schema.dfSchema, "csv", "tab", false)
      .withColumn("your_uri", normalizeUrlUdf(col("y_ROVER_URL")))


    val verifyResultUdf = udf(verifyLandingPage(_: String, _: String))


    val df = ourDf.join(yourDf, $"my_uri" === $"your_uri" && $"CRLTN_GUID_TXT" === $"y_CRLTN_GUID_TXT", "inner")
      .withColumn("c_LND_PAGE_URL_NAME", verifyResultUdf($"LND_PAGE_URL_NAME", $"y_LND_PAGE_URL_NAME"))


    var df_res = df.select(click_columns.head, click_columns.tail: _*).where($"c_LND_PAGE_URL_NAME" === false)

    logger.info("diff count is huiclu: " + df_res.count())

    val path = new Path(params.workDir, new Path(params.click_outputPath).getName)
    fs.delete(path, true)

    //df_res = df_res.limit(1000)
    saveDFToFiles(df_res, path.toString, "gzip", "csv", "tab")

/*    val df_tmp = readFilesAsDF(path.toString, null, "csv", "tab", false)
    val total = df_tmp.count()*/




    /*var outputStream: FSDataOutputStream = null


    try {
      outputStream = fs.create(new Path(params.click_outputPath))

      outputStream.writeChars("--------------------------------------------------------" + "\n")

      outputStream.writeChars(s"LND_PAGE_URL_NAME inconsistent count : $total \n")


      outputStream.flush()

    } finally {
      if (outputStream != null) {
        outputStream.close()
      }
    }*/
  }

  def verifyResult(my: String, your: String): Boolean = {
    if (my == null && your == null)
      return true
    if (my != null && your != null)
      return my.equalsIgnoreCase(your)
    false
  }

  def verifyLandingPage(my: String, your: String): Boolean = {
    if (my == null && your == null)
      return true
    if (my != null && your != null) {
      if (my.equalsIgnoreCase(your)) {
        return true
      } else {
        logger.info("our landing page is: " + my)
        logger.info("Their landing page is: " + your)
        return false
      }
    }
    false
  }

  def verifyPageId(my: String, your: String): Boolean = {
    if (my == null && your == null)
      return true
    if (my != null && your != null) {
      if (my.equalsIgnoreCase(your)) {
        return true
      } else {

        logger.info("our landing page is: " + my)
        logger.info("Their landing page is: " + your)
        return false
      }
    }
    return my.equalsIgnoreCase(your)
    false
  }


  def removeParams(url: String): String = {
    if (url == null)
      return ""
    val splitter = url.split("&").filter(item => !item.startsWith("dashenId") && !item.startsWith("dashenCnt"))
    var res = splitter.mkString("&")

    if (res.startsWith("http://")) {
      res = res.substring(7)
    }else if (res.startsWith("https://")) {
      res = res.substring(8)
    }
    res
  }

  // should remove raptor=1 from rover URL
  def normalizeUrl(url: String): String = {
    if (url == null)
      return ""
    var result = url.replace("raptor=1", "")
    val lastChar = result.charAt(result.length - 1)
    if (lastChar == '&' || lastChar == '?') {
      result = result.substring(0, result.length - 1)
    }
    if (result.startsWith("http://")) {
      result = result.substring(7)
    }else if (result.startsWith("https://")) {
      result = result.substring(8)
    }
    result
  }

}
