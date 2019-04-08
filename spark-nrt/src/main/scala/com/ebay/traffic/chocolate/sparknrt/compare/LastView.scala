package com.ebay.traffic.chocolate.sparknrt.compare

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.common.schema.TableSchema
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.sql.functions.{col, udf}


class LastView(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode) {
  import spark.implicits._

  override def run(): Unit = {

    val schema = TableSchema("df_epn_click.json")
    val your_schema = TableSchema("df_your_epn_click.json")

    @transient lazy val our_columns: Array[String] = {
      val column_list = Array("my_uri","CRLTN_GUID_TXT", "LAST_VWD_ITEM_ID", "LAST_VWD_ITEM_TS")
      column_list
    }

    @transient lazy val your_columns: Array[String] = {
      val column_list = Array("your_uri", "y_CRLTN_GUID_TXT", "y_LAST_VWD_ITEM_ID", "y_LAST_VWD_ITEM_TS")
      column_list
    }

    val removeParamsUdf = udf(removeParams(_: String))
    val normalizeUrlUdf = udf((roverUrl: String) => normalizeUrl(roverUrl))

    var ourDf = readFilesAsDF(params.click_source, schema.dfSchema, "csv", "tab", false)
      .withColumn("my_uri", removeParamsUdf($"ROVER_URL"))
    ourDf = ourDf.select(our_columns.head, our_columns.tail: _*)

    val ourBeforeDedupeCount = ourDf.count()
    ourDf = ourDf.dropDuplicates("my_uri", "CRLTN_GUID_TXT")
    val ourAfterDedupeCount = ourDf.count()

    var yourDf = readFilesAsDF(params.click_dest, your_schema.dfSchema, "csv", "tab", false)
      .withColumn("your_uri", normalizeUrlUdf(col("y_ROVER_URL")))
    yourDf = yourDf.select(your_columns.head, your_columns.tail: _*)


    val epnBeforeDedupeCount = yourDf.count()
    yourDf = yourDf.dropDuplicates("your_uri", "y_CRLTN_GUID_TXT")
    val epnAfterDedupeCount = yourDf.count()


    val verifyResultUdf = udf(verifyResult(_: String, _: String))

    val verifyLastViewItemUdf = udf(verifyLastViewItemResult(_: String, _: String))


    var df = ourDf.join(yourDf, $"my_uri" === $"your_uri" && $"CRLTN_GUID_TXT" === $"y_CRLTN_GUID_TXT", "inner")
      .withColumn("c_LAST_VWD_ITEM_ID", verifyResultUdf($"LAST_VWD_ITEM_ID", $"y_LAST_VWD_ITEM_ID")).drop("LAST_VWD_ITEM_ID").drop("y_LAST_VWD_ITEM_ID")
      .withColumn("c_LAST_VWD_ITEM_TS", verifyLastViewItemUdf($"LAST_VWD_ITEM_TS", $"y_LAST_VWD_ITEM_TS")).drop("LAST_VWD_ITEM_TS").drop("y_LAST_VWD_ITEM_TS")
      .drop("ROVER_URL").drop("y_ROVER_URL")


    val path = new Path(params.workDir, new Path(params.click_outputPath).getName)
    fs.delete(path, true)
    saveDFToFiles(df, path.toString)
    df = readFilesAsDF(path.toString)
    val total = df.count()


    val LAST_VWD_ITEM_ID = df.where($"c_LAST_VWD_ITEM_ID" === false).count()
    val LAST_VWD_ITEM_TS = df.where($"c_LAST_VWD_ITEM_TS" === false).count()

    var outputStream: FSDataOutputStream = null

    try {
      outputStream = fs.create(new Path(params.click_outputPath))
      outputStream.writeChars(s"Chocolate Total(RAW): $ourBeforeDedupeCount \n")
      outputStream.writeChars(s"Chocolate Total(dedupe): $ourAfterDedupeCount \n")
      outputStream.writeChars(s"EPN Total(RAW): $epnBeforeDedupeCount \n")
      outputStream.writeChars(s"EPN Total(dedupe): $epnAfterDedupeCount \n")
      outputStream.writeChars(s"Join Total: $total \n")
      outputStream.writeChars(s"Chocolate join ratio: ${((total.toFloat/ourAfterDedupeCount)*100).toInt}% \n")
      outputStream.writeChars(s"EPN join ratio: ${((total.toFloat/epnAfterDedupeCount)*100).toInt}% \n")

      outputStream.writeChars("--------------------------------------------------------" + "\n")

      outputStream.writeChars(s"LAST_VWD_ITEM_ID inconsistent: ${((LAST_VWD_ITEM_ID.toFloat/total)*100).toInt}% \n")
      outputStream.writeChars(s"LAST_VWD_ITEM_TS inconsistent: ${((LAST_VWD_ITEM_TS.toFloat/total)*100).toInt}% \n")

      outputStream.flush()

    } finally {
      if (outputStream != null) {
        outputStream.close()
      }
    }
  }

  def verifyResult(my: String, your: String): Boolean = {
    if (my == null && your == null)
      return true
    if (my != null && your != null)
      return my.equalsIgnoreCase(your)
    false
  }

  def verifyResult2(my: String, epn_flag1: String, epn_flag2: String): Boolean = {
    if (my == null && epn_flag1 == null && epn_flag2 == null)
      return true
    if ((epn_flag1 != null && epn_flag1.equals("1")) || (epn_flag2 != null && epn_flag2.equals("1"))) {
      if (my.equals("1"))
        return true
    }
    false
  }

  def verifyClick_TSResult(my: String, your: String): Boolean = {
    if (my == null && your == null)
      return true
    if (my != null && your != null) {
      val my_ts = my.split(" ")
      val your_ts = my.split(" ")
      if (my_ts(0).equalsIgnoreCase(your_ts(0)))
        return true
    }
    false
  }

  def verifyLastViewItemResult(my: String, your: String): Boolean = {
    if (my == null && your == null)
      return true
    if (my != null && your != null) {
      val my_ts = my.split(".")
      val your_ts = my.split(".")
      if (my_ts(0).equalsIgnoreCase(your_ts(0)))
        return true
    }
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
