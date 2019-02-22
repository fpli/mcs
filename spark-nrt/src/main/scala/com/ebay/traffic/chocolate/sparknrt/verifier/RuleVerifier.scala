package com.ebay.traffic.chocolate.sparknrt.verifier

import java.util.regex.{Matcher, Pattern}

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object RuleVerifier {
  def main(args: Array[String]): Unit = {
    val params = Parameter(args)
    val job = new RuleVerifier(params)
    job.run()
    job.stop()
  }
}

class RuleVerifier(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode) {

  // Here lists all fields in order in ams_click that are required for verification.
  val amsClickSchema: StructType = StructType(
    Seq(
      StructField("click_ts", StringType, nullable = true),
      StructField("click_id", StringType, nullable = true),
      StructField("crltn_guid_txt", StringType, nullable = true),
      StructField("guid_txt", StringType, nullable = true),
      StructField("user_id", StringType, nullable = true),
      StructField("clnt_rmt_ip", StringType, nullable = true),
      StructField("pblshr_id", StringType, nullable = true),
      StructField("rfr_url_name", StringType, nullable = true),
      StructField("ams_trans_rsn_cd", StringType, nullable = true),
      StructField("rover_url_txt", StringType, nullable = true),
      StructField("rt_rule_flag1", StringType, nullable = true),
      StructField("rt_rule_flag2", StringType, nullable = true),
      StructField("rt_rule_flag3", StringType, nullable = true),
      StructField("rt_rule_flag4", StringType, nullable = true),
      StructField("rt_rule_flag5", StringType, nullable = true),
      StructField("rt_rule_flag6", StringType, nullable = true),
      StructField("rt_rule_flag7", StringType, nullable = true),
      StructField("rt_rule_flag8", StringType, nullable = true),
      StructField("rt_rule_flag9", StringType, nullable = true),
      StructField("rt_rule_flag10", StringType, nullable = true),
      StructField("rt_rule_flag12", StringType, nullable = true),
      StructField("rt_rule_flag13", StringType, nullable = true),
      StructField("rt_rule_flag15", StringType, nullable = true),
      StructField("nrt_rule_flag39", StringType, nullable = true),
      StructField("nrt_rule_flag43", StringType, nullable = true),
      StructField("nrt_rule_flag51", StringType, nullable = true),
      StructField("nrt_rule_flag53", StringType, nullable = true),
      StructField("nrt_rule_flag54", StringType, nullable = true),
      StructField("nrt_rule_flag56", StringType, nullable = true)
    )
  )

  import spark.implicits._

  lazy val workDir = params.workPath + "/tmp/"

  override def run(): Unit = {
    // 1. Load chocolate data
    logger.info("load data for inputpath1: " + params.srcPath)

    /*
    val containsDashenIdUdf = udf(containsDashenId(_: String))
    val dashenCntAbove1Udf = udf(dashenCntAbove1(_: String))
    val dfDashen = readFilesAsDF(params.srcPath)
      .where($"channel_action" === "CLICK" and $"channel_type" === "EPN" and containsDashenIdUdf($"uri") === "TRUE")
    val countDashenId = dfDashen.count()
    val countDashenCntAbove1 = dfDashen.where(dashenCntAbove1Udf($"uri") === "TRUE").count()
    saveDFToFiles(dfCguid = dfDashen, outputPath = workDir + "/chocolate/dashenid/" + new Path(params.srcPath).getName,
      compressFormat = null, outputFormat = "csv", delimiter = "space")

    println("number of records in dashenid: " + countDashenId)
    println("number of records in dashenCntAbove1: " + countDashenCntAbove1)
    */

    val removeParamsUdf = udf(removeParams(_: String))
    var df1 = readFilesAsDF(params.srcPath)
      .where($"channel_action" === "CLICK" and $"channel_type" === "EPN")
      .withColumn("new_uri", removeParamsUdf($"uri"))
      .drop("uri")
      .select($"new_uri", $"cguid", $"guid", $"request_headers", $"response_headers")

    val path1 = new Path(workDir + "/chocolate/", (new Path(params.srcPath).getName))
    fs.delete(path1, true)
    saveDFToFiles(df1, path1.toString)
    df1 = readFilesAsDF(path1.toString)
    val count1 = df1.count()
    println("show df1")
    df1.show(false)

    println("number of records in df1: " + count1)

    var dfNullCguid = df1.where($"cguid" === "").drop("guid")
//    var dfNullGuid = df1.where($"guid" === "").drop("cguid").repartition(1)

    val pathNullCguid = new Path(workDir + "/chocolate/nullCguid/", (new Path(params.srcPath).getName))
    fs.delete(pathNullCguid, true)
    saveDFToFiles(dfNullCguid, pathNullCguid.toString)
    dfNullCguid = readFilesAsDF(pathNullCguid.toString)
    val countNullCguid = dfNullCguid.count()

//    val pathNullGuid = new Path(workDir + "/chocolate/nullGuid/", (new Path(params.srcPath).getName))
//    fs.delete(pathNullGuid, true)
//    saveDFToFiles(dfNullGuid, pathNullGuid.toString)
//    dfNullGuid = readFilesAsDF(pathNullGuid.toString)
//    val countNullGuid = dfNullGuid.count()

    // 2. Load EPN data fetched from ams_click
    logger.info("load data for inputpath2: " + params.targetPath)

    val normalizeUrlUdf = udf((roverUrl: String) => normalizeUrl(roverUrl))
    // assume df2 only has columns that we want!
    var df2 = readFilesAsDF(params.targetPath, inputFormat = "csv", schema = amsClickSchema, delimiter = "bel")
      .withColumn("rover_url", normalizeUrlUdf(col("rover_url_txt")))
      .drop("rover_url_txt")
      .select($"rover_url", $"crltn_guid_txt", $"guid_txt")

    val path2 = new Path(workDir + "/epn/", (new Path(params.targetPath).getName))
    fs.delete(path2, true)
    saveDFToFiles(df2, path2.toString)
    df2 = readFilesAsDF(path2.toString)
    val count2 = df2.count()
    println("show df2")
    df2.show(false)

    println("number of records in df2: " + count2)

    // 3. Aggregation - join
    logger.info("start aggregation...")

    // define udf for RT rule verification
    val verifyByBitUdf1 = udf(verifyByBit(_: Int, _: Long, _: String))
    val verifyByBitUdf2 = udf(verifyByBit(_: Int, _: Long, _: String, _: String))

    var dfCguid = dfNullCguid.join(df2, $"new_uri" === $"rover_url", "inner")
      .select("request_headers", "response_headers", "new_uri", "crltn_guid_txt")
      .repartition(1)
    val joinNullCguid = dfCguid.count()
    val joinValidNullCguid = dfCguid.select("request_headers").dropDuplicates().count()

    val dfCguidInLocation = dfCguid.where(cguidInLocation() === $"crltn_guid_txt").repartition(1)
    val nullCguidInLocation = dfCguidInLocation.count()
    val nullCguidNowhere = countNullCguid - nullCguidInLocation

    val pathCguid = new Path(workDir + "/join/nullCguid/", (new Path(params.srcPath).getName))
    fs.delete(pathCguid, true)
    saveDFToFiles(df = dfCguid, outputPath = pathCguid.toString, compressFormat = null,
      outputFormat = "csv", delimiter = "tab")

    val pathCguidInLocation = new Path(workDir + "/join/nullCguidInLocation/", (new Path(params.srcPath).getName))
    fs.delete(pathCguidInLocation, true)
    saveDFToFiles(df = dfCguidInLocation, outputPath = pathCguidInLocation.toString, compressFormat = null,
      outputFormat = "csv", delimiter = "tab")

//    var dfGuid = dfNullGuid.join(df2, $"new_uri" === $"rover_url", "inner")
//      .select("request_headers", "response_headers", "new_uri", "guid_txt")
//      .repartition(1)
//    val joinNullGuid = dfGuid.count()
//
//    val pathGuid = new Path(workDir + "/join/nullGuid/", (new Path(params.srcPath).getName))
//    fs.delete(pathGuid, true)
//    saveDFToFiles(df = dfGuid, outputPath = pathGuid.toString, compressFormat = null,
//      outputFormat = "csv", delimiter = "tab")

    // 4. Write out result to file on hdfs
    var outputStream: FSDataOutputStream = null
    try {
      outputStream = fs.create(new Path(params.outputPath))
      outputStream.writeChars(s"Null cguid: $countNullCguid \n")
      outputStream.writeChars(s"Null cguid after join: $joinNullCguid \n")
      outputStream.writeChars(s"Valid Null cguid after join: $joinValidNullCguid \n")
      outputStream.writeChars(s"Null cguid in Location: $nullCguidInLocation \n")
      outputStream.writeChars(s"Null cguid nowhere: $nullCguidNowhere \n")
//      outputStream.writeChars(s"Null guid: $countNullGuid \n")
//      outputStream.writeChars(s"Null guid after join: $joinNullGuid \n")

      outputStream.flush()
    } finally {
      if (outputStream != null) {
        outputStream.close()
      }
    }
  }

  /**
    * filter out dashenId, dashenCnt
    */
  def removeParams(url: String): String = {
    val splitter = url.split("&").filter(item => !item.startsWith("dashenId") && !item.startsWith("dashenCnt"))
    var result = splitter.mkString("&")
    if (result.startsWith("http://")) {
      result = result.substring(7)
    } else if (result.startsWith("https://")) {
      result = result.substring(8)
    }

    result
  }

  def containsDashenId(url: String): String = {
    if (url.contains("dashenId")) {
      "TRUE"
    } else {
      "FALSE"
    }
  }

  def dashenCntAbove1(url: String): String = {
    var result = "FALSE"
    if (url.contains("dashenCnt")) {
      val p: Pattern = Pattern.compile("dashenCnt(=|%3D)[0-9]")
      val m: Matcher = p.matcher(url)
      if (m.find) {
        val urlWithCnt = m.group
        val dashenCnt: Int = Integer.valueOf(urlWithCnt.substring(urlWithCnt.length - 1))
        if (dashenCnt >= 1) {
          result = "TRUE"
        }
      }
    }

    result
  }

  // should remove raptor=1 from rover URL
  def normalizeUrl(url: String): String = {
    var result = url.replace("raptor=1", "")
    val lastChar = result.charAt(result.length - 1)
    if (lastChar == '&' || lastChar == '?') {
      result = result.substring(0, result.length - 1)
    }
    if (result.startsWith("http://")) {
      result = result.substring(7)
    } else if (result.startsWith("https://")) {
      result = result.substring(8)
    }
    result
  }

  def verifyByBit(bit: Int, chocolate_rule_flag: Long, epn_rule_flag: String): Boolean = {
    val mask = chocolate_rule_flag & 1L << bit
    if (StringUtils.isNotEmpty(epn_rule_flag) && epn_rule_flag.equals("1")) {
      mask != 0
    } else {
      mask == 0
    }
  }

  def verifyByBit(bit: Int, chocolate_rule_flag: Long, epn_rule_flag1: String, epn_rule_flag2: String): Boolean = {
    val mask = chocolate_rule_flag & 1L << bit
    if ((StringUtils.isNotEmpty(epn_rule_flag1) && epn_rule_flag1.equals("1")) ||
      StringUtils.isNotEmpty(epn_rule_flag2) && epn_rule_flag2.equals("1")) {
      mask != 0
    } else {
      mask == 0
    }
  }

  // parse CGUID from response_headers' Location param
  def cguidInLocation(): Column = {
    split($"response_headers", "cguid=")(1).substr(0, 32).alias("cguidInLocation")
  }
}
