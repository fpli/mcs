package com.ebay.traffic.chocolate.sparknrt.verifier0

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
  * Created by jialili1 on 11/23/18.
  */
object RuleVerifier0 {
  def main(args: Array[String]): Unit = {
    val params = Parameter(args)
    val job = new RuleVerifier0(params)
    job.run()
    job.stop()
  }
}

class RuleVerifier0(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode) {

  import spark.implicits._

  lazy val workDir = params.workPath + "/tmp/0/"

  val amsClickSchema: StructType = StructType(
    Seq(
      StructField("click_ts", StringType, nullable = true),
      StructField("click_id", StringType, nullable = true),
      StructField("crltn_guid_txt", StringType, nullable = true),
      StructField("guid_txt", StringType, nullable = true),
      StructField("user_id", StringType, nullable = true),
      StructField("clnt_rmt_ip", StringType, nullable = true),
      StructField("pblshr_id", StringType, nullable = true),
      StructField("rover_url_txt", StringType, nullable = true),
      StructField("rt_rule_flag1", StringType, nullable = true),
      StructField("rt_rule_flag2", StringType, nullable = true),
      StructField("rt_rule_flag3", StringType, nullable = true),
      StructField("rt_rule_flag4", StringType, nullable = true),
      StructField("rt_rule_flag5", StringType, nullable = true),
      StructField("rt_rule_flag6", StringType, nullable = true),
      StructField("rt_rule_flag7", StringType, nullable = true),
      StructField("rt_rule_flag8", StringType, nullable = true),
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

  var chocoSchema: StructType = null

  def setChocoSchema(schema: StructType) = {
    chocoSchema = schema
  }

  def verifyChocoQuick(df: DataFrame, outputPath: String) = {

    val today = new Path(params.chocoTodayPath).getName

    countNullChoco(df, outputPath + "/quick/null/cguid/" + today, $"CGUID")
    countNullChoco(df, outputPath + "/quick/null/guid/" + today, $"guid")

//    groupyByCount(df, outputPath + "/quick/cguid/" + today, $"CGUID")
//    groupyByCount(df, outputPath + "/quick/cguidpub/" + today, $"CGUID", $"publisher_id")
//    groupyByCount(df, outputPath + "/quick/ip/" + today, $"IP")
//    groupyByCount(df, outputPath + "/quick/ippub/" + today, $"IP", $"publisher_id")
  }

  def countNullChoco(df: DataFrame, output: String, col: Column) = {
    val dfNull = df.select(col, $"request_headers", $"response_headers")
      .where(col === "")
      .repartition(1)

    fs.delete(new Path(output), true)
    saveDFToFiles(df = dfNull, outputPath = output, compressFormat = null,
      outputFormat = "csv", delimiter = "tab")
  }

  def groupyByCount(df: DataFrame, output: String, cols: Column*) = {
    val dfCount = df.select(cols: _*)
      .groupBy(cols: _*)
      .count()
      .orderBy($"count".desc)
      .limit(1000)
      .repartition(1)

    fs.delete(new Path(output), true)
    saveDFToFiles(df = dfCount, outputPath = output, compressFormat = null,
      outputFormat = "csv", delimiter = "space")
  }

  def verifyChocoDetails(dfToday: DataFrame, countToday: Long, outputPath: String) = {

    val today = new Path(params.chocoTodayPath).getName

    // Load yesterday's chocolate data
    logger.info("load yesterday's data for chocoPath: " + params.chocoYesterdayPath)

    val dfYesterday = readFilesAsDF(inputPath = params.chocoYesterdayPath, schema = chocoSchema,
      inputFormat = params.chocoInputFormat, delimiter = params.chocoInputDelimiter)
      .where($"channel_action" === "CLICK" and $"channel_type" === "EPN")
      .withColumn("today", lit(0))
      .select($"snapshot_id", $"timestamp", $"publisher_id", $"guid", cguid(), ip(), $"today", $"rt_rule_flags", $"nrt_rule_flags")

    val path = new Path(workDir + "/chocolate/", today)
    fs.delete(path, true)
    saveDFToFiles(dfToday.union(dfYesterday), path.toString)
    val df = readFilesAsDF(path.toString)
    //    val count = df.count()
    //
    //    println("number of records in choco: " + count)
    //    df.show(count.toInt)

    // collect fraud cguid count
    val dfCguidCount = dfToday.select($"CGUID")
      .groupBy($"CGUID")
      .count()
      .filter($"count" > 5000)

    val fraudCount = dfCguidCount.filter($"CGUID".isNotNull).agg(sum("count")).first.get(0).asInstanceOf[Long]

    val excludesCguids = dfCguidCount.select($"CGUID")
      .filter($"CGUID".isNotNull).collect().map(row => row(0).asInstanceOf[String]).toSet

    val excludesCguidUdf = udf(excludeCGUID(_: String, excludesCguids))

    // CGUID long rules
    val cguidLongThreshold = 120
    val cguidLongWindow = 86400000

    val rddCguid =
      df.select($"snapshot_id", $"timestamp", $"CGUID", $"today")
        .filter($"CGUID".isNotNull) // filter all null CGUID
        .filter(excludesCguidUdf($"CGUID") =!= true)
        .rdd
        .map(row => (row(2).asInstanceOf[String], (row(0).asInstanceOf[Long], row(1).asInstanceOf[Long], row(3).asInstanceOf[Int])))
        .groupByKey(1000)
        .flatMap(e => {
          val sortedEvents = e._2.toList.sortBy(v => v._2) // click events for "CGUID", sorted by timestamp
          var events = new ListBuffer[Long]()
          var i = 0
          for (i <- 0 until sortedEvents.length) {
            if (sortedEvents(i)._3 == 1) { // today
            var j = 0
              while (j < i && (sortedEvents(i)._2 - sortedEvents(j)._2 > cguidLongWindow)) {
                j += 1
              }
              if (i - j + 1 >= cguidLongThreshold) {
                events += sortedEvents(i)._1
              }
            }
          }
          events.iterator
        })
        .map(id => Row(id))
    var dfCguid = spark.createDataFrame(rddCguid, schema)

    val pathCguid = new Path(workDir + "/chocolate/cguid/", today)
    fs.delete(pathCguid, true)
    saveDFToFiles(dfCguid, pathCguid.toString)
    dfCguid = readFilesAsDF(pathCguid.toString)

    val countCguidLCorrect = dfCguid.count()

    println("number of records - countCguidLCorrect: " + countCguidLCorrect)

    val countCguidL = dfToday.where($"nrt_rule_flags".bitwiseAND(64) =!= 0).count()

    println("number of records - countCguidL: " + countCguidL)

    val countCguidJoin = dfCguid.join(dfToday.where($"nrt_rule_flags".bitwiseAND(64) =!= 0).select($"snapshot_id"),
      $"snapshot_id" === $"snapshot_id_1", "inner")
      .count()

    println("number of records - countCguidJoin: " + countCguidJoin)


    var outputStream: FSDataOutputStream = null
    try {
      outputStream = fs.create(new Path(params.outputPath, today))
      outputStream.writeChars(s"Chocolate Total - Nodeupe: $countToday \n")

      outputStream.writeChars("-----------------------CGUID Long---------------------------------" + "\n")
      outputStream.writeChars("CGuid Long: " + countCguidL + "\n")
      outputStream.writeChars("CGuid Long - Correct: " + (countCguidLCorrect + fraudCount) + "\n")
      outputStream.writeChars("CGuid Long join: " + countCguidJoin + "\n")
      outputStream.writeChars("CGuid Long fraud: " + fraudCount + "\n")
      outputStream.writeChars("CGuid Long inconsistent: " +
        (countCguidL - fraudCount - countCguidJoin + countCguidLCorrect - countCguidJoin).toFloat/countToday + "\n")

      outputStream.flush()
    } finally {
      if (outputStream != null) {
        outputStream.close()
      }
    }

  }

  def verifyChocolate() = {

    // Load today's chocolate data
    logger.info("load today's data for chocoPath: " + params.chocoTodayPath)

    var dfToday = readFilesAsDF(inputPath = params.chocoTodayPath, schema = chocoSchema,
      inputFormat = params.chocoInputFormat, delimiter = params.chocoInputDelimiter)
      .where($"channel_action" === "CLICK" and $"channel_type" === "EPN")
      .withColumn("today", lit(1))
      .select($"snapshot_id", $"timestamp", $"publisher_id", $"guid", cguid(), ip(), $"request_headers", $"response_headers", $"today", $"rt_rule_flags", $"nrt_rule_flags")

    val pathToday = new Path(workDir + "/chocolate/today/", (new Path(params.chocoTodayPath).getName))
    fs.delete(pathToday, true)
    saveDFToFiles(dfToday, pathToday.toString)
    dfToday = readFilesAsDF(pathToday.toString)
    val countToday = dfToday.count()

    println("number of records in df Today: " + countToday)

    if (params.chocoQuickCheck) {
      verifyChocoQuick(dfToday, params.outputPath + "/chocolate/" )
    } else {
      verifyChocoDetails(dfToday, countToday, params.outputPath + "/chocolate/")
    }
  }

  def countNullEpn(df: DataFrame, output: String, col: Column) = {
    val dfNull = df.select(col)
      .where(col === "")
      .repartition(1)

    fs.delete(new Path(output), true)
    saveDFToFiles(df = dfNull, outputPath = output, compressFormat = null,
      outputFormat = "csv", delimiter = "tab")
  }

  def verifyEpnQuick(df: DataFrame, outputPath: String) = {

    val today = new Path(params.epnTodayPath).getName

    countNullChoco(df, outputPath + "/quick/null/cguid/" + today, $"crltn_guid_txt")
    countNullChoco(df, outputPath + "/quick/null/guid/" + today, $"guid_txt")

//    groupyByCount(df, outputPath + "/quick/cguid/" + today, $"crltn_guid_txt")
//    groupyByCount(df, outputPath + "/quick/cguidpub/" + today, $"crltn_guid_txt", $"pblshr_id")
//    groupyByCount(df, outputPath + "/quick/ip/" + today, $"clnt_rmt_ip")
//    groupyByCount(df, outputPath + "/quick/ippub/" + today, $"clnt_rmt_ip", $"pblshr_id")

  }

  def verifyEpnDetails(df: DataFrame, outputPath: String) = {

  }

  def verifyEpn() = {

    // Load today's epn data
    logger.info("load today's data for epnPath: " + params.epnTodayPath)

    val dfToday = readFilesAsDF(inputPath = params.epnTodayPath,
      inputFormat = params.epnInputFormat, schema = amsClickSchema, delimiter = params.epnInputDelimiter)
      .withColumn("today", lit(1))

    if (params.epnQuickCheck) {
      verifyEpnQuick(dfToday, params.outputPath + "/epn/")
    } else {
      verifyEpnDetails(dfToday, params.outputPath + "/epn/")
    }

  }

  override def run()= {

    if (params.chocoTodayPath != null && !params.chocoTodayPath.isEmpty) {
      verifyChocolate()
    }
    if (params.epnTodayPath != null && !params.epnTodayPath.isEmpty) {
      verifyEpn()
    }

  }

  val schema: StructType = StructType(
    Seq(
      StructField("snapshot_id_1", LongType, nullable = true)
    )
  )

  //parse CGUID from request_headers
  def cguid(): Column = {
    $"cguid".alias("CGUID")
  }

  //parse IP from request_headers
  def ip(): Column = {
    $"remote_ip".alias("IP")
  }

  def excludeCGUID(cguid: String, excludes: Set[String]): Boolean = {
    if (excludes.contains(cguid)) {
      true
    } else {
      false
    }
  }

  /**
    * filter out dashenId, dashenCnt
    */
  def removeParams(url: String): String = {
    val splitter = url.split("&").filter(item => !item.startsWith("dashenId") && !item.startsWith("dashenCnt"))
    splitter.mkString("&")
  }
}