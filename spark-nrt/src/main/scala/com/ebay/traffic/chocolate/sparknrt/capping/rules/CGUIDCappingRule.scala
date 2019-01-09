package com.ebay.traffic.chocolate.sparknrt.capping.rules

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.Parameter
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
  * Created by jialili1 on 5/28/18.
  */
class CGUIDCappingRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String)
    extends GenericCountRule(params: Parameter, bit: Long, dateFiles: DateFiles,  cappingRuleJobObj: BaseSparkJob, window: String) {

  import cappingRuleJobObj.spark.implicits._

  //workdir
  override lazy val fileName = "/cguid_" + window + "/"
  //specific columns for CGUID Capping Rule
  override val cols = Array(col("CGUID"))

  //filter condition for counting df
  def filterCondition(): Column = {
    $"channel_action" === "CLICK"
  }

  // parse CGUID from response_headers, if null, parse from request_headers
  def cguid(cguidColumn: String): Column = {
    if (cguidColumn != null) {
      col(cguidColumn).alias("CGUID")
    } else {
      val CGUID_Response = split($"response_headers", "cguid/")(1).substr(0, 32)
      val CGUID_Request = split($"request_headers", "cguid/")(1).substr(0, 32)
      when(CGUID_Response.isNotNull, CGUID_Response).otherwise(CGUID_Request).alias("CGUID")
    }
  }

  //counting columns
  def selectCondition(cguidColumn: String): Array[Column] = {
    Array(cguid(cguidColumn))
  }

  //add new column if needed
  def withColumnCondition(cguidColumn: String): Column = {
    when($"channel_action" === "CLICK", cguid(cguidColumn)).otherwise("NA")
  }

  //final join condition
  def joinCondition(df: DataFrame, dfCount: DataFrame): Column = {
    df.col(cols(0).toString()) === dfCount.col(cols(0).toString())
  }

  override def test(): DataFrame = {

    //Step 1: Prepare counting data. If this job has no events, return snapshot_id and capping = 0.
    //filter click only
    var dfCGUID = dfFilterInJob(filterCondition())

    //if job has no events, then return df with capping column directly
    val head = dfCGUID.take(1)
    if (head.length == 0) {
      dfNoEvents()
    }
    else {
      val firstRow = head(0)
      val timestamp = dfCGUID.select($"timestamp").first().getLong(0)

      val cguidColumn = if (dfCGUID.columns.contains("cguid")) "cguid" else null

      //Step 2: Count by CGUID in this job, then integrate data to 1 file, and add timestamp to file name.
      //count by CGUID and in the job
      dfCGUID = dfLoadCappingInJob(dfCGUID, selectCondition(cguidColumn))

      //reduce the number of counting file to 1, and rename file name to include timestamp
      saveCappingInJob(dfCGUID, timestamp)

      //Step 3: Read a new df for join purpose, just select CGUID and snapshot_id, and read previous data for counting purpose.
      //df for join
      val selectCols: Array[Column] = $"snapshot_id" +: cols
      var df = dfForJoin(cols(0), withColumnCondition(cguidColumn), selectCols)

      //read previous data and add to count path
      val cguidCountPath = getCappingDataPath(timestamp)

      //Step 4: Count all data, including previous data and data in this job, then join the result with the new df, return only snapshot_id and capping.
      //count through whole timeWindow and filter those over threshold
      dfCGUID = dfCappingInJob(null, cguidCountPath)

      //join origin df and counting df
      df = dfJoin(df, dfCGUID, joinCondition(df, dfCGUID))
      df
    }
  }
}
