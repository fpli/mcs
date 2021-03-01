package com.ebay.traffic.chocolate.sparknrt.capping_v2.rules

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping_v2.Parameter_v2
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.functions.{count, lit, sum}
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Created by jialili1 on 5/11/18.
  */
abstract class GenericCountRule(params: Parameter_v2, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String)
  extends GenericRule(params: Parameter_v2, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String){

  lazy val threshold = properties.getProperty(ruleName).toInt

  import cappingRuleJobObj.spark.implicits._

  val cols: Array[Column]

  //count by specific columns in the job
  override def dfLoadCappingInJob(dfCapping: DataFrame, selectCols: Array[Column]): DataFrame = {
    dfCapping.select(selectCols: _*)
      .groupBy(cols: _*).agg(count(lit(1)).alias("count"))
  }

  //count through whole timeWindow and filter those over threshold
  override def dfCappingInJob(dfJoin: DataFrame, cappingPath: List[String]): DataFrame = {
    cappingRuleJobObj.readFilesAsDFEx(cappingPath.toArray)
      .groupBy(cols: _*)
      .agg(sum("count") as "amnt")
      .filter($"amnt" >= threshold)
      .withColumn("capping", lit(cappingBit))
      .drop("count")
      .drop("amnt")
  }
}
