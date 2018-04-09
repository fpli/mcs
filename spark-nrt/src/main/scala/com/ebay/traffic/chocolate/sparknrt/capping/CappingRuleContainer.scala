package com.ebay.traffic.chocolate.sparknrt.capping

import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.traffic.chocolate.sparknrt.capping.rules.{IPCappingRule, SnidCappingRule}
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable

/**
  * Created by xiangli4 on 4/8/18.
  */
class CappingRuleContainer(params: Parameter, sparkSession: SparkSession){

  @transient lazy val channelsRules = mutable.HashMap(
      ChannelType.EPN -> mutable.HashMap(
        "IPCappingRule" -> new IPCappingRule(params),
        "SnidCappingRule" -> new SnidCappingRule(params)
      ),
      ChannelType.DISPLAY -> mutable.HashMap(
      )
    )
  @transient lazy val spark = sparkSession

  def cleanBaseDir() = {
    val channelRules = channelsRules.get(ChannelType.valueOf(params.channel)).iterator
    while(channelRules.hasNext) {
      val rules = channelRules.next().iterator
      var df: DataFrame = null
      while(rules.hasNext) {
        val rule = rules.next()._2
        rule.cleanBaseDir()
      }
    }
  }

  import spark.implicits._

  def test(params: Parameter, dateFiles: DateFiles): DataFrame = {
    val channelRules = channelsRules.get(ChannelType.valueOf(params.channel)).iterator
    var dfs: List[DataFrame] = List()
    while(channelRules.hasNext) {
      val rules = channelRules.next().iterator
      while(rules.hasNext) {
        val rule = rules.next()._2
        val df = rule.test(dateFiles)
        dfs = dfs:+df
      }
    }

    var df: DataFrame = null
    val dfIter = dfs.iterator
    if(dfIter.hasNext) {
      df = dfIter.next()
    }
    while(dfIter.hasNext) {
      val rightDf = dfIter.next().withColumnRenamed("snapshot_id", "snapshot_id_right")
        .withColumnRenamed("filter_failed", "filter_failed_1")
        .select($"snapshot_id_right", $"filter_failed_1")
      df = df.join(rightDf, $"snapshot_id" === $"snapshot_id_right", "right_outer")
        .drop($"snapshot_id_right")
        .withColumn("filter_failed", updateFilterFailed($"filter_failed", $"filter_failed_1"))
        .drop($"filter_failed_1")
    }
    df
  }

  def renameBaseTempFiles(dateFiles: DateFiles) = {
    val channelRules = channelsRules.get(ChannelType.valueOf(params.channel)).iterator
    var dfs: List[DataFrame] = List()
    while(channelRules.hasNext) {
      val rules = channelRules.next().iterator
      while(rules.hasNext) {
        val rule = rules.next()._2
        rule.renameBaseTempFiles(dateFiles)
      }
    }

    var df: DataFrame = null
    val dfIter = dfs.iterator
    if(dfIter.hasNext) {
      df = dfIter.next()
    }
    while(dfIter.hasNext) {
      val rightDf = dfIter.next().withColumnRenamed("snapshot_id", "snapshot_id_right")
        .withColumnRenamed("filter_failed", "filter_failed_1")
        .select($"snapshot_id_right", $"filter_failed_1")
      df = df.join(rightDf, $"snapshot_id" === $"snapshot_id_right", "right_outer")
        .drop($"snapshot_id_right")
        .withColumn("filter_failed", updateFilterFailed($"filter_failed", $"filter_failed_1"))
        .drop($"filter_failed_1")
    }
  }

  val updateFilterFailed = udf[String, String, String]((filterFailed: String, filterFailed1: String) => {
    if (filterFailed == null || filterFailed.isEmpty) filterFailed1
    else filterFailed
  })
}
