package com.ebay.traffic.chocolate.sparknrt.capping

import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.traffic.chocolate.monitoring.ESMetrics
import com.ebay.traffic.chocolate.sparknrt.capping.rules._
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by xiangli4 on 4/8/18.
  */
class CappingRuleContainer(params: Parameter, dateFiles: DateFiles, sparkJobObj: CappingRuleJob) {

  lazy val windowLong = "long"
  lazy val windowShort = "short"
  lazy val METRICS_INDEX_PREFIX = "chocolate-metrics-";

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  @transient lazy val channelsRules = mutable.HashMap(
    ChannelType.EPN -> mutable.HashMap(
      CappingRuleEnum.IPCappingRule ->
          new IPCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule), dateFiles, sparkJobObj),
      CappingRuleEnum.IPPubCappingRule_S ->
          new IPPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.IPPubCappingRule_S), dateFiles, sparkJobObj, windowShort),
      CappingRuleEnum.IPPubCappingRule_L ->
          new IPPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.IPPubCappingRule_L), dateFiles, sparkJobObj, windowLong),
      CappingRuleEnum.CGUIDPubCappingRule_S ->
          new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_S), dateFiles, sparkJobObj, windowShort),
      CappingRuleEnum.CGUIDPubCappingRule_L ->
          new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_L), dateFiles, sparkJobObj, windowLong),
      CappingRuleEnum.CGUIDCappingRule_S ->
          new CGUIDCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDCappingRule_S), dateFiles, sparkJobObj, windowShort),
      CappingRuleEnum.CGUIDCappingRule_L ->
          new CGUIDCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDCappingRule_L), dateFiles, sparkJobObj, windowLong),
      // Snid capping rule is special. 2 rules are implemented in 1 single rule for better performance
      // Use SnidCappingRule_L as hashmap key. Actually it doesn't affect what it is
      CappingRuleEnum.SnidCappingRule_L ->
          new SNIDCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L),
            CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S), dateFiles, sparkJobObj, windowLong)
    ),
    ChannelType.DISPLAY -> mutable.HashMap(
    )
  )

  @transient lazy val metrics: ESMetrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESMetrics.init(METRICS_INDEX_PREFIX, params.elasticsearchUrl)
      ESMetrics.getInstance()
    } else null
  }

  def preTest() = {
    val channelRules = channelsRules.get(ChannelType.valueOf(params.channel)).iterator
    while (channelRules.hasNext) {
      val rules = channelRules.next().iterator
      while (rules.hasNext) {
        val rule = rules.next()._2
        rule.preTest()
      }
    }
  }

  import sparkJobObj.spark.implicits._

  def test(params: Parameter): DataFrame = {
    val channelRules = channelsRules.get(ChannelType.valueOf(params.channel)).iterator
    var dfs: List[DataFrame] = List()
    while (channelRules.hasNext) {
      val rules = channelRules.next().iterator
      while (rules.hasNext) {
        val rule = rules.next()._2
        val df = rule.test()
        dfs = dfs :+ df
      }
    }

    // join dfs
    var df: DataFrame = null
    val dfIter = dfs.iterator
    if (dfIter.hasNext) {
      df = dfIter.next().withColumn("nrt_rule_flags", coalesce($"capping", lit(0l))).drop($"capping")
    }

    //join all rules' result
    while (dfIter.hasNext) {
      val rightDf = dfIter.next().withColumnRenamed("snapshot_id", "snapshot_id_right")
          .withColumnRenamed("capping", "capping_1")
          .select($"snapshot_id_right", $"capping_1")

      df = df.join(rightDf, $"snapshot_id" === $"snapshot_id_right", "left_outer")
          .drop($"snapshot_id_right")
          .withColumn("nrt_rule_flags", coalesce($"nrt_rule_flags", lit(0l)).bitwiseOR(coalesce($"capping_1", lit(0l))))
          .drop($"capping_1")
    }

    //join with whole df
    df = df.withColumnRenamed("snapshot_id", "snapshot_id_tmp")
        .withColumnRenamed("nrt_rule_flags", "nrt_rule_flags_tmp")
        .select($"snapshot_id_tmp", $"nrt_rule_flags_tmp")

    var dfResult = sparkJobObj.readFilesAsDFEx(dateFiles.files)
    dfResult = dfResult.join(df, $"snapshot_id" === $"snapshot_id_tmp")
        .withColumn("nrt_rule_flags", $"nrt_rule_flags_tmp")
        .drop("snapshot_id_tmp")
        .drop("nrt_rule_flags_tmp")

    //metrics
    if (params.cappingMetrics) {
      val dfMetrics = dfResult.filter($"nrt_rule_flags" =!= 0)
      val capping = dfMetrics.count()
      if (metrics != null) {
        metrics.meter("CappingCount", capping, System.currentTimeMillis())
        metrics.meter("IPPubShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPPubCappingRule_S, "CLICK", "EPN"), System.currentTimeMillis(), "CLICK", "EPN")
        metrics.meter("IPPubShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPPubCappingRule_S, "CLICK", "DISPLAY"), System.currentTimeMillis(), "CLICK", "DISPLAY")
        metrics.meter("IPPubLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPPubCappingRule_L, "CLICK", "EPN"), System.currentTimeMillis(), "CLICK", "EPN")
        metrics.meter("IPPubLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPPubCappingRule_L, "CLICK", "DISPLAY"), System.currentTimeMillis(), "CLICK", "DISPLAY")
        metrics.meter("CGUIDShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDCappingRule_S, "CLICK", "EPN"), System.currentTimeMillis(), "CLICK", "EPN")
        metrics.meter("CGUIDShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDCappingRule_S, "CLICK", "DISPLAY"), System.currentTimeMillis(), "CLICK", "DISPLAY")
        metrics.meter("CGUIDLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDCappingRule_L, "CLICK", "EPN"), System.currentTimeMillis(), "CLICK", "EPN")
        metrics.meter("CGUIDLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDCappingRule_L, "CLICK", "DISPLAY"), System.currentTimeMillis(), "CLICK", "DISPLAY")
        metrics.meter("CGUIDPubShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDPubCappingRule_S, "CLICK", "EPN"), System.currentTimeMillis(), "CLICK", "EPN")
        metrics.meter("CGUIDPubShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDPubCappingRule_S, "CLICK", "DISPLAY"), System.currentTimeMillis(), "CLICK", "DISPLAY")
        metrics.meter("CGUIDPubLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDPubCappingRule_L, "CLICK", "EPN"), System.currentTimeMillis(), "CLICK", "EPN")
        metrics.meter("CGUIDPubLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDPubCappingRule_L, "CLICK", "DISPLAY"), System.currentTimeMillis(), "CLICK", "DISPLAY")
        metrics.meter("SnidShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_S, "CLICK", "EPN"), System.currentTimeMillis(), "CLICK", "EPN")
        metrics.meter("SnidShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_S, "CLICK", "DISPLAY"), System.currentTimeMillis(), "CLICK", "DISPLAY")
        metrics.meter("SnidLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_L, "CLICK", "EPN"), System.currentTimeMillis(), "CLICK", "EPN")
        metrics.meter("SnidLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_L, "CLICK", "DISPLAY"), System.currentTimeMillis(), "CLICK", "DISPLAY")
        metrics.flushMetrics()
      }
    }

    dfResult
  }

  def CappingCount(df: DataFrame, value: CappingRuleEnum.Value, channelAction: String, channelType: String): Long = {
    df.filter($"channel_action" === channelAction and $"channel_type" === channelType)
        .filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(value)).=!=(0)).count()
  }

  def postTest() = {
    val channelRules = channelsRules.get(ChannelType.valueOf(params.channel)).iterator
    var dfs: List[DataFrame] = List()
    while (channelRules.hasNext) {
      val rules = channelRules.next().iterator
      while (rules.hasNext) {
        val rule = rules.next()._2
        rule.postTest()
      }
    }
  }
}
