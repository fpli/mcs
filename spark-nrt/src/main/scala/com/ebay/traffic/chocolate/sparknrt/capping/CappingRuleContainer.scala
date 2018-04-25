package com.ebay.traffic.chocolate.sparknrt.capping

import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.traffic.chocolate.sparknrt.capping.rules.{IPCappingRule}
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.DataFrame
import scala.collection.mutable

/**
  * Created by xiangli4 on 4/8/18.
  */
class CappingRuleContainer(params: Parameter, dateFiles: DateFiles, sparkJobObj: CappingRuleJob) {

  @transient lazy val channelsRules = mutable.HashMap(
    ChannelType.EPN -> mutable.HashMap(
      CappingRuleEnum.IPCappingRule ->
      new IPCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule), dateFiles, sparkJobObj)
    ),
    ChannelType.DISPLAY -> mutable.HashMap(
    )
  )

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
    while (dfIter.hasNext) {
      val rightDf = dfIter.next().withColumnRenamed("snapshot_id", "snapshot_id_right")
        .withColumnRenamed("capping", "capping_1")
        .select($"snapshot_id_right", $"capping_1")

      df = df.join(rightDf, $"snapshot_id" === $"snapshot_id_right", "right_outer")
        .drop($"snapshot_id_right")
        .withColumn("nrt_rule_flags", coalesce($"nrt_rule_flags", lit(0l)).bitwiseOR(coalesce($"capping_1", lit(0l))))
        .drop($"capping_1")
    }
    df
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
