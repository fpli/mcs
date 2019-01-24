package com.ebay.traffic.chocolate.sparknrt.monitoring

import com.ebay.traffic.monitoring.{ESMetrics, Field, Metrics}
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.capping.CappingRuleEnum
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.spark.sql.DataFrame

/**
  * Created by jialili1 on 11/14/18.
  */
object MonitoringJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)
    val job = new MonitoringJob(params)

    job.run()
    job.stop()
  }
}
class MonitoringJob(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  lazy val METRICS_INDEX_PREFIX = "chocolate-metrics-"
  lazy val batchSize = 10

  @transient lazy val metadata = {
    Metadata(params.workDir, params.channel, MetadataEnum.capping)
  }

  @transient lazy val metrics: Metrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESMetrics.init(METRICS_INDEX_PREFIX, params.elasticsearchUrl)
      ESMetrics.getInstance()
    } else null
  }

  import spark.implicits._

  override def run(): Unit = {
    var cappingResult = metadata.readDedupeOutputMeta(".monitoring")

    if (cappingResult.length > batchSize) {
      cappingResult = cappingResult.slice(0, batchSize)
    }

    if(cappingResult.length > 0) {
      cappingResult.foreach(metaIter => {
        val file = metaIter._1
        val datesFiles = metaIter._2

        datesFiles.foreach(datesFile => {
          val date = getDate(datesFile._1)
          val df = readFilesAsDFEx(datesFile._2)
          logger.info("Load DataFrame: date=" + date + ", files=" + datesFile._2.mkString(","))

          val dfMetrics = df.filter($"nrt_rule_flags" =!= 0)
          val head = dfMetrics.take(1)
          if (head.length == 0) {
            logger.info("No need to count capping result")
          } else {
            val firstEvent = head(0)
            val eventTime = firstEvent.getLong(firstEvent.fieldIndex("timestamp"))

            logger.info("Start counting...")
            val capping = dfMetrics.count()
            val fieldClick = Field.of[String, AnyRef]("channelAction", "CLICK")
            val fieldEpn = Field.of[String, AnyRef]("channelType", "EPN")
            val fieldDisplay = Field.of[String, AnyRef]("channelType", "DISPLAY")

            if (metrics != null) {
              metrics.meter("CappingCount", capping, eventTime)

              //EPN nrt rules
              metrics.meter("IPPubShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPPubCappingRule_S,
                "CLICK", "EPN"), eventTime, fieldClick, fieldEpn)
              metrics.meter("IPPubLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPPubCappingRule_L,
                "CLICK", "EPN"), eventTime, fieldClick, fieldEpn)
              metrics.meter("CGUIDShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDCappingRule_S,
                "CLICK", "EPN"), eventTime, fieldClick, fieldEpn)
              metrics.meter("CGUIDLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDCappingRule_L,
                "CLICK", "EPN"), eventTime, fieldClick, fieldEpn)
              metrics.meter("CGUIDPubShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDPubCappingRule_S,
                "CLICK", "EPN"), eventTime, fieldClick, fieldEpn)
              metrics.meter("CGUIDPubLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDPubCappingRule_L,
                "CLICK", "EPN"), eventTime, fieldClick, fieldEpn)
              metrics.meter("SnidShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_S,
                "CLICK", "EPN"), eventTime, fieldClick, fieldEpn)
              metrics.meter("SnidLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_L,
                "CLICK", "EPN"), eventTime, fieldClick, fieldEpn)

              //Display nrt rules
              metrics.meter("IPCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPCappingRule,
                "CLICK", "DISPLAY"), eventTime, fieldClick, fieldDisplay)
              metrics.meter("SnidShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_S,
                "CLICK", "DISPLAY"), eventTime, fieldClick, fieldDisplay)
              metrics.meter("SnidLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_L,
                "CLICK", "DISPLAY"), eventTime, fieldClick, fieldDisplay)
              metrics.flush()
            }
          }
          metadata.deleteDedupeOutputMeta(file)
        })
      })
    }
  }

  def getDate(date: String): String = {
    val splitted = date.split("=")
    if (splitted != null && splitted.nonEmpty) splitted(1)
    else throw new Exception("Invalid date field in metafile.")
  }

  def CappingCount(df: DataFrame, value: CappingRuleEnum.Value, channelAction: String, channelType: String): Long = {
    df.filter($"channel_action" === channelAction and $"channel_type" === channelType)
      .filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(value)).=!=(0)).count()
  }


}
