package com.ebay.traffic.chocolate.sparknrt.monitoring_v2

import java.util.Properties

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.capping.CappingRuleEnum
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import com.ebay.traffic.monitoring.Field
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics
import org.apache.spark.sql.DataFrame

/**
  * Created by yuhxiao on 22/06/21.
  */
object MonitoringJob_v2 extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter_v2(args)
    val job = new MonitoringJob_v2(params)

    job.run()
    job.stop()
  }
}

class MonitoringJob_v2(params: Parameter_v2)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  lazy val batchSize = 10

  @transient lazy val metadata = {
    Metadata(params.workDir, params.channel, MetadataEnum.capping)
  }

  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("sherlockio.properties"))
    properties
  }

    @transient lazy val metrics: SherlockioMetrics = {
    SherlockioMetrics.init(properties.getProperty("sherlockio.namespace"),properties.getProperty("sherlockio.endpoint"),properties.getProperty("sherlockio.user"))
    val sherlockioMetrics: SherlockioMetrics = SherlockioMetrics.getInstance()
    sherlockioMetrics.setJobName(params.appName)
    sherlockioMetrics
  }

  import spark.implicits._

  override def run(): Unit = {
    var cappingResult = metadata.readDedupeOutputMeta(".monitoring_v2")

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
            val fieldClick = Field.of[String, AnyRef]("channelAction", "CLICK")
            val fieldImp = Field.of[String, AnyRef]("channelAction", "IMPRESSION")
            val fieldEpn = Field.of[String, AnyRef]("channelType", "EPN")
            val fieldDisplay = Field.of[String, AnyRef]("channelType", "DISPLAY")

            if (metrics != null) {
              //Capping output total
              metrics.meter("CappingOutput", CappingTotalCount(df, "CLICK", "EPN"), fieldClick, fieldEpn)
              metrics.meter("CappingOutput", CappingTotalCount(df, "CLICK", "DISPLAY"), fieldClick, fieldDisplay)
              metrics.meter("CappingOutput", CappingTotalCount(df, "IMPRESSION", "EPN"), fieldImp, fieldEpn)
              metrics.meter("CappingOutput", CappingTotalCount(df, "IMPRESSION", "DISPLAY"), fieldImp, fieldDisplay)

              //Capping fail total
              metrics.meter("CappingCount", CappingTotalCount(dfMetrics, "CLICK", "EPN"), fieldClick, fieldEpn)
              metrics.meter("CappingCount", CappingTotalCount(dfMetrics, "CLICK", "DISPLAY"), fieldClick, fieldDisplay)

              //EPN nrt rules
              metrics.meter("IPLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPCappingRule,
                "CLICK", "EPN"), fieldClick, fieldEpn)
              metrics.meter("IPPubShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPPubCappingRule_S,
                "CLICK", "EPN"), fieldClick, fieldEpn)
              metrics.meter("IPPubLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPPubCappingRule_L,
                "CLICK", "EPN"), fieldClick, fieldEpn)
              metrics.meter("CGUIDShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDCappingRule_S,
                "CLICK", "EPN"), fieldClick, fieldEpn)
              metrics.meter("CGUIDLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDCappingRule_L,
                "CLICK", "EPN"), fieldClick, fieldEpn)
              metrics.meter("CGUIDPubShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDPubCappingRule_S,
                "CLICK", "EPN"), fieldClick, fieldEpn)
              metrics.meter("CGUIDPubLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.CGUIDPubCappingRule_L,
                "CLICK", "EPN"), fieldClick, fieldEpn)
              metrics.meter("IPBrowserShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPBrowserCappingRule_S,
                "CLICK", "EPN"), fieldClick, fieldEpn)
              metrics.meter("IPBrowserMediumCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPBrowserCappingRule_M,
                "CLICK", "EPN"), fieldClick, fieldEpn)
              metrics.meter("IPBrowserLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPBrowserCappingRule_L,
                "CLICK", "EPN"), fieldClick, fieldEpn)
//              metrics.meter("SnidShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_S,
//                "CLICK", "EPN"), fieldClick, fieldEpn)
//              metrics.meter("SnidLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_L,
//                "CLICK", "EPN"), fieldClick, fieldEpn)

              //Display nrt rules
              metrics.meter("IPLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.IPCappingRule,
                "CLICK", "DISPLAY"), fieldClick, fieldDisplay)
//              metrics.meter("SnidShortCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_S,
//                "CLICK", "DISPLAY"), fieldClick, fieldDisplay)
//              metrics.meter("SnidLongCappingCount", CappingCount(dfMetrics, CappingRuleEnum.SnidCappingRule_L,
//                "CLICK", "DISPLAY"), fieldClick, fieldDisplay)

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

  def CappingTotalCount(df: DataFrame, channelAction: String, channelType: String): Long = {
    df.filter($"channel_action" === channelAction and $"channel_type" === channelType).count()
  }

  def CappingCount(df: DataFrame, value: CappingRuleEnum.Value, channelAction: String, channelType: String): Long = {
    df.filter($"channel_action" === channelAction and $"channel_type" === channelType)
      .filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(value)).=!=(0)).count()
  }


}