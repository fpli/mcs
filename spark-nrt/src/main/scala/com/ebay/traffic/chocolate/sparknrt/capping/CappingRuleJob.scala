package com.ebay.traffic.chocolate.sparknrt.capping

import java.text.SimpleDateFormat

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}

/**
  * Created by xiangli4 on 3/30/18.
  */
object CappingRuleJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new CappingRuleJob(params)

    job.run()
    job.stop()
  }
}

class CappingRuleJob(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  @transient lazy val inputMetadata = {
    Metadata(params.workDir, params.channel, MetadataEnum.dedupe)
  }

  @transient lazy val outputMetadata = {
    Metadata(params.workDir, params.channel, MetadataEnum.capping)
  }

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")

  lazy val baseDir = params.workDir + "/capping/" + params.channel + "/"
  lazy val sparkDir = baseDir + "/spark/"
  lazy val outputDir = params.outputDir

  override def run(): Unit = {

    val dedupeOutputMeta = inputMetadata.readDedupeOutputMeta()

    if(dedupeOutputMeta.length > 0) {
      val file = dedupeOutputMeta(0)._1
      val datesFiles = dedupeOutputMeta(0)._2
      // apply capping rules
      val datesArray = datesFiles.keys.toArray
      val metaFiles = new MetaFiles(datesArray.map(
        date => capping(date, datesFiles.get(date).get,
          new CappingRuleContainer(params, new DateFiles(date, datesFiles.get(date).get), this)))
      )
      outputMetadata.writeDedupeOutputMeta(metaFiles)
      inputMetadata.deleteDedupeOutputMeta(file)
    }
  }

  /**
    * capping logic
    *
    * @param date                 date
    * @param input                input file paths
    * @param cappingRuleContainer container of capping rules
    * @return DateFiles
    */
  def capping(date: String, input: Array[String], cappingRuleContainer: CappingRuleContainer): DateFiles = {
    // clean base dir
    cappingRuleContainer.cleanBaseDir()
    val dateFiles = new DateFiles(date, input)
    // run every capping rule
    var df = cappingRuleContainer.test(params, dateFiles)

    // save result to spark dir
    df = df.repartition(params.partitions)
    saveDFToFiles(df, sparkDir)

    // rename result to output dir
    val files = renameFiles(outputDir, sparkDir, date)

    // rename base temp files
    cappingRuleContainer.renameBaseTempFiles(dateFiles)
    new DateFiles(date, files)
  }
}
