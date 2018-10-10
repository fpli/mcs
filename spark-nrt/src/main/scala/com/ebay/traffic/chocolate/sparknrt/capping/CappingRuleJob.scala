package com.ebay.traffic.chocolate.sparknrt.capping

import java.text.SimpleDateFormat
import java.util.Properties

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.commons.lang3.StringUtils

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
  lazy val outputDir = params.outputDir + "/" + params.channel + "/capping/"
  lazy val archiveDir = params.archiveDir + "/" + params.channel + "/capping/"

  override def run(): Unit = {

    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("capping_rule.properties"))
    val suffix = properties.getProperty("meta.output.suffix")
    var suffixArray: Array[String] = Array()
    if (StringUtils.isNotEmpty(suffix)) {
      suffixArray = suffix.split(",")
    }

    val dedupeOutputMeta = inputMetadata.readDedupeOutputMeta()

    if(dedupeOutputMeta.length > 0) {
      val file = dedupeOutputMeta(0)._1
      val datesFiles = dedupeOutputMeta(0)._2
      // apply capping rules
      val datesArray = datesFiles.keys.toArray
      // Be very careful here! DateFiles constructor takes 'date=yyy-MM-dd' as the key
      val metaFiles = new MetaFiles(datesArray.map(
        date => capping(date, datesFiles.get(date).get,
          new CappingRuleContainer(params, new DateFiles(date, datesFiles.get(date).get), this)))
      )
      outputMetadata.writeDedupeOutputMeta(metaFiles, suffixArray)
      // archive for future replay
      archiveMetafile(file, archiveDir)
    }
  }

  /**
    * capping logic
    *
    * @param date                 date string includes date col =: date=yyy-MM-dd
    * @param input                input file paths
    * @param cappingRuleContainer container of capping rules
    * @return DateFiles
    */
  def capping(date: String, input: Array[String], cappingRuleContainer: CappingRuleContainer): DateFiles = {
    // clean base dir
    cappingRuleContainer.preTest()

    // run every capping rule
    var df = cappingRuleContainer.test(params)

    // save result to spark dir
    df = df.repartition(params.partitions)
    saveDFToFiles(df, sparkDir)

    // rename result to output dir
    val files = renameFiles(outputDir, sparkDir, date)

    // rename base temp files
    cappingRuleContainer.postTest()
    new DateFiles(date, files)
  }
}
