package com.ebay.traffic.chocolate.sparknrt.capping_v2

import java.text.SimpleDateFormat
import java.util.Properties

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.commons.lang3.StringUtils

/**
 * Created by yuhxiao on 1/3/21.
 */
object CappingRuleJob_v2 extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter_v2(args)

    val job = new CappingRuleJob_v2(params)

    job.run()
    job.stop()
  }
}

class CappingRuleJob_v2(params: Parameter_v2)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  @transient lazy val inputMetadata = {
    Metadata(params.workDir, params.channel, MetadataEnum.dedupe)
  }

  @transient lazy val outputMetadata = {
    Metadata(params.workDir, params.channel, MetadataEnum.capping)
  }

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")

  lazy val cappingDir = "/capping/"
  lazy val baseDir = params.workDir + cappingDir + params.channel + "/"
  lazy val sparkDir = baseDir + "/spark/"
  lazy val outputDir = params.outputDir + "/" + params.channel + cappingDir
  lazy val archiveDir = params.archiveDir + "/" + params.channel + cappingDir

  override def run(): Unit = {

    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("capping_rule_v2.properties"))
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
          new CappingRuleContainer_v2(params, new DateFiles(date, datesFiles.get(date).get), this)))
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
  def capping(date: String, input: Array[String], cappingRuleContainer: CappingRuleContainer_v2): DateFiles = {
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
