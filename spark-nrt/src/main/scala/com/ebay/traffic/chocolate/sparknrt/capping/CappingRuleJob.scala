package com.ebay.traffic.chocolate.sparknrt.capping

import java.text.SimpleDateFormat
import java.util

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

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
  extends BaseSparkJob(params.appName, params.mode) {

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  @transient lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient lazy val inputMetadata = {
    Metadata(params.inputDir, params.channel)
  }

  @transient lazy val outputMetadata = {
    Metadata(params.workDir, params.channel)
  }

  lazy val DATE_COL = "date"

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")

  lazy val baseDir = params.workDir + "/capping/" + params.channel + "/"
  lazy val sparkDir = baseDir + "/spark/"
  lazy val outputDir = params.outputDir

  override def run(): Unit = {

    // manage output files from upstream
    val metaFiles = inputMetadata.readDedupeOutputMeta()
    val dedupeOutputMeta = inputMetadata.readDedupeOutputMeta()
    val dates = new util.HashSet[String]()
    val datesFilesMap = new util.HashMap[String, Array[String]]()
    if (dedupeOutputMeta != null) {
      val iteratorMeta = dedupeOutputMeta.iterator
      while (iteratorMeta.hasNext) {
        val metaFile = iteratorMeta.next()
        val iteratorOfDate = metaFile._2.iterator
        while (iteratorOfDate.hasNext) {
          val dateFilesEntry = iteratorOfDate.next()
          val date = dateFilesEntry._1
          dates.add(date)
          var input = dateFilesEntry._2
          if (datesFilesMap.containsKey(date)) {
            input = input.union(datesFilesMap.get(date))
            datesFilesMap.put(date, input)
          }
          else {
            datesFilesMap.put(date, input)
          }
        }
      }

      // apply capping rules
      val cappingRuleContainer = new CappingRuleContainer(params, spark)
      val datesArray = dates.toArray()
      val metaFiles = new MetaFiles(datesArray.map(date => capping(date.asInstanceOf[String], datesFilesMap.get(date), cappingRuleContainer)))
      outputMetadata.writeDedupeOutputMeta(metaFiles)
    }
  }

  /**
    * capping logic
    * @param date date
    * @param input input file paths
    * @param cappingRuleContainer container of capping rules
    * @return DateFiles
    */
  def capping(date: String, input: Array[String], cappingRuleContainer: CappingRuleContainer): DateFiles = {
    // clean base dir
    cappingRuleContainer.cleanBaseDir()
    val dateFiles = new DateFiles(date, input)
    // run every capping rule
    var df = cappingRuleContainer.test(params, dateFiles)
    df.show()

    // save result to spark dir
    df = df.repartition(params.partitions)
    saveDFToFiles(df, sparkDir)

    // rename result to output dir
    val dateOutputPath = new Path(outputDir + "/" + DATE_COL + "=" + date)
    var max = -1
    if (fs.exists(dateOutputPath)) {
      val outputStatus = fs.listStatus(dateOutputPath)
      if (outputStatus.length > 0) {
        max = outputStatus.map(status => {
          val name = status.getPath.getName
          Integer.valueOf(name.substring(5, name.indexOf(".")))
        }).sortBy(i => i).last
      }
    } else {
      fs.mkdirs(dateOutputPath)
    }

    val fileStatus = fs.listStatus(new Path(sparkDir))
    val files = fileStatus.filter(status => status.getPath.getName != "_SUCCESS")
      .zipWithIndex
      .map(swi => {
        val src = swi._1.getPath
        val seq = ("%5d" format max + 1 + swi._2).replace(" ", "0")
        val target = new Path(dateOutputPath, s"part-${seq}.snappy.parquet")
        logger.info("Rename from: " + src.toString + " to: " + target.toString)
        fs.rename(src, target)
        target.toString
      })
    // rename base temp files
    cappingRuleContainer.renameBaseTempFiles(dateFiles)
    new DateFiles(date, files)
  }
}
