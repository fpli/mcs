/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.basenrt

import java.util.Properties

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.sql.DataFrame



/**
  * Created by xiangli4 on 08/17/20.
  */
abstract class BaseNrtJob(override val jobName: String,
                               override val mode: String = "yarn") extends BaseSparkJob(jobName, mode) {

  /**
    * The hadoop conf
    */
  @transient lazy val hadoopConf: Configuration = {
    new Configuration()
  }

  /**
    * The file system
    */
  @transient lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }
  
  lazy val DATE_COL = "date"

  /**
    * Rename temp output files to output dir
    * @param outputDir final destination
    * @param sparkDir temp result dir
    * @param date current handled date
    * @return files array handled
    */
  def renameFiles(outputDir: String, sparkDir: String, date: String): Array[String] = {
    // rename result to output dir
    val dateOutputPath = new Path(outputDir + "/" + date)
    var max = -1
    if (fs.exists(dateOutputPath)) {
      val outputStatus = fs.listStatus(dateOutputPath)
      if (outputStatus.nonEmpty) {
        max = outputStatus.map(status => {
          val name = status.getPath.getName
          Integer.valueOf(name.substring(5, name.indexOf(".")))
        }).maxBy(i => i)
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
    files
  }

  def archiveMetafile(metafile: String, archiveDir: String): Unit = {
    val outputPath = new Path(archiveDir)
    if (!fs.exists(outputPath)) {
      fs.mkdirs(outputPath)
    }

    val src = new Path(metafile)
    val target = new Path(archiveDir, src.getName)
    fs.rename(src, target)
  }



}
