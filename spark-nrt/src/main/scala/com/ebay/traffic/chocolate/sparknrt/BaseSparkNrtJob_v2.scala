package com.ebay.traffic.chocolate.sparknrt

import com.ebay.traffic.chocolate.spark.BaseSparkJob_v2
import org.apache.hadoop.fs.Path

/**
  * Created by xiangli4 on 4/11/18.
  */
abstract class BaseSparkNrtJob_v2(override val jobName: String,
                                  override val mode: String = "yarn") extends BaseSparkJob_v2(jobName, mode) {

  lazy val DATE_COL = "date"

  /**
    * Rename temp output files to output dir
    * @param outputDir final destination
    * @param sparkDir temp result dir
    * @param date current handled date
    * @return files array handled
    */
  def renameFiles(outputDir: String, sparkDir: String, date: String) = {
    // rename result to output dir
    val dateOutputPath = new Path(outputDir + "/" + date)
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
    files
  }

}
