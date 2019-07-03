package com.ebay.traffic.chocolate.spark

import java.io.{BufferedReader, InputStreamReader}
import java.text.SimpleDateFormat
import java.util

import com.ebay.traffic.chocolate.job.BaseJobOptions
import com.ebay.traffic.chocolate.utils.ConfigLoader
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory


/**
  * Created by zhofan on 2019/06/11.
  */
object RotationClientsDataMergeFileJob extends App {
  override def main(args: Array[String]) = {
    val baseJobOptions = BaseJobOptions(args)
    val argMap = baseJobOptions.argMap

    val jobParam = RotationDataParam.apply(argMap)
    val job = new RotationClientsDataMergeFileJob(jobParam)
    job.execute(jobParam.localThread)
  }
}

//merge td output partition into one output file
class RotationClientsDataMergeFileJob(jobParam: RotationDataParam) {
  final lazy val logger = LoggerFactory.getLogger(classOf[RotationClientsDataMergeFileJob])

  def execute(local: Int): Unit = {
    implicit val sparkJobContext = SparkJobContext(local, false, jobParam.defaultFs)
    implicit val sparkSession = sparkJobContext.sparkSession
    implicit val fs = sparkJobContext.fileSystem

    logger.info("Repartition number {}", jobParam.partitionNum)
    logger.info("Load source data from {}", jobParam.srcPath)
    logger.info("Export data to {}", jobParam.destPath)

    val destDir = new Path(jobParam.destPath)
    if (fs.exists(destDir)) {
      fs.delete(destDir, true)
    }

    val dataFields = getRotationTableFields(jobParam.ds)
    val fields = dataFields.split(",").map(fieldName => StructField(fieldName, StringType, nullable=true))
    val schema = StructType(fields)
    var dfWithSchema = sparkSession.read.format("com.databricks.spark.csv").option("header", "false").schema(schema).load(jobParam.srcPath)

    dfWithSchema
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("codec", "gzip")
      .save(jobParam.destPath)
  }

  def getRotationTableFields(ds: String): String = {
    val query = new StringBuilder
    val reader = new BufferedReader(new InputStreamReader(ConfigLoader.getClass.getResourceAsStream(s"/sql/spark/rotation_${ds}_fields.txt")))

    var line = reader.readLine()

    while (line != null) {
      query.append(line).append("\n")
      line = reader.readLine()
    }

    var queryStr = query.toString()
    queryStr = queryStr.replaceAll("\n", "")
    queryStr
  }
}
