package com.ebay.traffic.chocolate.spark

import com.ebay.traffic.chocolate.pojo.BasePojo
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.Builder
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}


/**
  * Created by zhofan on 2019/06/11.
  */
class SparkJobContext(local: Int, hiveEnabled: Boolean, defaultFS: String) {
  private lazy val logger = LoggerFactory.getLogger(classOf[SparkJobContext])

  private val sparkConfig = new SparkConf()
  sparkConfig.registerKryoClasses(Array(classOf[BasePojo]))

  if (local > 0) {
    sparkConfig.setMaster(s"local[$local]")
    logger.info("run spark using local mode")
  }

  val sparkSession = SparkSession.builder()
    .config(sparkConfig)
    .tryEnableHiveSupport(hiveEnabled)
    .getOrCreate()

  private val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
  if(defaultFS == null || defaultFS.isEmpty){
    hadoopConf.set("fs.defaultFS", SparkCommonConf.defaultFS)
  }else{
    hadoopConf.set("fs.defaultFS", defaultFS)
  }

  val fileSystem = FileSystem.get(hadoopConf)

  implicit class BuilderWrapper(builder: Builder) {

    private val logger = LoggerFactory.getLogger(classOf[BuilderWrapper])

    def tryEnableHiveSupport(boolean: Boolean): Builder = {
      if (boolean) {
        Try {
          builder.enableHiveSupport()
        } match {
          case Success(value) =>
            value
          case Failure(ex) =>
            logger.error(ex.toString)
            builder
        }
      } else builder
    }
  }
}

object SparkJobContext {
  def apply(local: Int, hiveEnabled: Boolean, defaultFS: String): SparkJobContext = new SparkJobContext(local, hiveEnabled, defaultFS)
}
