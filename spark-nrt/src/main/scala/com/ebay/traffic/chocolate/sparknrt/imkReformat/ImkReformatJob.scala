package com.ebay.traffic.chocolate.sparknrt.imkReformat
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob

object ImkReformatJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)
    val job = new ImkReformatJob(params)

    job.run()
    job.stop()
  }
}

class ImkReformatJob(params: Parameter, override val enableHiveSupport: Boolean = true) extends
  BaseSparkNrtJob(params.appName, params.mode) {

  override def run(): Unit = {
    logger.info("imkReformat begin")
    logger.info(params.sqlFile)
    sqlsc.sql(params.sqlFile)
    logger.info("imkReformat end")
  }
}