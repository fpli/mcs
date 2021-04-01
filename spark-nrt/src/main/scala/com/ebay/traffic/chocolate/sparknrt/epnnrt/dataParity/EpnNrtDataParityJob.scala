package com.ebay.traffic.chocolate.sparknrt.epnnrt.dataParity

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob

object EpnNrtDataParityJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)
    val job = new EpnNrtDataParityJob(params)
    job.run()
    job.stop()
  }
}
class EpnNrtDataParityJob(params: Parameter, override val enableHiveSupport: Boolean = true) extends
  BaseSparkNrtJob(params.appName, params.mode) {

  override def run(): Unit = {
    logger.info("epnnrt data parity begin")
    logger.info(params.sqlFile)
    sqlsc.sql(params.sqlFile)
    logger.info("epnnrt data parity end")
  }
}