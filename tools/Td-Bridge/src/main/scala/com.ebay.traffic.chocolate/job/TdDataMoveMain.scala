package com.ebay.traffic.chocolate.job

import com.ebay.traffic.chocolate.util.{TeradataSqlFileSubmitter, TeradataType}
import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory

/**
  * Created by lxiong1
  */
object TdDataMoveMain extends BaseJob {

  private val logger = LoggerFactory.getLogger(this.getClass)

  protected override val retryCount = 3

  def config(): Unit = {
    val logDir = "../conf/log4j-td-bridge.properties"
    PropertyConfigurator.configure(logDir)
  }

  override def execute(argMap: Map[String, String]): Unit = {

    config()

    val sql = argMap.apply("sql")
    val td = argMap.apply("td")
    val hdp = argMap.apply("HDP")

    if (sql.isEmpty) {
      val errMsg = "sql is not set"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    logger.info("sql: " + sql)

    if (td.isEmpty) {
      val errMsg = "td is not set"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    logger.info("td: " + td)

    if (hdp.isEmpty) {
      val errMsg = "HDP is not set"
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    val tdType = TeradataType.getTdType(td)

    if (null == tdType) {
      val errMsg = "unsupported TD type: " + td
      logger.error(errMsg)
      throw new Exception(errMsg)
    }

    logger.info("ready to execute td move sql")

    TeradataSqlFileSubmitter.execute(tdType, sql, argMap)
  }

}
