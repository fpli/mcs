package com.ebay.traffic.chocolate.job

import com.ebay.traffic.chocolate.utils.{TeradataSqlFileSubmitter, TeradataType}
import org.slf4j.LoggerFactory


/**
  * Created by zhofan on 2019/06/11.
  */
object TdDataMove extends BaseJob {
  private val logger = LoggerFactory.getLogger(this.getClass)

  protected override val retryCount = 3

  override def execute(argMap: Map[String, String]): Unit = {

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
