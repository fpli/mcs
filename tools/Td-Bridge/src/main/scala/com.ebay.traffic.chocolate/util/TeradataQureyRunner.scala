package com.ebay.traffic.chocolate.util

import java.sql.DriverManager

import TeradataType.TeradataType
import org.slf4j.LoggerFactory

/**
  * Created by lxiong1
  */
object TeradataQureyRunner {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(tdType: TeradataType, query: String, userInfo: (String, String)): Unit = {

    val connectUrl = getConnectUrl(tdType)

    logger.info("login td [" + connectUrl + "] | user [" + userInfo._1 + "]")
    Class.forName("com.teradata.jdbc.TeraDriver")
    val conn = DriverManager.getConnection(connectUrl, userInfo._1, userInfo._2)

    logger.info("query: " + query)
    val statements = query.split(";")

    for (i <- 0 until (statements.length - 1)) {
      logger.info("execute statement: " + statements.apply(i))
      val stmt = conn.prepareStatement(statements.apply(i))
      try {
        stmt.execute
      } catch {
        case e: Exception => {
          logger.error("Sql exception: " + e.getMessage)
          throw new Exception(e)
        }

      }
    }

  }

  private def getConnectUrl(tdType: TeradataType): String = {
    logger.info("------into getConnectUrl------")
    try {
      val configs = ConfigLoader.loadConfig()
      val key = "td." + tdType.toString.toLowerCase + ".connection.url"
      logger.info(key)
      configs.apply(key)
    } catch {
      case e: Exception => {
        logger.error("getConnectUrl exception: " + e.getMessage)
        throw new Exception(e)
      }
    }
  }

}
