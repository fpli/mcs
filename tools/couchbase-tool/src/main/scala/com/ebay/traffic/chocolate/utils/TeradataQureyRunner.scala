package com.ebay.traffic.chocolate.utils

import java.sql.{DriverManager}

import TeradataType._
import org.slf4j.LoggerFactory


/**
  * Created by zhofan on 2019/06/11.
  */
object TeradataQureyRunner {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def run(tdType : TeradataType, query : String) : Unit = {

    val connectUrl = getConnectUrl(tdType)
    val login = parseLogin(tdType)

    logger.info("login td [" + connectUrl + "] | user [" + login._1 + "]")
    Class.forName("com.teradata.jdbc.TeraDriver")
    val conn = DriverManager.getConnection(connectUrl, login._1, login._2)

    val statements = query.split(";")

    for (i <- 0 until (statements.length - 1)) {
      logger.info("execute statement: " + statements.apply(i))
      val stmt = conn.prepareStatement(statements.apply(i))
      try {
        stmt.execute
      } catch {
        case e : Exception => {
          logger.error("Sql exception: " + e.getMessage)
          throw new Exception(e)
        }

      }
    }

  }

  private def getConnectUrl(tdType : TeradataType) : String = {
    val configs = ConfigLoader.loadConfig()
    val key = "td." + tdType.toString.toLowerCase + ".connection.url"
    configs.apply(key)
  }

  private def parseLogin(tdType : TeradataType) : (String, String) = {
    val configs = ConfigLoader.loadConfig()
    val key = "td." + tdType.toString.toLowerCase + ".login"
    val loginLine = configs.apply(key)

    //    for (line <- Source.fromFile(filename).getLines) {
    //      if (null != line) {
    //          return TDLoginParser.getLogin(line)
    //      }
    //    }

    if (null != loginLine) {
      return TDLoginParser.getLogin(loginLine)
    }

    logger.error("login cannot be found")

    return (null, null)
  }



}

