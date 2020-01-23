package com.ebay.traffic.chocolate.util

import java.io.{BufferedReader, InputStreamReader}

import TeradataType.TeradataType
import org.slf4j.LoggerFactory

/**
  * Created by lxiong1
  */
object TeradataSqlFileSubmitter {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def execute(tdType: TeradataType, sqlFile: String, params: Map[String, String]): Unit = {
    // build query statements
    val query = buildSqlStatement(sqlFile, params)

    logger.info("build query: " + query)

    try {
      //create userInfo tuple
      val userInfo = (params.get("TDUserName").get, params.get("TDPassWord").get)

      // run query
      TeradataQureyRunner.run(tdType, query, userInfo)
    } catch {
      case e: Exception => {
        logger.error("execute exception: " + e.getMessage)
        throw new Exception(e)
      }

    }
  }

  def buildSqlStatement(sqlFile: String, params: Map[String, String]): String = {
    val query = new StringBuilder
    val reader = new BufferedReader(new InputStreamReader(ConfigLoader.getClass.getResourceAsStream("/sql.teradata/" + sqlFile)))

    var line = reader.readLine()

    while (line != null) {
      query.append(line).append("\n")
      line = reader.readLine()
    }

    var queryStr = query.toString()

    params.foreach(k => {
      logger.info("replace key " + buildReplacementKey(k._1) + " | value " + k._2)
      queryStr = queryStr.replaceAll(buildReplacementKey(k._1), k._2)
    }

    )
    queryStr
  }

  private def buildReplacementKey(key: String): String = {
    """\{:""" + key + """:\}"""
  }

}
