package com.ebay.traffic.chocolate.utils

import java.io.{BufferedReader, InputStreamReader}
import org.slf4j.LoggerFactory
import TeradataType.TeradataType


/**
  * Created by zhofan on 2019/06/11.
  */
object TeradataSqlFileSubmitter {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def execute(tdType : TeradataType, sqlFile : String, params : Map[String, String]) : Unit = {
    // build query statements
    val query = buildSqlStatement(sqlFile, params)

    logger.info("build query: " + query)

    // run query
    TeradataQureyRunner.run(tdType, query)
  }

  def buildSqlStatement(sqlFile : String, params : Map[String, String]) : String = {
    val query = new StringBuilder
    val reader = new BufferedReader(new InputStreamReader(ConfigLoader.getClass.getResourceAsStream("/sql/teradata/" + sqlFile)))

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

  private def buildReplacementKey(key : String) : String = {
    """\{:""" + key + """:\}"""
  }
}
