package com.ebay.traffic.chocolate.util

import scopt.OptionParser

/**
  * Created by lxiong1 on 11/27/18.
  */
case class Parameter(appName: String = "CheckJob",
                     mode: String = "yarn",
                     countDataDir: String = "",
                     ts: Long = 1l,
                     taskFile: String = "task.xml",
                     elasticsearchUrl: String = ""
                     )

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("CheckJob") {
    head("CheckJob")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("countDataDir")
      .required
      .valueName("countDataDir")
      .action((cont, param) => param.copy(countDataDir = cont))

    opt[Long]("ts")
      .required
      .valueName("timestamp")
      .action((cont, param) => param.copy(ts = cont))

    opt[String]("taskFile")
      .optional
      .valueName("taskFile")
      .action((cont, param) => param.copy(taskFile = cont))

    opt[String]("elasticsearchUrl")
      .optional
      .valueName("elasticsearchUrl")
      .action((cont, param) => param.copy(elasticsearchUrl = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }

}
