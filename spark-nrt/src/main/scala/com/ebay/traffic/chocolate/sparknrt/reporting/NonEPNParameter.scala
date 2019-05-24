package com.ebay.traffic.chocolate.sparknrt.reporting

import scopt.OptionParser

case class NonEPNParameter(appName: String = "NonEPNReporting",
                           mode: String = "yarn",
                           channel: String = "",
                           filterAction: String = "all",
                           workDir: String = "",
                           archiveDir: String = "",
                           elasticsearchUrl: String = "",
                           batchSize: Int = 10,
                           hdfsUri: String = "")

object NonEPNParameter {

  private lazy val parser = new OptionParser[NonEPNParameter]("NonEPNReporting") {
    head("Reporting")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("channel")
      .required
      .valueName("channel")
      .action((cont, param) => param.copy(channel = cont))

    opt[String]("filterAction")
      .required
      .valueName("filterAction")
      .action((cont, param) => param.copy(filterAction = cont))

    opt[String]("workDir")
      .required
      .valueName("workDir")
      .action((cont, param) => param.copy(workDir = cont))

    opt[String]("archiveDir")
      .required
      .valueName("archiveDir")
      .action((cont, param) => param.copy(archiveDir = cont))

    opt[String]("elasticsearchUrl")
      .optional
      .valueName("elasticsearchUrl")
      .action((cont, param) => param.copy(elasticsearchUrl = cont))

    opt[Int]("batchSize")
      .optional
      .valueName("batchSize")
      .action((cont, param) => param.copy(batchSize = cont))

    opt[String]("hdfsUri")
      .optional
      .valueName("hdfsUri")
      .action((cont, param) => param.copy(hdfsUri = cont))

  }

  def apply(args: Array[String]): NonEPNParameter = parser.parse(args, NonEPNParameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}