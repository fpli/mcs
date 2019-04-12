package com.ebay.traffic.chocolate.sparknrt.reporting

import scopt.OptionParser

case class EPNParameter(appName: String = "Reporting",
                        mode: String = "yarn",
                        action: String = "",
                        workDir: String = "",
                        archiveDir: String = "",
                        elasticsearchUrl: String = "",
                        batchSize: Int = 10)

object EPNParameter {

  private lazy val parser = new OptionParser[EPNParameter]("EPNReporting") {
    head("Reporting")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("action")
      .required
      .valueName("action")
      .action((cont, param) => param.copy(action = cont))

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

  }

  def apply(args: Array[String]): EPNParameter = parser.parse(args, EPNParameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}