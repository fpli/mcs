package com.ebay.traffic.chocolate.sparknrt.reporting

import scopt.OptionParser

/**
  * Created by weibdai on 5/19/18.
  */
case class Parameter(appName: String = "Reporting",
                     mode: String = "yarn",
                     channel: String = "",
                     workDir: String = "",
                     archiveDir: String = "")

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("Reporting") {
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

    opt[String]("workDir")
      .required
      .valueName("workDir")
      .action((cont, param) => param.copy(workDir = cont))

    opt[String]("archiveDir")
      .required
      .valueName("archiveDir")
      .action((cont, param) => param.copy(archiveDir = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}