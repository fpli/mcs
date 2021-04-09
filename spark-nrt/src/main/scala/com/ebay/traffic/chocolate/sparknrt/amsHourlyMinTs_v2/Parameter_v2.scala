package com.ebay.traffic.chocolate.sparknrt.amsHourlyMinTs_v2

import scopt.OptionParser

/**
 * Created by jialili1 on 06/21/19.
 */
case class Parameter_v2(appName: String = "AMSHourlyMinTs_v2",
                     mode: String = "yarn",
                     workDir: String = "",
                     channel: String = "",
                     usage: String = "",
                     metaSuffix: String = "",
                     outputDir: String = "")

object Parameter_v2 {

  private lazy val parser = new OptionParser[Parameter_v2]("AMSHourlyMinTs_v2") {
    head("AMSHourlyMinTs_v2")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("workDir")
      .required
      .valueName("workDir")
      .action((cont, param) => param.copy(workDir = cont))

    opt[String]("channel")
      .required
      .valueName("channel")
      .action((cont, param) => param.copy(channel = cont))

    opt[String]("usage")
      .required
      .valueName("usage")
      .action((cont, param) => param.copy(usage = cont))

    opt[String]("metaSuffix")
      .required
      .valueName("metaSuffix")
      .action((cont, param) => param.copy(metaSuffix = cont))

    opt[String]("outputDir")
      .required
      .valueName("outputDir")
      .action((cont, param) => param.copy(outputDir = cont))

  }

  def apply(args: Array[String]): Parameter_v2 = parser.parse(args, Parameter_v2()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}
