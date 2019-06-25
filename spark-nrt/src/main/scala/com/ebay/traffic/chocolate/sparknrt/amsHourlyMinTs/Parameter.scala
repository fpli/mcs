package com.ebay.traffic.chocolate.sparknrt.amsHourlyMinTs

import scopt.OptionParser

/**
  * Created by jialili1 on 06/21/19.
  */
case class Parameter(appName: String = "AMSHourlyMinTs",
                     mode: String = "yarn",
                     workDir: String = "",
                     channel: String = "",
                     usage: String = "",
                     metaSuffix: String = "",
                     outputDir: String = "")

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("AMSHourlyMinTs") {
    head("AMSHourlyMinTs")

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

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}
