package com.ebay.traffic.chocolate.job

import scopt.OptionParser

case class Parameter(appName: String = "AmsDiffReport",
                     mode: String = "yarn",
                     outputPath: String="")

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("AmsClickReport") {
    head("AmsDiffReport")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("outputPath")
      .required
      .valueName("outputPath")
      .action((cont, param) => param.copy(outputPath = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}
