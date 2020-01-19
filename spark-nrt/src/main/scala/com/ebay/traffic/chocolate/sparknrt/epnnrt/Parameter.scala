package com.ebay.traffic.chocolate.sparknrt.epnnrt

import scopt.OptionParser


case class Parameter(appName: String = "epnnrt",
                     mode: String = "yarn",
                     partitions: Int = 1,
                     workDir: String = "",
                     resourceDir: String = "",
                     filterTime: String = "",
                     outputDir: String = "/apps/epn-nrt")

object Parameter {
  private lazy val parser = new OptionParser[Parameter]("epnnrt") {
    head("epnnrt")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[Int]("partitions")
      .optional
      .valueName("partitions")
      .action((cont, param) => param.copy(partitions = cont))

    opt[String]("workDir")
      .required
      .valueName("workDir")
      .action((cont, param) => param.copy(workDir = cont))

    opt[String]("resourceDir")
      .required
      .valueName("resourceDir")
      .action((cont, param) => param.copy(resourceDir = cont))

    opt[String]("filterTime")
      .optional
      .valueName("filterTime")
      .action((cont, param) => param.copy(filterTime = cont))

    opt[String]("outPutDir")
      .required
      .valueName("outPutDir")
      .action((cont, param) => param.copy(outputDir = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}
