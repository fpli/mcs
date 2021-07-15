package com.ebay.traffic.chocolate.sparknrt.epnnrtV2

import scopt.OptionParser


case class ParameterV2(appName: String = "epnnrt_v2",
                       mode: String = "yarn",
                       partitions: Int = 1,
                       inputWorkDir: String = "",
                       outputWorkDir: String = "",
                       resourceDir: String = "",
                       filterTime: String = "",
                       outputDir: String = "")
object ParameterV2 {
  private lazy val parser = new OptionParser[ParameterV2]("epnnrt_v2") {
    head("epnnrt_v2")

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

    opt[String]("inputWorkDir")
      .required
      .valueName("inputWorkDir")
      .action((cont, param) => param.copy(inputWorkDir = cont))

    opt[String]("outputWorkDir")
      .required
      .valueName("outputWorkDir")
      .action((cont, param) => param.copy(outputWorkDir = cont))

    opt[String]("resourceDir")
      .required
      .valueName("resourceDir")
      .action((cont, param) => param.copy(resourceDir = cont))

    opt[String]("filterTime")
      .optional
      .valueName("filterTime")
      .action((cont, param) => param.copy(filterTime = cont))

    opt[String]("outputDir")
      .required
      .valueName("outputDir")
      .action((cont, param) => param.copy(outputDir = cont))
  }

  def apply(args: Array[String]): ParameterV2 = parser.parse(args, ParameterV2()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}


