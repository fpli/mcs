package com.ebay.traffic.chocolate.sparknrt.epnnrt_v2

import scopt.OptionParser


case class Parameter_v2(appName: String = "epnnrt_v2",
                        mode: String = "yarn",
                        partitions: Int = 1,
                        lvsWorkDir: String = "",
                        slcWorkDir: String = "",
                        resourceDir: String = "",
                        filterTime: String = "",
                        outputDir: String = "")
object Parameter_v2 {
  private lazy val parser = new OptionParser[Parameter_v2]("epnnrt_v2") {
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

    opt[String]("lvsWorkDir")
      .required
      .valueName("lvsWorkDir")
      .action((cont, param) => param.copy(lvsWorkDir = cont))

    opt[String]("slcWorkDir")
      .required
      .valueName("slcWorkDir")
      .action((cont, param) => param.copy(slcWorkDir = cont))

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

  def apply(args: Array[String]): Parameter_v2 = parser.parse(args, Parameter_v2()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}


