package com.ebay.traffic.chocolate.sparknrt.capping

import scopt.OptionParser

/**
  * Created by xiangli4 on 4/8/18.
  */
case class Parameter(appName: String = "CappingRule",
                     mode: String = "yarn",
                     channel: String = "",
                     workDir: String = "",
                     outputDir: String = "",
                     partitions: Int = 3,
                     ipThreshold: Int = 1000)

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("CappingRule") {
    head("CappingRule")

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

    opt[String]("outputDir")
      .required
      .valueName("outputDir")
      .action((cont, param) => param.copy(outputDir = cont))

    opt[Int]("partitions")
      .optional
      .valueName("partitions")
      .action((cont, param) => param.copy(partitions = cont))

    opt[Int]("ipThreshold")
      .optional
      .valueName("ipThreshold")
      .action((cont, param) => param.copy(ipThreshold = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}