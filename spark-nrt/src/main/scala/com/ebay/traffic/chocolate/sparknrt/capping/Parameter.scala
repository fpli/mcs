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
                     archiveDir: String = "",
                     partitions: Int = 3,
                     elasticsearchUrl: String = "")

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

    opt[String]("archiveDir")
      .required
      .valueName("archiveDir")
      .action((cont, param) => param.copy(archiveDir = cont))

    opt[Int]("partitions")
      .optional
      .valueName("partitions")
      .action((cont, param) => param.copy(partitions = cont))

    opt[String]("elasticsearchUrl")
        .optional
        .valueName("elasticsearchUrl")
        .action((cont, param) => param.copy(elasticsearchUrl = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}