package com.ebay.traffic.chocolate.sparknrt.cappingV2
import scopt.OptionParser

/**
 * Created by yuhxiao on 1/3/21.
 */
case class ParameterV2(appName: String = "CappingRule_v2",
                        mode: String = "yarn",
                        channel: String = "",
                        propertiesFile: String = "",
                        workDir: String = "",
                        outputDir: String = "",
                        archiveDir: String = "",
                        partitions: Int = 3)

object ParameterV2 {

  private lazy val parser = new OptionParser[ParameterV2]("CappingRule_v2") {
    head("CappingRule_v2")

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

    opt[String]("propertiesFile")
      .required
      .valueName("propertiesFile")
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
  }

  def apply(args: Array[String]): ParameterV2 = parser.parse(args, ParameterV2()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}