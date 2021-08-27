package com.ebay.traffic.chocolate.sparknrt.monitoringV2

import scopt.OptionParser

/**
 * Created by yuhxiao on 22/06/21.
  */
case class ParameterV2(appName: String = "Monitoring_v2",
                       mode: String = "yarn",
                       channel: String = "",
                       workDir: String = ""
                    )

object ParameterV2 {

  private lazy val parser = new OptionParser[ParameterV2]("Monitoring_v2") {
    head("Monitoring")

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
  }

  def apply(args: Array[String]): ParameterV2 = parser.parse(args, ParameterV2()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }

}