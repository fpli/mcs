package com.ebay.traffic.chocolate.sparknrt.cb2nd

import scopt.OptionParser

/**
 * Created by yuhxiao on 23/02/21.
 */
case class Parameter(appName: String = "CB2NDTool",
                     mode: String = "yarn",
                     begin: String = "",
                     end: String= "")

object Parameter {
  private lazy val parser = new OptionParser[Parameter]("CB2NDTool") {
    head("CB2NDTool")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("begin")
      .required
      .valueName("begin")
      .action((cont, param) => param.copy(begin = cont))

    opt[String]("end")
      .required
      .valueName("end")
      .action((cont, param) => param.copy(end = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}
