package com.ebay.traffic.chocolate.sparknrt.hercules

import scopt.OptionParser

case class Parameter(appName: String = "touchImkHourlyDone",
                     mode: String = "yarn",
                     workDir: String = "",
                     lagDir: String = "",
                     doneDir: String = "")

object Parameter {
  private lazy val parser = new OptionParser[Parameter]("touchImkHourlyDone") {
    head("touchImkHourlyDone")

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

    opt[String]("lagDir")
      .required
      .valueName("lagDir")
      .action((cont, param) => param.copy(lagDir = cont))

    opt[String]("doneDir")
      .required
      .valueName("doneDir")
      .action((cont, param) => param.copy(doneDir = cont))

  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }

}
