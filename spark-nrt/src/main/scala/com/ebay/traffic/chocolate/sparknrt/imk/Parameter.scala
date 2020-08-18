/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.imk

import com.ebay.traffic.chocolate.sparknrt.imkETL.Parameter
import scopt.OptionParser

case class Parameter(appName: String = "ImkNrtJob",
                     mode: String = "yarn",
                     deltaDir: String = "",
                     outPutDir: String = "",
                     partitions: Int = 3)

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("Sword") {
    head("Sword")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("deltaDir")
      .required
      .valueName("deltaDir")
      .action((cont, param) => param.copy(deltaDir = cont))

    opt[String]("outPutDir")
      .required
      .valueName("outPutDir")
      .action((cont, param) => param.copy(outPutDir = cont))

    opt[Int]("partitions")
      .optional
      .valueName("partitions")
      .action((cont, param) => param.copy(partitions = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}
