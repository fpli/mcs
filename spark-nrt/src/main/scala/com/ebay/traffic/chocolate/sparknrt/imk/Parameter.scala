/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.imk

import scopt.OptionParser

/**
  * @author Xiang Li
  * @since 2020/08/18
  * Input parameter of IMK job
  * @param appName  app name
  * @param mode application submit mode
  * @param inputSource input source table name
  * @param deltaDir delta lake table dir
  * @param outPutDir output table dir
  * @param doneFileDir done file dir of output table
  * @param doneFilePrefix done file prefix
  * @param partitions partitions of the output
  */
case class Parameter(appName: String = "ImkNrtJob",
                     mode: String = "yarn",
                     inputSource: String = "",
                     deltaDir: String = "",
                     outPutDir: String = "",
                     doneFileDir: String = "",
                     jobDir: String = "",
                     doneFilePrefix: String = "",
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

    opt[String]("inputSource")
      .required
      .valueName("inputSource")
      .action((cont, param) => param.copy(inputSource = cont))

    opt[String]("deltaDir")
      .required
      .valueName("deltaDir")
      .action((cont, param) => param.copy(deltaDir = cont))

    opt[String]("outPutDir")
      .required
      .valueName("outPutDir")
      .action((cont, param) => param.copy(outPutDir = cont))

    opt[String]("doneFileDir")
      .required
      .valueName("doneFileDir")
      .action((cont, param) => param.copy(doneFileDir = cont))

    opt[String]("jobDir")
      .required
      .valueName("jobDir")
      .action((cont, param) => param.copy(jobDir = cont))

    opt[String]("doneFilePrefix")
      .required
      .valueName("doneFilePrefix")
      .action((cont, param) => param.copy(doneFilePrefix = cont))

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
