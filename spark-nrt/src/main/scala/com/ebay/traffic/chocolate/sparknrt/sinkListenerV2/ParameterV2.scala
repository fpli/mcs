package com.ebay.traffic.chocolate.sparknrt.sinkListenerV2
import scopt.OptionParser
case class ParameterV2(appName: String = "DedupeAndSinkListener_v2",
                       mode: String = "yarn",
                       channel: String = "",
                       kafkaTopic: String = "",
                       workDir: String = "",
                       outputDir: String = "",
                       partitions: Int = 3,
                       maxConsumeSize: Long = 100000l,
                       couchbaseDedupe: Boolean = true,
                       couchbaseTTL: Int = 3 * 24 * 60 * 60)

object ParameterV2 {

  private lazy val parser = new OptionParser[ParameterV2]("DedupeAndSinkListener_v2") {
    head("DedupeAndSinkListener_v2")

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

    opt[String]("kafkaTopic")
      .required
      .valueName("kafka topic")
      .action((cont, param) => param.copy(kafkaTopic = cont))

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

    opt[Long]("maxConsumeSize")
      .optional
      .valueName("maxConsumeSize")
      .action((cont, param) => param.copy(maxConsumeSize = cont))

    opt[Boolean]("couchbaseDedupe")
      .optional()
      .valueName("couchbaseDedupe")
      .action((cont, param) => param.copy(couchbaseDedupe = cont))

    opt[Int]("couchbaseTTL")
      .optional()
      .valueName("couchbaseTTL")
      .action((cont, param) => param.copy(couchbaseTTL = cont))
  }

  def apply(args: Array[String]): ParameterV2 = parser.parse(args, ParameterV2()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}