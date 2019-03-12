package com.ebay.traffic.chocolate.sparknrt.compare


import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob


object CompareJob {
  def main(args: Array[String]): Unit = {
    val params = Parameter(args)
   /* val job = new CompareJob(params)
    job.run()
    job.stop()*/
   /*val impressionJob = new ImpressionCompare(params)
    impressionJob.run()
    impressionJob.stop()*/
 /*   val testJob = new TmpCompare(params)
    testJob.run()
    testJob.stop()*/
    val clickJob = new ImpressionCompare(params)
    clickJob.run()
    clickJob.stop()
  }
}


class CompareJob(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode) {
  override def run(): Unit = {
    logger.info("Comparing click and impression data...")
    /*if (params.click_run) {
      val clickJob = new ClickCompare(params)
      clickJob.run()
      clickJob.stop()
    }
    if (params.impression_run) {
      val impressionJob = new ImpressionCompare(params)
      impressionJob.run()
      impressionJob.stop()
    }*/
   /*// val clickJob = new ClickCompare(params)
    val impressionJob = new ImpressionCompare(params)
   // clickJob.run()
    impressionJob.run()
   // clickJob.stop()
    impressionJob.stop()*/
  }
}
