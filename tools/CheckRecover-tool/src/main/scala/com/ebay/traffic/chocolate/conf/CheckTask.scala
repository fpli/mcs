package com.ebay.traffic.chocolate.conf

/**
  * Created by lxiong1 on 27/11/18.
  */
case class CheckTask(val jobName: String,
                     val inputDir: String,
                     val ts: Long,
                     val td: Int,
                     val period: Int,
                     val dataCountDir: String)
