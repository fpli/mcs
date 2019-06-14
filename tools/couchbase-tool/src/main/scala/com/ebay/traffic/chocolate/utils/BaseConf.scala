package com.ebay.traffic.chocolate.utils

import com.typesafe.config.ConfigFactory


/**
  * Created by zhofan on 2019/06/11.
  */

trait BaseConf {
  final lazy val conf = BaseConf.conf
}

object BaseConf {
  lazy val conf = ConfigFactory.load()
}
