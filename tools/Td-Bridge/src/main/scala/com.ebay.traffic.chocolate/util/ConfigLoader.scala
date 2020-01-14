package com.ebay.traffic.chocolate.util

import java.io.{BufferedReader, InputStreamReader}

/**
  * Created by lxiong1
  */
object ConfigLoader {

  def loadConfig(filePath: String) : scala.collection.mutable.Map[String, String] = {
    val configs = scala.collection.mutable.Map[String, String]()

    val reader = new BufferedReader(new InputStreamReader(ConfigLoader.getClass.getResourceAsStream(filePath)))

    var line = reader.readLine()

    while (line != null) {
      val kv = line.split("=")
      if (kv.length > 1) {
        configs.put(kv.apply(0), kv.apply(1))
      }
      line = reader.readLine()
    }

    configs
  }

  def loadConfig() : scala.collection.mutable.Map[String, String] = loadConfig("/config/mrkt-event-config.properties")
}

