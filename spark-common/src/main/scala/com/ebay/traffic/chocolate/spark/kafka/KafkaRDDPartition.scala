package com.ebay.traffic.chocolate.spark.kafka

import org.apache.kafka.common.TopicPartition
import org.apache.spark.Partition

/**
  * Created by yliu29 on 3/12/18.
  */
case class KafkaRDDPartition(
                         val index: Int,
                         val tp: TopicPartition,
                         val untilOffset: Long
                       ) extends Partition
