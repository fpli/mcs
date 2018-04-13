package com.ebay.traffic.chocolate.spark.kafka

import java.util
import java.util.concurrent.TimeoutException

import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._

/**
  * Created by yliu29 on 3/12/18.
  *
  * User should call #commitOffsets manually.
  */
class KafkaRDD[K, V](
                      @transient val sc: SparkContext,
                      val topic: String,
                      val kafkaProperties: util.Properties,
                      val maxConsumeSize: Long = 100000000l // maximum number of events can be consumed in one task: 100M
                    ) extends RDD[ConsumerRecord[K, V]](sc, Nil) {
  val POLL_STEP_MS = 3000

  @transient lazy val consumer = {
    kafkaProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    new KafkaConsumer[K, V](kafkaProperties)
  }

  @transient lazy val untilOffsets = {
    val kpartitions = new util.ArrayList[TopicPartition]()
    // get kafka partitions
    val iter = consumer.partitionsFor(topic).iterator()
    while (iter.hasNext) {
      kpartitions.add(new TopicPartition(topic, (iter.next().partition())))
    }

    // get end offsets of kafka partitions
    val endOffsets = consumer.endOffsets(kpartitions)
    log.info(s"###topic: ${topic}, endOffsets: ${endOffsets}")

    consumer.assign(kpartitions)

    // until offsets
    val untilOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()
    val endOffsetIter = endOffsets.entrySet().iterator()
    while (endOffsetIter.hasNext) {
      val endOffset = endOffsetIter.next()
      val tp = endOffset.getKey
      val position = consumer.position(tp)
      val until = Math.min(endOffset.getValue, position + maxConsumeSize)
      log.info(s"###topic-partition: ${tp}, position: ${position}, until: ${until}")
      untilOffsets.put(endOffset.getKey, new OffsetAndMetadata(until))
    }
    consumer.unsubscribe()

    untilOffsets
  }

  /**
    * Each Spark partition consumes one kafka partition
    *
    * @return the partitions of kafka RDD
    */
  override protected def getPartitions: Array[Partition] = {
    val partitions = new Array[Partition](untilOffsets.size) // spark partitions
    val iterator = untilOffsets.entrySet().iterator() // iterator of until offsets of kafka partitions
    var index = 0 // index of partition
    while (iterator.hasNext) {
      val entry = iterator.next()
      partitions(index) = new KafkaRDDPartition(index, entry.getKey, entry.getValue.offset())
      index = index + 1
    }
    partitions
  }

  /**
    * Compute the spark partition which consumes one kafka partition
    *
    * @param partition the partition
    * @param context spark task context
    * @return iterator of records
    */
  override def compute(
                        partition: Partition,
                        context: TaskContext): Iterator[ConsumerRecord[K, V]] = {
    val part = partition.asInstanceOf[KafkaRDDPartition]

    log.info(s"Computing topic partition: ${part.tp}, untilOffset: ${part.untilOffset}, index: ${part.index}")

    // assign topic partitions to consumer
    consumer.assign(util.Arrays.asList(part.tp))
    context.addTaskCompletionListener(context => {
      consumer.close()
    })

    new KafkaRDDIterator(part, consumer, context)
  }

  /**
    * Commit the offsets manually.
    */
  def commitOffsets() = {
    consumer.commitSync(untilOffsets)
  }

  /**
    * Close kafka consumer
    */
  def close() = {
    consumer.close()
  }

  /**
    * Class of kafka RDD iterator
    */
  private class KafkaRDDIterator(
                                  val part: KafkaRDDPartition,
                                  val consumer: Consumer[K, V],
                                  val context: TaskContext
                                ) extends Iterator[ConsumerRecord[K, V]] {
    val topicPartition = part.tp
    // get current position of consumer group for kafka topic partition
    var pos = consumer.position(topicPartition)
    log.info(s"KafkaRDDIterator: ${topicPartition}, " +
      s"position: ${pos}, untilOffset: ${part.untilOffset}, index: ${part.index}")

    var buffer: util.Iterator[ConsumerRecord[K, V]] = null

    override def hasNext: Boolean = pos < part.untilOffset

    override def next(): ConsumerRecord[K, V] = {
      assert (hasNext, "Can't call next() once there is no more records")

      pos = pos + 1

      if (buffer != null && buffer.hasNext) {
        buffer.next
      } else {
        poll(POLL_STEP_MS, pos - 1)
      }
    }

    /** poll records from kafka topic partition **/
    private def poll(timeout: Long, offset: Long): ConsumerRecord[K, V] = {
      var result : ConsumerRecord[K, V] = null

      buffer = null
      while (buffer == null) {
        val records = consumer.poll(timeout)
        val iter = records.iterator()
        if (iter.hasNext) {
          result = iter.next
          if (result.offset() > offset) {
            log.warn(s"Cannot fetch records in [${offset}, ${result.offset})")
          } else if (result.offset() < offset) {
            log.warn(s"Tried to fetch ${offset} but the returned record offset was ${result.offset}")
          }
          buffer = iter
        } else {
          // We cannot fetch anything after `poll`. Two possible cases:
          // - `offset` is out of range so that Kafka returns nothing.
          // - Cannot fetch any data before timeout. TimeoutException will be thrown.
          val range = getAvailableOffsetRange()
          log.info(s"KafkaRDDIterator iterating: ${topicPartition}, " +
            s"offset: ${offset}, available offset range: [${range.earliest}, ${range.latest})")
          if (offset < range.earliest || offset > range.latest) {
            throw new OffsetOutOfRangeException(
              Map(topicPartition -> java.lang.Long.valueOf(offset)).asJava)
          } else {
            throw new TimeoutException(
              s"Cannot fetch record for offset ${offset} in ${timeout} milliseconds")
          }
        }
      }
      result
    }

    case class AvailableOffsetRange(earliest: Long, latest: Long)

    /**
      * Return the available offset range of the current partition. It's a pair of the earliest offset
      * and the latest offset.
      */
    def getAvailableOffsetRange(): AvailableOffsetRange = {
      consumer.seekToBeginning(Set(topicPartition).asJava)
      val earliestOffset = consumer.position(topicPartition)
      consumer.seekToEnd(Set(topicPartition).asJava)
      val latestOffset = consumer.position(topicPartition)
      AvailableOffsetRange(earliestOffset, latestOffset)
    }
  }
}
