package com.ebay.traffic.chocolate.spark.kafka

import java.{lang, util}
import java.util.Map
import java.util.concurrent.TimeoutException

import com.ebay.traffic.monitoring.Field
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._

/**
  * Created by yuhxiao on 4/2/21.
  *
  * User should call #commitOffsets manually.
  */
class KafkaRDD_v2[K, V](
                      @transient val sc: SparkContext,
                      val topic: String,
                      val properties: util.Properties,
                      val appName: String,
                      val maxConsumeSize: Long = 100000l // maximum number of events can be consumed in one task: 100M
                    ) extends RDD[ConsumerRecord[K, V]](sc, Nil) {
  val POLL_STEP_MS = 30000

  @transient lazy val consumer = {
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    new KafkaConsumer[K, V](properties)
  }

  @transient lazy val metrics: SherlockioMetrics = {
    SherlockioMetrics.init(properties.getProperty("sherlockio.namespace"),properties.getProperty("sherlockio.endpoint"),properties.getProperty("sherlockio.user"))
    val sherlockioMetrics: SherlockioMetrics = SherlockioMetrics.getInstance()
    sherlockioMetrics.setJobName(appName)
    sherlockioMetrics
  }

  @transient lazy val untilOffsets = {
    log.info(s"###topic: ${topic}, computing endOffsets")
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
    var latestExceed=false;
    while ((!latestExceed)&endOffsetIter.hasNext) {
      val endOffset = endOffsetIter.next()
      val tp = endOffset.getKey
      val position = consumer.position(tp)
      val until = Math.min(endOffset.getValue, position + maxConsumeSize)
      log.info(s"###topic-partition: ${tp}, position: ${position}, until: ${until}")
      if (metrics != null) {
        // lag metric
        metrics.mean("SparkKafkaConsumerLag", endOffset.getValue - position,
          Field.of[String, AnyRef]("topic", tp.topic()),
          Field.of[String, AnyRef]("consumer", Int.box(tp.partition)))
      }
      if(until <= position&&properties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG).equals("latest")){
        latestExceed=true
      }
      else if (until > position) {
        untilOffsets.put(endOffset.getKey, new OffsetAndMetadata(until))
      }
    }
    if(latestExceed){
      val commitOffsets = new util.HashMap[TopicPartition, OffsetAndMetadata]()
      val it: util.Iterator[Map.Entry[TopicPartition, lang.Long]] = endOffsets.entrySet().iterator()
      while (it.hasNext) {
        val endOffset = it.next()
        val tp = endOffset.getKey
        val position = consumer.position(tp)
        commitOffsets.put(endOffset.getKey, new OffsetAndMetadata(position))
      }
      log.info(s"commitOffsets: $commitOffsets")
      consumer.commitSync(commitOffsets)
    }
    consumer.unsubscribe()
    if (metrics != null)
      metrics.flush()
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
      if (metrics != null)
        metrics.flush()
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
    var offset = consumer.position(topicPartition)
    log.info(s"KafkaRDDIterator: ${topicPartition}, " +
      s"position: ${offset}, untilOffset: ${part.untilOffset}, index: ${part.index}")

    // metrics
    if (metrics != null) {
      metrics.meterByGauge("SparkKafkaConsumerOffset", offset,
        Field.of[String, AnyRef]("topic", topicPartition.topic()),
        Field.of[String, AnyRef]("consumer", Int.box(topicPartition.partition())))
    }

    var nextRecord: ConsumerRecord[K, V] = null // cache the next record

    var buffer: util.Iterator[ConsumerRecord[K, V]] = null

    override def hasNext: Boolean = offset < part.untilOffset && getNext

    def getNext(): Boolean = {
      if (nextRecord == null) {
        nextRecord = if (buffer != null && buffer.hasNext) {
          buffer.next
        } else {
          poll(POLL_STEP_MS)
        }
      }

      nextRecord != null
    }

    override def next(): ConsumerRecord[K, V] = {
      if (!hasNext) {
        throw new IllegalStateException("Can't call next() once there is no more records")
      }
      val record = nextRecord
      offset = record.offset() + 1 // update offset
      if (metrics != null) {
        metrics.meterByGauge("KafkaRDDInput", 1)
      }

      nextRecord = null
      record
    }

    /**
     * validate resultOffset, in normal cases, resultOffset should equals to offset
     * @param resultOffset the offset of this message
     * @param offset the topic partition current offset
     * @param partUtilOffset max consume offset, min(endOffset.getValue, position + maxConsumeSize)
     */
    private def validateResultOffset(resultOffset: Long, offset: Long, partUtilOffset: Long): Unit = {
      if (resultOffset > offset) {
        log.warn(s"Cannot fetch records in [${offset}, ${resultOffset})")
        if (resultOffset >= partUtilOffset) {
          throw new IllegalStateException(
            s"Tried to fetch ${offset} but the returned record offset was ${resultOffset} " +
              s"which exceeded untilOffset ${partUtilOffset}")
        } else {
          log.warn(s"Skip missing records in [$offset, ${resultOffset})")
        }
      } else if (resultOffset < offset) {
        throw new IllegalStateException(
          s"Tried to fetch ${offset} but the returned record offset was ${resultOffset}")
      }
    }

    /**
     * validate partUtilOffset, in normal cases, partUtilOffset should be larger than rangeEarliest
     * @param rangeEarliest the earliest offset of this partition
     * @param offset the topic partition current offset
     * @param partUtilOffset max consume offset, min(endOffset.getValue, position + maxConsumeSize)
     */
    def validatePartUtilOffset(rangeEarliest: Long, offset: Long, partUtilOffset: Long): Unit = {
      if (rangeEarliest >= partUtilOffset) { // range.earliest >= part.untilOffset
        throw new IllegalStateException(
          s"Tried to fetch [${offset}, ${partUtilOffset}) but the range earliest is ${rangeEarliest}")
      }
    }

    /**
     * validate current offset, current offset should be less than latest offset of the partition
     * @param offset current offset
     * @param rangeLatest latest offset of the partition
     * @param timeout poll timeout
     */
    def validateCurrentOffset(offset: Long, rangeLatest:Long, timeout: Long):Unit = {
      if (offset >= rangeLatest) {
        throw new IllegalStateException(
          s"Tried to fetch ${offset} which exceeds the latest offset ${rangeLatest}.")
      } else { // range.earliest <= offset < range.latest
        throw new TimeoutException(
          s"Cannot fetch record for offset ${offset} in ${timeout} milliseconds")
      }
    }

    /** poll records from kafka topic partition **/
    private def poll(timeout: Long): ConsumerRecord[K, V] = {
      var result : ConsumerRecord[K, V] = null

      buffer = null
      var finish = false
      var reset = false // reset flag
      while (!finish) {
        if (reset) { // if reset flag is true, we need to do seek
          consumer.seek(topicPartition, offset)
          reset = false
        }
        val records = consumer.poll(timeout)
        val iter = records.iterator()
        if (iter.hasNext) {
          result = iter.next
          validateResultOffset(result.offset(), offset, part.untilOffset)
          buffer = iter
          finish = true
        } else {
          // We cannot fetch anything after `poll`. Two possible cases:
          // - `offset` is out of range so that Kafka returns nothing.
          // - Cannot fetch any data before timeout. TimeoutException will be thrown.
          val range = getAvailableOffsetRange()
          log.info(s"KafkaRDDIterator iterating: ${topicPartition}, " +
            s"offset: ${offset}, available offset range: [${range.earliest}, ${range.latest})")
          if (range.earliest == range.latest) {
            log.warn(s"No more records, available offset range: [${range.earliest}, ${range.latest})")
            offset = range.latest
            finish = true
          } else {
            if (offset < range.earliest) {
              validatePartUtilOffset(range.earliest, offset, part.untilOffset)
              log.warn(s"Skip missing records in [$offset, ${range.earliest})")
              offset = range.earliest
              reset = true
            } else {
              validateCurrentOffset(offset, range.latest, timeout)
            }
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
