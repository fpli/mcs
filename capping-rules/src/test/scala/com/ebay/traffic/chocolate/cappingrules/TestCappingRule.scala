package com.ebay.traffic.chocolate.cappingrules

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

/**
  * Created by yliu29 on 11/3/17.
  */
class TestCappingRule extends BaseFunSuite {
  val TRANSACTION_TABLE_NAME = "transaction"
  val TRANSACTION_CF_DEFAULT = "cf_default"

  lazy val hbaseUtility = new HBaseTestingUtility

  lazy val hbaseConf = {
    hbaseUtility.getConfiguration
  }

  lazy val hbaseConnection = {
    hbaseUtility.getConnection
  }

  lazy val hbaseAdmin = {
    hbaseUtility.getHBaseAdmin
  }

  lazy val transactionTable = {
    new HTable(TableName.valueOf(TRANSACTION_TABLE_NAME), hbaseConnection)
  }

  lazy val spark = {
    SparkSession.builder().master("local[4]").appName("test")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
      .getOrCreate()
  }

  lazy val sc = {
    spark.sparkContext
  }

  override def beforeAll() = {
    hbaseUtility.startMiniCluster()

    initHBaseTransactionTable()
  }

  override def afterAll() = {
    spark.stop()
    hbaseUtility.shutdownMiniCluster()
  }

  private def initHBaseTransactionTable() = {
    val tableDesc = new HTableDescriptor(TableName.valueOf(TRANSACTION_TABLE_NAME))
    tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
      .setCompressionType(Compression.Algorithm.NONE))
    hbaseAdmin.createTable(tableDesc)

    addEvent(transactionTable, new Event(1, 0, 0, 0, "snid1", true))
    addEvent(transactionTable, new Event(2, 0, 0, 0, "snid2", true))
    addEvent(transactionTable, new Event(3, 0, 0, 0, "snid3", true))
    addEvent(transactionTable, new Event(4, 0, 0, 0, "snid4", true))
    addEvent(transactionTable, new Event(5, 0, 0, 0, "snid5", true))
  }

  test("read hbase in spark") {
    hbaseConf.set(TableInputFormat.INPUT_TABLE, TRANSACTION_TABLE_NAME)

    val rdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    assert(rdd.count() == 5)


//    val scan = new Scan()
//    scan.addFamily(Bytes.toBytes(TRANSACTION_CF_DEFAULT))
//    val rs = table.getScanner(scan)
//    try {
//      var r: Result = rs.next
//      while (r != null) {
//        val snapId = Bytes.toLong(r.getRow)
//        val timestamp = Bytes.toLong(r.getColumn(Bytes.toBytes(TRANSACTION_CF_DEFAULT), Bytes.toBytes("timestamp")).get(0).getValueArray)
//
//        System.out.println("Result+++:" + snapId)
//        r = rs.next()
//      }
//
//    } finally {
//      rs.close()
//    }
  }

  private def putCell[T](put: Put, family: String, qualifier: String, value: T) = {
    val bytes = value match {
      case _: Long => Bytes.toBytes(value.asInstanceOf[Long])
      case _: String => Bytes.toBytes(value.asInstanceOf[String])
      case _: Boolean => Bytes.toBytes(value.asInstanceOf[Boolean])
    }
    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), bytes)
  }

  case class Event(snapshot_id: Long,
                   timestamp: Long,
                   publisher_id: Long,
                   campaign_id: Long,
                   snid: String,
                   is_tracked: Boolean)

  private def addEvent(table: HTable, event: Event) = {
    val put = new Put(Bytes.toBytes(event.snapshot_id))
    putCell(put, TRANSACTION_CF_DEFAULT, "timestamp", event.timestamp)
    putCell(put, TRANSACTION_CF_DEFAULT, "publisher_id", event.publisher_id)
    putCell(put, TRANSACTION_CF_DEFAULT, "campaign_id", event.campaign_id)
    putCell(put, TRANSACTION_CF_DEFAULT, "snid", event.snid)
    putCell(put, TRANSACTION_CF_DEFAULT, "is_tracked", event.is_tracked)
    table.put(put)
  }
}