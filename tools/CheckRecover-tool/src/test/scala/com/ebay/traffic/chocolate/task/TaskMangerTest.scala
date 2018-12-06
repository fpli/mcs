package com.ebay.traffic.chocolate.task

import com.ebay.traffic.chocolate.conf.CheckTask
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.spark.sql.SparkSession
import org.junit.Before

class TaskMangerTest extends BaseFunSuite {

  @Before
  val spark = SparkSession.builder()
    .appName("test")
    .master("local")
    .appName("SparkUnitTesting")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
    .getOrCreate();

  test("test getLastCount") {

    val data = spark.sqlContext.createDataFrame(Seq(CountData("test", 1543477200000l, 1)));
    val checkTask = CheckTask("test", "test", 1543477260000l, 1, 1, "test");
    val lastCount = TaskManger.getLastCount(data, checkTask);
    assert(lastCount == 1);
  }


}
