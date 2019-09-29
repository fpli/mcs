//package com.ebay.traffic.chocolate.spark
//
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//import org.scalamock.scalatest.MockFactory
//
//
///**
//  * Created by zhofan on 2019/06/11.
//  */
//class RotationMergeFileTest extends FunSuite with BeforeAndAfterAll with MockFactory {
//  val testRoot = getClass.getResource("/data").getPath + "/RotationMergeFileTest"
//
//  override def beforeAll(): Unit = {
//    TestFileUtility.createDir(testRoot)
//  }
//
//  test("Test clients file merge") {
//    println("==========" + testRoot)
//    val clientsRoot = s"$testRoot/clients"
//    val srcData = s"$clientsRoot/in"
//    val destPath = s"$clientsRoot/out"
//
//    TestFileUtility.createDir(srcData)
//    TestFileUtility.copyDir(srcData, getClass.getResource("/data/").getPath + "clients/")
//
//    val job = new RotationClientsDataMergeFileJob(
//      RotationDataParam(Map("srcPath" -> srcData, "destPath" -> destPath, "partitionNum" -> "1", "ds" -> "dw_mpx_clients"))
//    )
//    job.execute(8)
//  }
//}
