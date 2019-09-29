//package com.ebay.traffic.chocolate
//
//import com.ebay.traffic.chocolate.utils.{TeradataSqlFileSubmitter, TeradataType}
//import org.scalatest.{BeforeAndAfterAll, FunSuite}
//
//
///**
//  * Created by zhofan on 2019/06/11.
//  */
//class TeradataSqlFileSubmitterTest extends FunSuite with BeforeAndAfterAll {
//  test("test - buildSqlStatement") {
//    var params = Map("dummy_entry" -> "dummy")
//    params += ("HDP" -> "/test_path")
//    val statment = TeradataSqlFileSubmitter.buildSqlStatement("dw_mpx_clients.sql", params)
//
//    System.out.println("sql statement: " + statment)
//  }
//
//  test("test - dw_mpx_clients move from teradata to hive rno") {
//    var params = Map("dummy_entry" -> "dummy")
//    params += ("HDP" -> "viewfs://apollo-rno/user/zhofan/rotation/dw_mpx_clients/dw_mpx_clients_")
//
//    TeradataSqlFileSubmitter.execute(TeradataType.Mozart, "dw_mpx_clients.sql", params)
//  }
//}
