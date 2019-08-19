package com.ebay.traffic.chocolate.sparknrt.crabTransform

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.utils.{XID, XIDAccount, XIDMapping, XIDResponse}

class TestXID extends BaseFunSuite{

  test("test XID") {
    val xid1 = XID("xid1", 2)
    val accounts1 = List(XIDAccount("account1", 1L), XIDAccount("account2", 2L))
    val idfas1 = List(XIDAccount("idfa1", 11L), XIDAccount("idfa2", 13L))
    val cguids1 = List(XIDAccount("cguid1", 21), XIDAccount("cguid2", 23L))
    val mapping1 = XIDMapping(Some(xid1), Some(accounts1), Some(idfas1), None, Some(cguids1), None, None, None)

    val xid2 = XID("xid2", 2)
    val accounts2 = List(XIDAccount("account3", 4L))
    val cguids2 = List(XIDAccount("cguid3", 25L), XIDAccount("cguid4", 5L))
    val mapping2 = XIDMapping(Some(xid2), Some(accounts2), None, None, Some(cguids2), None, None, None)

    val response = XIDResponse(Some(List(mapping1, mapping2)))

    val myid = response.toMyID()
    assert(myid.accounts == List("account3", "account2", "account1"))
    assert(myid.idfas == List("idfa2", "idfa1"))
    assert(myid.gadids == List.empty[String])
    assert(myid.cguids == List("cguid3", "cguid2", "cguid1", "cguid4"))

    val none = XIDResponse(None)
    val myid2 = none.toMyID()
    assert(myid2.accounts == List.empty[String])
  }

}
