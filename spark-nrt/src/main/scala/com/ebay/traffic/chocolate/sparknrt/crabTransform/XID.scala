package com.ebay.traffic.chocolate.sparknrt.crabTransform

/**
  * Created by yimhuang on 1/10/17.
  */
object XID extends DefaultJsonProtocol {
  implicit val _format = jsonFormat2(apply)
}

case class XID(id: String, dqScore: Int)

object XIDAccount extends DefaultJsonProtocol {
  implicit val _format = jsonFormat2(apply)
}

case class XIDAccount(id: String, lastSeenTime: Long)

object XIDMapping extends DefaultJsonProtocol {
  implicit val _format = jsonFormat8(apply)
}

case class XIDMapping(
                       xid: Option[XID],
                       accounts: Option[List[XIDAccount]],
                       idfas: Option[List[XIDAccount]],
                       gadids: Option[List[XIDAccount]],
                       cguids: Option[List[XIDAccount]],
                       gdids: Option[List[XIDAccount]],
                       hydras: Option[List[XIDAccount]],
                       adobes: Option[List[XIDAccount]])

object XIDResponse extends DefaultJsonProtocol {
  implicit val _format = jsonFormat1(apply)
}

case class XIDResponse(idMap: Option[List[XIDMapping]]) {
  def toMyID(): MyID = {
    val ids = idMap.getOrElse(List.empty[XIDMapping])
      .foldLeft((Set.empty[XIDAccount], Set.empty[XIDAccount], Set.empty[XIDAccount], Set.empty[XIDAccount])) {
        case (result, current) =>
          val accounts = result._1.++(current.accounts.getOrElse(List.empty[XIDAccount]))
          val idfas = result._2.++(current.idfas.getOrElse(List.empty[XIDAccount]))
          val gadids = result._3.++(current.gadids.getOrElse(List.empty[XIDAccount]))
          val cguids = result._4.++(current.cguids.getOrElse(List.empty[XIDAccount]))
          (accounts, idfas, gadids, cguids)
      }
    // merge ids from different xids and sort them desc by seen time
    MyID(
      ids._1.toList.sortWith(MyID.sortFn).map(_.id),
      //      ids._2.sortWith(MyID.sortFn).map(_.id.toUpperCase()),
      //      ids._3.sortWith(MyID.sortFn).map(_.id.toUpperCase()),
      ids._2.toList.sortWith(MyID.sortFn).map(_.id.toLowerCase),
      ids._3.toList.sortWith(MyID.sortFn).map(_.id.toLowerCase),
      ids._4.toList.sortWith(MyID.sortFn).map(_.id))
  }
}

object MyID extends DefaultJsonProtocol {
  implicit val _format = jsonFormat4(apply)

  def sortFn(left: XIDAccount, right: XIDAccount): Boolean = left.lastSeenTime > right.lastSeenTime
}

case class MyID(accounts: List[String], idfas: List[String], gadids: List[String], cguids: List[String])
