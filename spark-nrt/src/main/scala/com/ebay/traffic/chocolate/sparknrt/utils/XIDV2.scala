package com.ebay.traffic.chocolate.sparknrt.utils

import spray.json.DefaultJsonProtocol

object XIDV2 extends DefaultJsonProtocol {
  implicit val _format = jsonFormat2(apply)
}

case class
XIDV2(id: String, dqScore: Int)

object XIDAccountV2 extends DefaultJsonProtocol {
  implicit val _format = jsonFormat2(apply)
}

case class XIDAccountV2(id: String, lastSeenTime: Long)

object XIDMappingV2 extends DefaultJsonProtocol {
  implicit val _format = jsonFormat9(apply)
}

case class XIDMappingV2(
                         xid: Option[XIDV2],
                         accounts: Option[List[XIDAccountV2]],
                         idfas: Option[List[XIDAccountV2]],
                         gadids: Option[List[XIDAccountV2]],
                         cguids: Option[List[XIDAccountV2]],
                         gdids: Option[List[XIDAccountV2]],
                         hydras: Option[List[XIDAccountV2]],
                         adobes: Option[List[XIDAccountV2]],
                         pguids: Option[List[XIDAccountV2]])

object XIDResponseV2 extends DefaultJsonProtocol {
  implicit val _format = jsonFormat1(apply)
}

case class XIDResponseV2(idMap: Option[List[XIDMappingV2]]) {
  def toMyIDV2: MyIDV2 = {
    val ids = idMap.getOrElse(List.empty[XIDMappingV2])
      .foldLeft((Set.empty[XIDAccountV2], Set.empty[XIDAccountV2], Set.empty[XIDAccountV2], Set.empty[XIDAccountV2], Set.empty[XIDAccountV2])) {
        case (result, current) =>
          val accounts = result._1.++(current.accounts.getOrElse(List.empty[XIDAccountV2]))
          val idfas = result._2.++(current.idfas.getOrElse(List.empty[XIDAccountV2]))
          val gadids = result._3.++(current.gadids.getOrElse(List.empty[XIDAccountV2]))
          val cguids = result._4.++(current.cguids.getOrElse(List.empty[XIDAccountV2]))
          val pguids = result._5.++(current.pguids.getOrElse(List.empty[XIDAccountV2]))
          (accounts, idfas, gadids, cguids, pguids)
      }
    // merge ids from different xids and sort them desc by seen time
    MyIDV2(
      ids._1.toList.sortWith(MyIDV2.sortFn).map(_.id),
      ids._2.toList.sortWith(MyIDV2.sortFn).map(_.id.toLowerCase),
      ids._3.toList.sortWith(MyIDV2.sortFn).map(_.id.toLowerCase),
      ids._4.toList.sortWith(MyIDV2.sortFn).map(_.id),
      ids._5.toList.sortWith(MyIDV2.sortFn).map(_.id))
  }
}

object MyIDV2 extends DefaultJsonProtocol {
  implicit val _format = jsonFormat5(apply)

  def sortFn(left: XIDAccountV2, right: XIDAccountV2): Boolean = left.lastSeenTime > right.lastSeenTime
}

case class MyIDV2(accounts: List[String], idfas: List[String], gadids: List[String], cguids: List[String], pguids: List[String])
