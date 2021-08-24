package com.ebay.traffic.chocolate.sparknrt.epnnrtV2

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

object RetryUtil {

  private val className = getClass.getSimpleName

  @tailrec
  def retry[A](times: Int,delay: Long)(any: => A, anyFail: => A = null): A = {
    Try(any) match {
      case Success(v) =>
        println(s"[$className] Success: left $times times")
        v
      case Failure(e) =>
        println(s"[$className] Failure: left $times times")
        anyFail
        if (times > 0) {
          println(e)
          Thread.sleep(delay)
          retry(times - 1, delay)(any, anyFail)
        } else throw e
    }
  }
}