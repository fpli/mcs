package com.ebay.traffic.chocolate.sparknrt.utils

import java.time.temporal.ChronoUnit

import net.jodah.failsafe.{Failsafe, Policy, RetryPolicy}
import net.jodah.failsafe.function.CheckedSupplier

object RetryUtil {
  def retry[R](exec: => R): R = try {
    Failsafe
      .`with`[R, Policy[R]](
        new RetryPolicy[R]()
          .handle(classOf[Exception])
          .withMaxRetries(4)
          .withBackoff(5, 40, ChronoUnit.MILLIS)
      )
      .get(new CheckedSupplier[R] {
        def get(): R = {
          val r: R = exec
          r
        }
      })
  }
}
