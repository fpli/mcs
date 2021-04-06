package com.ebay.traffic.chocolate.sparknrt.imkETLV2

import com.ebay.traffic.chocolate.sparknrt.imkDump.Tools
import org.scalatest.FunSuite

class ToolsTest extends FunSuite {
  val tools = new Tools("chocolate", "ingress.sherlock-service.svc.33.tess.io/api/pushgateway/v1/",
    "urn:ebay-marketplace-consumerid:ced0fa0f-d68b-4a32-8586-b8e6d94c96a3", "ut")

  test("testJudgeNotBot") {
    assert(!tools.judgeNotBot("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"))
    assert(!tools.judgeNotBot("Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.112 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"))
  }

}
