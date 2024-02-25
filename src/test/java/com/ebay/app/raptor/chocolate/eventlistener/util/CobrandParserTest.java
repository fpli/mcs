/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class CobrandParserTest {

  @Test
  public void parse() {
    CobrandParser parser = new CobrandParser();
    assertEquals("0", parser.parse(null, "ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0"));
    assertEquals("7", parser.parse(null, "HTC"));
    assertEquals("7", parser.parse(null, "iPhone"));
    assertEquals("7", parser.parse(null, "Mobile#Safari"));

    assertEquals("7", parser.parse(null, "HTC Mobile#Safari"));
    assertEquals("7", parser.parse(null, "^HTC Mobile#Safari"));

    assertEquals("7", parser.parse(null, "ebayUserAgent/eBayAndroid;6.4.0;Android;10;samsung;a50;CLARO P.R.;1080x2131;2.6"));

    assertEquals("7", parser.parse(null, "Mozilla/5.0 (Linux; Android 10; moto g(7) Build/QPUS30.52-23-4; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/85.0.4183.81 Mobile Safari/537.36 Instagram 156.0.0.26.109 Android (29/10; 480dpi; 1080x2088; motorola; moto g(7); river; qcom; en_US; 240726484)"));
    assertEquals("7",parser.parse(null, "eBayAndroid/6.6.6.18"));
    assertEquals("7", parser.parse(null, "Mozilla/5.0 (iPad; CPU OS 9_3_5 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13G36 Safari/601.1"));

  }
}