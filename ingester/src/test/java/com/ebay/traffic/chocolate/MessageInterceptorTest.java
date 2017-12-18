package com.ebay.traffic.chocolate;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MessageInterceptorTest {

  @Test
  public void testGetTimestamp() {
    String message = "{\"snapshot_id\":6917529028792138877,\"timestamp\":1512449976346,\"publisher_id\":-1," +
      "\"campaign_id\":5338195018,\"request_headers\":\"User-Agent: Mozilla/5.0 (compatible; um-LN/1.0; mailto: " +
      "techinfo@ubermetrics-technologies.com)|Connection: keep-alive|Host: rover.ebay.com|Accept: text/html, " +
      "image/gif, image/jpeg, *; q=.2, */*; q=.2|X-eBay-Client-IP: 88.198.44.37|X-eBay-Chocolis-IP: 10.43.240.14\"," +
      "\"uri\":\"http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item" +
      "=263365814851&vectorid=229466&lgeo=1&utm_source=dlvr.it&utm_medium=twitter\"," +
      "\"response_headers\":\"X-EBAY-C-REQUEST-ID: ri=DDthxqAlYQmx,rci=qd4OPr3S138vQr9b|Cache-Control: private," +
      "no-cache,no-store|Server: ebay server|Set-Cookie: " +
      "npii=btguid/250b28271600a9c48b659c21f569a4545c075b38^trm/svid%3D1119041641291672935c075b38^tpim/15a2627f4" +
      "^cguid/250b28271600a9c48b659c21f569a4525c075b38^;Domain=.ebay.com;Expires=Wed, 05-Dec-2018 04:59:36 GMT;" +
      "Path=/|Pragma: no-cache|P3P: policyref=\\\"/w3c/p3p.xml\\\", CP=\\\"NOI CURa ADMa DEVa PSDo PSAa OUR SAMo IND " +
      "UNI COM NAV INT STA DEM PRE\\\"|Content-Length: 0|Date: Tue, 05 Dec 2017 04:59:36 GMT|RlogId: " +
      "t6qjpbq%3F%3Ctofthu%60t*274676%3F%29pqtfwpu%29pie%29fgg%7E-fij-160250b2828-0x154|X-EBAY-CHOCOLATE: " +
      "true|Location: http://www.ebay.com/itm/like/263365814851?lgeo=1&vectorid=229466&utm_medium=twitter&utm_source" +
      "=dlvr.it&item=263365814851&rmvSB=true%7CContent-Type: image/gif\",\"valid\":{\"boolean\":false}," +
      "\"filter_failed\":{\"string\":\"VALID_PUBLISHER\"},\"channel_action\":\"CLICK\",\"channel_type\":\"EPN\"," +
      "\"http_method\":\"HEAD\",\"snid\":\"\",\"is_tracked\":false}";

    byte[] messageBytes = message.getBytes();
    String timestamp = new MessageInterceptor().getTimestamp(messageBytes);
    assertEquals("1512449976346", timestamp);
  }
}
