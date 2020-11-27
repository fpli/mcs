package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.flink.nrt.util.UDF;
import org.junit.Test;

import static org.junit.Assert.*;

public class UDFTest {

  @Test
  public void parseChannelType() {
    String urlquerystring = "/roveropen/0/e12060/7?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060&bu=43886848848&euid=942d35b23ee140b69989083c45abb869";
    assertEquals("7", UDF.parseChannelId(urlquerystring));
  }
}