package com.ebay.traffic.chocolate.mkttracksvc.util;

import com.ebay.traffic.chocolate.mkttracksvc.util.DriverId;
import com.ebay.traffic.chocolate.mkttracksvc.util.SessionId;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author jialili
 */
@SuppressWarnings("javadoc")
public class DriverIdTest {

  @Test(expected = IllegalArgumentException.class)
  public void testGetDriverIdNullHostname() {
    DriverId.getDriverIdFromIp(null, 12l);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetDriverIdBadHostname() {
    DriverId.getDriverIdFromIp(" ", 12l);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetDriverIdBadIp() {
    DriverId.getDriverIdFromIp("localhost", 0l);
  }

  @Test
  public void testGetDriverIdFallbackToDecimal() {
    final int driverIdRange = Long.valueOf(SessionId.MAX_DRIVER_ID).intValue() + 1;
    boolean[] found = new boolean[driverIdRange];
    Arrays.fill(found, false);

    for (int i = 1; i < driverIdRange * 3; ++i) {
      int parsed = DriverId.getDriverIdFromIp("localhost", i);
      assertEquals(i % driverIdRange, parsed);
      if (i > driverIdRange) {
        assertTrue(found[i % driverIdRange]);
      } else {
        found[i % driverIdRange] = true;
      }

      // Invalid number sequence should still fallback
      assertEquals(parsed, DriverId.getDriverIdFromIp("localhost-12389471298347198327491874", i));
    }
  }

  @Test
  public void testDriverIdParseFromHost() {
    // String positions
    final int driverIdRange = Long.valueOf(SessionId.MAX_DRIVER_ID).intValue() + 1;
    //System.out.println(DriverId.getDriverIdFromIp("7777-localhost", 1l));
    assertEquals(97, DriverId.getDriverIdFromIp("7777-localhost", 1l));
    assertEquals(97, DriverId.getDriverIdFromIp("7777", 1l));
    assertEquals(97, DriverId.getDriverIdFromIp("localhost-7777-localhost", 1l));
    assertEquals(97, DriverId.getDriverIdFromIp("localhost7777", 1l));
    assertEquals(97, DriverId.getDriverIdFromIp("localhost1-7777", 1l));
    assertEquals(97, DriverId.getDriverIdFromIp("localhost7777-1", 1l));

    // Have a mixture of numbers and sequencing
    assertEquals(23, DriverId.getDriverIdFromIp("localhost7777-1-93423421142342423l-23423421423142342423", 1l));
    assertEquals(23, DriverId.getDriverIdFromIp("localhost93423421142342423-7777-1-l-23423421423142342423", 1l));
    assertEquals(23, DriverId.getDriverIdFromIp("localhost7777-1-23423421423142342423-93423421142342423", 1l));
    assertEquals(23, DriverId.getDriverIdFromIp("localhost7777-93423421142342423-1-93423421142342423l-23423421423142342423", 1l));
    assertEquals(23, DriverId.getDriverIdFromIp("93423421142342423localhost7777-1-23423421423142342423", 1l));

  }
}
