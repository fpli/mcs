package com.ebay.app.raptor.chocolate.common;

import com.google.common.base.Objects;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DAPRvrIdTest {
  private DAPRvrId smallDapRvrId;
  private DAPRvrId largeDapRvrId;

  @Before
  public void setUp() throws Exception {
    smallDapRvrId = new DAPRvrId(2281638794639L);
    largeDapRvrId = new DAPRvrId(204730323015062656L);
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void getRepresentation() {
    assertEquals(2281638794639L, smallDapRvrId.getRepresentation());
    assertEquals(99965978042528L, largeDapRvrId.getRepresentation());
  }

  @Test
  public void testEquals() {
    assertFalse(smallDapRvrId.equals(null));
    assertFalse(smallDapRvrId.equals(Integer.valueOf(1)));
    assertFalse(smallDapRvrId.equals(new DAPRvrId(228163879463L)));
    assertTrue(smallDapRvrId.equals(new DAPRvrId(2281638794639L)));
  }

  @Test
  public void testHashCode() {
    assertNotEquals(1, smallDapRvrId.hashCode());
    assertEquals(Objects.hashCode(2281638794639L), smallDapRvrId.hashCode());
  }

  @Test
  public void testToString() {
    assertEquals("0x" + Long.toHexString(2281638794639L), smallDapRvrId.toString());
  }

  @Test
  public void compareTo() {
    assertEquals(-1, smallDapRvrId.compareTo(null));
    assertEquals(Long.compare(2281638794639L, 228163879463L), smallDapRvrId.compareTo(new DAPRvrId(228163879463L)));
  }
}