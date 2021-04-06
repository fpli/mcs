package com.ebay.app.raptor.chocolate.common;

import org.junit.Test;

import static org.junit.Assert.*;

public class DAPRvrIdTest {

  @Test
  public void getRepresentation() {
    DAPRvrId dapRvrId = new DAPRvrId(3311371026541824L);
    assertEquals(3311371026541824L, dapRvrId.getRepresentation());

    long timestamp = 1617077445036L;
    int driverId = 5;
    SnapshotId test = new SnapshotId(driverId, timestamp);

    dapRvrId = new DAPRvrId(test.getRepresentation());
    // timestamp
    assertEquals(1617077445036L, dapRvrId.getRepresentation() >> 12);
    // driver id
    assertEquals(5L, (dapRvrId.getRepresentation() >> 5) & (0b1111111));
    // sequence
    assertEquals(0L, dapRvrId.getRepresentation() & 0b11111);
  }

  @Test
  public void getMaxDriverId() {
    assertEquals(127, DAPRvrId.getMaxDriverId());
  }
}