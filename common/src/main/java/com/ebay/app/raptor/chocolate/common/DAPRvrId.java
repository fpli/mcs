package com.ebay.app.raptor.chocolate.common;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * This class is used to generate rvr_id, which will be passed to DAP.
 * The javascript response returned by DAP define rvr_id as number, we need to generate a number less than the maximum
 * safe integer in JavaScript (2^53 - 1)
 * -- the way this works is:
 * - Representation is a 53 bits
 * - first 42 bits are for system time (max year is 2109)
 * - middle 6 bits for driver (node) ID (max nodes is 63)
 * - end 5 bits for sequence ID (max sequence number is 31)
 */
public class DAPRvrId implements Serializable, Comparable<DAPRvrId> {
  private static final long serialVersionUID = 4054679458793664663L;

  // MAX sequence number 5bits=31
  private static final int MAX_SEQ_NUM = ((1 << 5) - 1);
  private static final long MAX_SEQ_NUM_BITS = Long.toBinaryString(MAX_SEQ_NUM).length();
  // MAX driver id 6bits = 63
  private static final int MAX_DRIVER_ID = ((1 << 6) - 1);
  private static final long MAX_DRIVER_BITS = Long.toBinaryString(MAX_DRIVER_ID).length();
  // MAX timestamp 42bits = 4398046511103
  private static final long MAX_TIMESTAMP = (1L << 42) - 1;
  // Length of driverId + sequenceNumber
  private static final long MAX_DS_BITS = MAX_DRIVER_BITS + MAX_SEQ_NUM_BITS;
  // The representation for this dap rvr id.
  private final long representation;

  private static final long JAVASCRIPT_MAX_NUMBER = (1L << 53) - 1;

  /**
   * Generate a dap rvr id from long snapshotId.
   *
   * @param representation existing long snapshotId
   */
  public DAPRvrId(final long representation) {
    // The representation for original snapshot ID.
    if(representation > JAVASCRIPT_MAX_NUMBER){
      SnapshotId sId = new SnapshotId(representation);
      long epochMilliseconds = sId.getTimeMillis();
      int sequenceNum = sId.getSequenceId();
      if(epochMilliseconds > MAX_TIMESTAMP) {
        epochMilliseconds = MAX_TIMESTAMP & epochMilliseconds;
      }
      if(sequenceNum > MAX_SEQ_NUM) {
        sequenceNum = MAX_SEQ_NUM & sequenceNum;
      }
      this.representation = (epochMilliseconds << MAX_DS_BITS) | (sId.getDriverId() << MAX_SEQ_NUM_BITS) | sequenceNum;
    } else {
      this.representation = representation;
    }
  }

  /**
   * return the raw representation
   */
  public long getRepresentation() {
    return this.representation;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    return ((DAPRvrId) obj).getRepresentation() == representation;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.representation);
  }

  @Override
  public String toString() {
    return "0x" + Long.toHexString(representation);
  }

  @Override
  public int compareTo(DAPRvrId o) {
    if (o == null) return -1;
    return Long.compare(representation, o.representation);
  }
}
