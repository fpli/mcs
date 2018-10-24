package com.ebay.app.raptor.chocolate.common;

import com.google.common.base.Objects;
import org.apache.commons.lang3.Validate;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ShortSnapshot ID -- the way this works is:
 * - Representation is a 59 bits
 * - first 42 bits are for system time (max year is 2109)
 * - middle 10 bits for driver (node) ID (max nodes is 1023)
 * - end 7 bits for sequence ID (max sequence number is 127)
 */
public class ShortSnapshotId implements Serializable, Comparable<ShortSnapshotId> {
  private static final long serialVersionUID = 4054679458793664663L;

  // MAX sequence number 7bits=127
  public static final int MAX_SEQ_NUM = 0x7F;
  private static final long MAX_SEQ_NUM_BITS = Long.toBinaryString(MAX_SEQ_NUM).length();
  // MAX driver id 10bits = 1023
  public static final int MAX_DRIVER_ID = 0x3FF;
  private static final long MAX_DRIVER_BITS = Long.toBinaryString(MAX_DRIVER_ID).length();
  // MAX timestamp 42bits = 4398046511103
  public static final long MAX_TIMESTAMP = 0x3FFFFFFFFFFl;
  // Length of driverId + sequenceNumber
  private static final long MAX_DS_BITS = MAX_DRIVER_BITS + MAX_SEQ_NUM_BITS;
  private static final long DRIVER_MASK = MAX_DRIVER_ID << MAX_SEQ_NUM_BITS;
  // Mask for the high 42 bits in a timestamp
  private static final long TIME_MASK = 0xFFFFFFl << 42l;
  // The representation for this short snapshot ID.
  private final long representation;

  // The representation for original snapshot ID.
  private final long originalRepresentation;

  public long getOriginalRepresentation() {
    return originalRepresentation;
  }

  /**
   * Generate a short snapshotId from long snapshotId.
   *
   * @param representation existing long snapshotId
   */
  public ShortSnapshotId(final long representation) {
    if(representation > 999999999999999999l){
      SnapshotId sId = new SnapshotId(representation);
      originalRepresentation = sId.getRepresentation();
      Validate.isTrue(sId.getDriverId() >= 0 && sId.getDriverId() <= MAX_DRIVER_ID);
      long epochMilliseconds = sId.getTimeMillis();
      int sequenceNum = sId.getSequenceId();
      if(epochMilliseconds > MAX_TIMESTAMP) epochMilliseconds = MAX_TIMESTAMP & epochMilliseconds;
      if(sequenceNum > MAX_SEQ_NUM) epochMilliseconds = MAX_SEQ_NUM & sequenceNum;
      this.representation = (epochMilliseconds << MAX_DS_BITS) | (sId.getDriverId() << MAX_SEQ_NUM_BITS) | sequenceNum;
    }else{
      this.representation = representation;
      originalRepresentation = ((getTimeMillis() & ~TIME_MASK) << 22l) | (getDriverId() << 12l) | getSequenceId();
    }
  }

  /**
   * @return the time represented by this entry
   */
  public long getTimeMillis() {
    return representation >>> MAX_DS_BITS;
  }

  /**
   * @return the driver ID represented by this entry
   */
  public int getDriverId() {
    return (int) ((representation & DRIVER_MASK) >>> MAX_SEQ_NUM_BITS);
  }

  /**
   * @return the sequence ID represented by this entry
   */
  public int getSequenceId() {
    return (int) (representation & MAX_SEQ_NUM);
  }

  /**
   * return the raw representation
   */
  public long getRepresentation() {
    return this.representation;
  }

  /**
   * @return Human-readable debug string
   */
  public String toDebugString() {
    StringBuilder sb = new StringBuilder();
    sb.append("snapshot[timestamp=").append(new Timestamp(getTimeMillis()).toString()).append(" driver=")
        .append(getDriverId()).append(" sequence=").append(getSequenceId()).append(']');
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj.getClass() != this.getClass()) return false;
    return ((ShortSnapshotId) obj).getRepresentation() == representation;
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
  public int compareTo(ShortSnapshotId o) {
    if (o == null) return -1;
    return Long.compare(representation, o.representation);
  }
}
