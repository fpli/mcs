package com.ebay.traffic.chocolate.mkttracksvc.util;


import com.ebay.app.raptor.chocolate.constant.MPLXClientEnum;
import com.ebay.kernel.calwrapper.CalActivity;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Rotation ID -- 18 digit number <br/>
 * the way this works is:
 * - Representation is a 60-bit
 * - first 42 bits are for system time
 * - 8 bits for driver ID
 * - 4 bits for sequence ID
 */
public class RotationId implements Serializable, Comparable<RotationId> {
  private static final long serialVersionUID = 1454064713917619360L;

  // Mask 10bits for the sequence ID
  public static final long SEQUENCE_MASK = 0xFl;
  public static final int SEQUENCE_MASK_LENGTH = Long.toBinaryString(SEQUENCE_MASK).length();
  // Maximum driver ID constant.
  public static final long MAX_DRIVER_ID = 0xFFl;
  public static final int MAX_DRIVER_ID_LENGTH = Long.toBinaryString(MAX_DRIVER_ID).length();
  // Mask 8bits for the driver ID
  private static final long DRIVER_MASK = MAX_DRIVER_ID << SEQUENCE_MASK_LENGTH;
  // Mask for the high 42 bits in a timestamp
  private static final long TIME_MASK = 0xFFFFFFl << 42l;
  // Counter to track all incoming session IDs.
  private static final AtomicLong counter = new AtomicLong(0l);
  static RotationId current = null;
  // The representation for this session ID.
  private final long representation;
  // The separator of rotation string
  public static final String HYPHEN = "-";


  /**
   * Generate a session ID from representation.
   *
   * @param representation to initialize using
   */
  public RotationId(final long representation) {
    this.representation = representation;
  }

  /**
   * Generate a new driver ID.
   *
   * @param driverId          to use in generating
   * @param epochMilliseconds to use in generating
   * @pre driver ID must be 255 or less
   */
  public RotationId(final int driverId, final long epochMilliseconds) {
    Validate.isTrue(driverId >= 0 && driverId <= MAX_DRIVER_ID);
    representation = ((epochMilliseconds & ~TIME_MASK) << (MAX_DRIVER_ID_LENGTH + SEQUENCE_MASK_LENGTH)) | (driverId << SEQUENCE_MASK_LENGTH);
  }


  /**
   * Generate new session ID from current time.
   *
   * @param driverId to use in generating
   * @pre driver ID must be 255 or less
   */
  public RotationId(final int driverId) {
    this(driverId, System.currentTimeMillis());
  }

  /**
   * Increments the next session ID from the previous one.
   *
   * @param previous to use in incrementing.
   * @param time     to use in incrementing
   * @pre previous cannot be null
   */

  public RotationId(RotationId previous, int driverId, long time) {
    if (previous.getTimeMillis() >= time) {
      this.representation = ((previous.getRepresentation() & ~DRIVER_MASK) | (driverId << SEQUENCE_MASK_LENGTH)) + 1L;
    } else {
      this.representation = new RotationId(driverId, time).getRepresentation();
    }
  }

  /**
   * Ctor to initialize session ID from a previous one, using a driver ID.
   */
  public RotationId(RotationId previous, int driverId) {
    this(previous, driverId, System.currentTimeMillis());
  }

  /**
   * Factory method to generate the next session ID.
   *
   * @param driverId to use in generation
   * @param time     to use in generation
   */
  public synchronized static RotationId getNext(final int driverId, final long time) {
    current = current == null ? new RotationId(driverId, time) : new RotationId(current, driverId, time);
    counter.incrementAndGet();
    return current;
  }

  /**
   * Factory method to generate the next session ID.
   *
   * @param driverId to use in generation
   */
  public synchronized static RotationId getNext(final int driverId) {
    long time = System.currentTimeMillis();
    return getNext(driverId, time);
  }

  /**
   * Method which will check and clear the atomic counter if it exceeds some threshold. This is meant to be a poor
   * man's partial compare and swap.
   *
   * @param threshold to exceed
   * @return the counter's value. It will be set to 0 if the threshold is exceeded.
   */
  public static synchronized long checkAndClearCounter(long threshold) {
    long state = counter.get(); // Get the current counter.
    if (state >= threshold) counter.set(0l);
    return state;
  }

  /**
   * Generates a session ID for testing purposes only.
   *
   * @param epoch      to use in generating time
   * @param sequenceId to set
   * @param driverId   to set the driver ID with
   */
  public static RotationId generateForUnitTests(final long epoch, final int sequenceId, final int driverId) {
    long representation = ((epoch & ~TIME_MASK) << (MAX_DRIVER_ID_LENGTH + SEQUENCE_MASK_LENGTH)) | (driverId << SEQUENCE_MASK_LENGTH) | sequenceId;
    return new RotationId(representation);
  }

  /**
   * @return the current counter value.
   */
  public static long getCounter() {
    return counter.get();
  }

  /**
   * @return the time represented by this entry
   */
  public long getTimeMillis() {
    return representation >>> 12L;
  }

  /**
   * @return the driver ID represented by this entry
   */
  public int getDriverId() { return (int) ((representation & DRIVER_MASK) >>> 4L);}

  /**
   * @return the sequence ID represented by this entry
   */
  public int getSequenceId() { return (int) (representation & SEQUENCE_MASK); }

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
    return ((RotationId) obj).getRepresentation() == representation;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.representation);
  }

  @Override
  public String toString() {
    return "0x" + Long.toHexString(representation);
  }

  /**
   * @return Human-readable debug string
   */
  public String toDebugString() {
    StringBuilder sb = new StringBuilder();
    sb.append("rotation_id: [timestamp=").append(new Timestamp(getTimeMillis()).toString()).append(" driver=")
        .append(getDriverId()).append(" sequence=").append(getSequenceId()).append(']');
    return sb.toString();
  }

  @Override
  public int compareTo(RotationId o) {
    if (o == null) return -1;
    return Long.compare(representation ^ 0x8000000000000000L, o.representation ^ 0x8000000000000000L);
  }

  /**
   * Get Rotation String for legacy system with 4 split format like : 707-xxx-xxx-xxx
   * @param clientId MPLX ClientId or eBay SiteId
   * @return
   */
  public String getRotationStr(Integer clientId, Long campaignId){

    String ridStr = null;
    if(campaignId == null || campaignId < 0){
      ridStr = String.valueOf(this.getRepresentation());
      ridStr = clientId + HYPHEN + ridStr.substring(0, 6) + HYPHEN + ridStr.substring(6, 12) + HYPHEN + ridStr.substring(12);
    }else{
      ridStr = String.valueOf(this.getTimeMillis());
      ridStr = clientId + HYPHEN + campaignId + HYPHEN + ridStr.substring(0, 6) + HYPHEN + ridStr.substring(6);
    }

    return ridStr;
  }
}