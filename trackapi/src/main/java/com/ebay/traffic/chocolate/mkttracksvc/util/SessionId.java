package com.ebay.traffic.chocolate.mkttracksvc.util;


import org.apache.commons.lang3.Validate;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Session ID -- the way this works is: - Representation is a 64-bit - first 42 bits are for system time (lower 40 of an epoch) - 8
 * bits for driver ID - 14 bits for sequence ID
 */
public class SessionId implements Serializable, Comparable<SessionId> {
  /**
   * Maximum driver ID constant.
   */
  public static final long MAX_DRIVER_ID = 0xFFl;
  // Spark entry representation
  private static final long serialVersionUID = -8743871640982700055L;
  // Counter to track all incoming session IDs.
  private static final AtomicLong counter = new AtomicLong(0l);

  // Mask for the high 42 bits in a timestamp
  private static final long TIME_MASK = 0xFFFFFFl << 42l;
  // Mask 8bits for the driver ID
  private static final long DRIVER_MASK = MAX_DRIVER_ID << 14l;
  // Mask 14bits for the sequence ID
  @SuppressWarnings("unused")
  private static final long SEQUENCE_MASK = 0x3FFFl;
  // Current session ID. Used in factory generation
  static SessionId current = null;
  // The representation for this session ID.
  private final long representation;
  // The default driverID
  public static final int DEFAULT_DRIVER_ID = DriverId.getDriverIdFromIp();

  /**
   * Generate a session ID from representation.
   *
   * @param representation to initialize using
   */
  public SessionId(final long representation) {
    this.representation = representation;
  }

  /**
   * Generate a new driver ID.
   *
   * @param driverId          to use in generating
   * @param epochMilliseconds to use in generating
   * @pre driver ID must be 255 or less
   */
  public SessionId(final int driverId, final long epochMilliseconds) {
    Validate.isTrue(driverId >= 0 && driverId <= MAX_DRIVER_ID);
    representation = ((epochMilliseconds & ~TIME_MASK) << 22l) | (driverId << 14l);
  }


  /**
   * Generate new session ID from current time.
   *
   * @param driverId to use in generating
   * @pre driver ID must be 255 or less
   */
  public SessionId(final int driverId) {
    this(driverId, System.currentTimeMillis());
  }

  /**
   * Increments the next session ID from the previous one.
   *
   * @param previous to use in incrementing.
   * @param time     to use in incrementing
   * @pre previous cannot be null
   */

  public SessionId(SessionId previous, int driverId, long time) {
    if (previous.getTimeMillis() >= time) {
      this.representation = ((previous.getRepresentation() & ~DRIVER_MASK) | (driverId << 14l)) + 1l;
    } else {
      this.representation = new SessionId(driverId, time).getRepresentation();
    }
  }

  /**
   * Ctor to initialize session ID from a previous one, using a driver ID.
   */
  public SessionId(SessionId previous, int driverId) {
    this(previous, driverId, System.currentTimeMillis());
  }

  /**
   * Factory method to generate the next session ID.
   *
   * @param driverId to use in generation
   * @param time     to use in generation
   */
  public synchronized static SessionId getNext(final int driverId, final long time) {
    current = current == null ? new SessionId(driverId, time) : new SessionId(current, driverId, time);
    counter.incrementAndGet();
    return current;
  }

  /**
   * Factory method to generate the next session ID.
   *
   * @param driverId to use in generation
   */
  public synchronized static SessionId getNext(final int driverId) {
    long time = System.currentTimeMillis();
    return getNext(driverId, time);
  }

  /**
   * Factory method to generate the next session ID.
   *
   */
  public synchronized static SessionId getNext() {
    long time = System.currentTimeMillis();
    return getNext(DEFAULT_DRIVER_ID, time);
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
  public static SessionId generateForUnitTests(final long epoch, final int sequenceId, final int driverId) {
    long representation = ((epoch & ~TIME_MASK) << 22l) | (driverId << 14l) | sequenceId;
    return new SessionId(representation);
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
    return representation >>> 22l;
  }

  /**
   * @return the driver ID represented by this entry
   */
  public int getDriverId() {
    return (int) ((representation & DRIVER_MASK) >>> 14l);
  }

  /**
   * @return the sequence ID represented by this entry
   */
  public int getSequenceId() {
    return (int) (representation & 0x3FFFl);
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
    return ((SessionId) obj).getRepresentation() == representation;
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
    sb.append("session[timestamp=").append(new Timestamp(getTimeMillis()).toString()).append(" driver=")
        .append(getDriverId()).append(" sequence=").append(getSequenceId()).append(']');
    return sb.toString();
  }

  @Override
  public int compareTo(SessionId o) {
    if (o == null) return -1;
    return Long.compare(representation ^ 0x8000000000000000L, o.representation ^ 0x8000000000000000L);
  }
}