package com.ebay.app.raptor.chocolate.common;

import com.google.common.base.Objects;
import org.apache.commons.lang3.Validate;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Snapshot ID -- the way this works is: - Representation is a 64-bit - first 40 bits are for system time (lower 40 of an epoch) - 10
 * bits for driver (node) ID - 14 bits for sequence ID
 */
public class SnapshotId implements Serializable, Comparable<SnapshotId> {
    // Spark entry representation
    private static final long serialVersionUID = -8743871640982700055L;

    // Current snapshot ID. Used in factory generation
    static SnapshotId current = null;

    // Counter to track all incoming snapshot IDs.
    private static final AtomicLong counter = new AtomicLong(0l);

    // Mask for the high 42 bits in a timestamp
    private static final long TIME_MASK = 0xFFFFFFl << 42l;

    /** Maximum driver ID constant. */
    public static final long MAX_DRIVER_ID = 0x3FFl;

    // Mask 10bits for the driver ID
    private static final long DRIVER_MASK = MAX_DRIVER_ID << 12l;

    // Mask 12bits for the sequence ID
    @SuppressWarnings("unused")
    private static final long SEQUENCE_MASK = 0xFFFl;

    // The representation for this snapshot ID.
    private final long representation;

    /**
     * Factory method to generate the next snapshot ID.
     *
     * @param driverId to use in generation
     * @param time to use in generation
     */
    public synchronized static SnapshotId getNext(final int driverId, final long time) {
        current = current == null ? new SnapshotId(driverId, time) : new SnapshotId(current, driverId, time);
        counter.incrementAndGet();
        return current;
    }
    /**
     * Factory method to generate the next snapshot ID.
     * 
     * @param driverId to use in generation
     */
    public synchronized static SnapshotId getNext(final int driverId) {
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
     * Generates a snapshot ID for testing purposes only.
     * 
     * @param epoch to use in generating time
     * @param sequenceId to set
     * @param driverId to set the driver ID with
     */
    public static SnapshotId generateForUnitTests(final long epoch, final int sequenceId, final int driverId) {
        long representation = ((epoch & ~TIME_MASK) << 22l) | (driverId << 12l) | sequenceId;
        return new SnapshotId(representation);
    }

    /** @return the current counter value. */
    public static long getCounter() {
        return counter.get();
    }

    /**
     * Generate a snapshot ID from representation.
     * 
     * @param representation to initialize using
     */
    public SnapshotId(final long representation) {
        this.representation = representation;
    }

    /**
     * Generate a new driver ID.
     * 
     * @pre driver ID must be 255 or less
     * @param driverId to use in generating
     * @param epochMilliseconds to use in generating
     */
    public SnapshotId(final int driverId, final long epochMilliseconds) {
        Validate.isTrue(driverId >= 0 && driverId <= MAX_DRIVER_ID);
        representation = ((epochMilliseconds & ~TIME_MASK) << 22l) | (driverId << 12l);
    }

    /**
     * Generate new snapshot ID from current time. 
     * 
     * @pre driver ID must be 255 or less
     * @param driverId to use in generating
     */
    public SnapshotId(final int driverId) {
        this(driverId, System.currentTimeMillis());
    }
    
    /**
     * Increments the next snapshot ID from the previous one.
     * 
     * @pre previous cannot be null
     * @param previous to use in incrementing.
     * @param time to use in incrementing
     */

    public SnapshotId(SnapshotId previous, int driverId, long time) {
        if (previous.getTimeMillis() >= time) {
            this.representation = ((previous.getRepresentation() & ~DRIVER_MASK) | (driverId << 12l)) + 1l;
        } else {
            this.representation = new SnapshotId(driverId, time).getRepresentation();
        }
    }

    /** Ctor to initialize snapshot ID from a previous one, using a driver ID. */
    public SnapshotId(SnapshotId previous, int driverId){
        this(previous, driverId, System.currentTimeMillis());
    }

    /** @return the time represented by this entry */
    public long getTimeMillis() {
        return representation >>> 22l;
    }

    /** @return the driver ID represented by this entry */
    public int getDriverId() { return (int) ((representation & DRIVER_MASK) >>> 12l); }

    /** @return the sequence ID represented by this entry */
    public int getSequenceId() { return (int) (representation & 0xFFFl); }

    /** return the raw representation */
    public long getRepresentation() {
        return this.representation;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj.getClass() != this.getClass()) return false;
        return ((SnapshotId) obj).getRepresentation() == representation;
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
        sb.append("snapshot[timestamp=").append(new Timestamp(getTimeMillis()).toString()).append(" driver=")
                .append(getDriverId()).append(" sequence=").append(getSequenceId()).append(']');
        return sb.toString();
    }

    @Override
    public int compareTo(SnapshotId o) {
        if (o == null) return -1;
        return Long.compare(representation ^ 0x8000000000000000L, o.representation ^ 0x8000000000000000L);
    }
}
