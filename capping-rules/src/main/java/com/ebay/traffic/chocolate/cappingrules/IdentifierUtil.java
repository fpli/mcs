package com.ebay.traffic.chocolate.cappingrules;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by yimeng on 11/18/17.
 */
public class IdentifierUtil {
  
  /**
   * Maximum driver ID constant
   */
  public static final long MAX_DRIVER_ID = 0x3FFl;
  /**
   * Mask for the high 24 bits in a timestamp
   */
  public static final long TIME_MASK = 0xFFFFFFl << 40l;
  /**
   * Date formatters to format dates from HBase to Cassandra-schema-compliant format
   */
  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
  public static final DateFormat MONTH_FORMAT = new SimpleDateFormat("yyyyMM");
  protected static final long HIGH_24 = 0x10000000000l;
  
  public static long getSnapshotId(long epochMilliseconds, int driverId) {
    Validate.isTrue(driverId >= 0 && driverId <= MAX_DRIVER_ID);
    return ((epochMilliseconds & ~TIME_MASK) << 24l) | (driverId << 14l);
  }
  
  public static byte[] generateIdentifier(long timestamp, int driverId, short modValue) throws IOException {
    ByteBuffer bufferStart = ByteBuffer.allocate(Short.BYTES);
    bufferStart.putShort(modValue);
    
    ByteArrayOutputStream stream = new ByteArrayOutputStream(10);
    stream.write(bufferStart.array());
    
    byte[] snapshotID = Bytes.toBytes(getSnapshotId(timestamp, driverId));
    stream.write(snapshotID);
    return ByteBuffer.wrap(stream.toByteArray()).array();
  }
  
  public static long getTimeMillisForSnapshotId(long snapshotId) {
    return snapshotId >>> 24l | HIGH_24;
  }
  
  public static long getTimeMillisFromRowkey(byte[] rowIdentifier) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream(10);
    for (int i = 0; i < rowIdentifier.length; i++) {
      if (i > 1) {
        stream.write(rowIdentifier[i]);
      }
    }
    long snapshotId = Bytes.toLong(stream.toByteArray());
    return getTimeMillisForSnapshotId(snapshotId);
  }
  
  /**
   * Generate 10 bytes row key
   *
   * @param timestamp timestamp
   * @param modValue  slice value
   * @return row key byte array
   */
  public static byte[] generateIdentifier(long timestamp, short modValue) throws IOException {
    byte[] snapshotID = Bytes.toBytes((timestamp & ~TIME_MASK) << 24l);
    
    java.io.ByteArrayOutputStream streamStart = new java.io.ByteArrayOutputStream(10);
    ByteBuffer bufferStart = ByteBuffer.allocate(Short.BYTES);
    bufferStart.putShort(modValue);
    streamStart.write(bufferStart.array());
    streamStart.write(snapshotID);
    
    byte[] identifier = ByteBuffer.wrap(streamStart.toByteArray()).array();
    return identifier;
  }
  
  public static int getMonthFromSnapshotId(long snapshotId) {
    return Integer.valueOf(MONTH_FORMAT.format(new Date(getTimeMillisForSnapshotId(snapshotId))));
  }
  
  public static int getDayFromSnapshotId(long snapshotId) {
    return Integer.valueOf(DATE_FORMAT.format(new Date(getTimeMillisForSnapshotId(snapshotId))));
  }
}
