package com.ebay.traffic.chocolate.cappingrules;

import com.ebay.app.raptor.chocolate.common.SnapshotId;
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
   * Date formatters to format dates from HBase to Cassandra-schema-compliant format
   */
  public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
  public static final DateFormat MONTH_FORMAT = new SimpleDateFormat("yyyyMM");

  public static long getSnapshotId(long epochMilliseconds, int driverId) {
    SnapshotId snapshotId = new SnapshotId(driverId, epochMilliseconds);
    return snapshotId.getRepresentation();
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
    return snapshotId >>> 22l;
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
    SnapshotId snapshotId = new SnapshotId(0, timestamp);
    byte[] snapshotID = Bytes.toBytes(snapshotId.getRepresentation());
    
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
