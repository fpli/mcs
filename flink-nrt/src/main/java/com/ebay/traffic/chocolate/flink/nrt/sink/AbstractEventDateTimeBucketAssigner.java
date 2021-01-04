package com.ebay.traffic.chocolate.flink.nrt.sink;

import com.ebay.traffic.chocolate.flink.nrt.constant.DateConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Assigns to buckets based on event timestamp.
 *
 * <p>The {@code CustomEventDateTimeBucketAssigner} will create directories of the following form:
 * {@code /{basePath}/{dateTimePath}/{hourPath}. The {@code basePath} is the path
 * that was specified as a base path when creating the
 * {@link StreamingFileSink }.
 * The {@code dateTimePath} and {hourPath} is determined based on the event timestamp.
 *
 *
 * <p>This will create for example the following bucket path:
 * {@code /base/dt=1976-12-31/hour=01}
 */
public abstract class AbstractEventDateTimeBucketAssigner<IN> implements BucketAssigner<IN, String> {
  public static final DateTimeFormatter EVENT_DT_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD).withZone(ZoneId.systemDefault());
  public static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern(DateConstants.HH).withZone(ZoneId.systemDefault());

  @Override
  public String getBucketId(IN element, Context context) {
    long eventTs = getEventTimestamp(element);
    Instant instant = Instant.ofEpochMilli(eventTs);
    String eventDt = EVENT_DT_FORMATTER.format(instant);
    String hour = HOUR_FORMATTER.format(instant);
    return StringConstants.DATE_BUCKET_PREFIX + eventDt + StringConstants.SLASH + StringConstants.HOUR_BUCKET_PREFIX + hour;
  }

  /**
   * Get timestamp based on the record
   * @param element record
   * @return timestamp
   */
  protected abstract long getEventTimestamp(IN element);

  @Override
  public SimpleVersionedSerializer<String> getSerializer() {
    return SimpleVersionedStringSerializer.INSTANCE;
  }
}
