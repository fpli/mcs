package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.BehaviorEvent;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.function.DailyDoneSinkFunction;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetMapInputFormat;
import org.apache.flink.formats.parquet.ParquetRowInputFormat;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.apache.parquet.avro.AvroSchemaConverter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Monitor Behavior event sink files and generate daily done files.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class BehaviorEventDailyDoneApp {
  public static final String EVENTTIMESTAMP = "eventtimestamp";
  public static final String STATE_NAME = "behavior-event-current-daily-done";
  protected StreamExecutionEnvironment streamExecutionEnvironment;

  private static final long DEFAULT_CHECK_POINT_PERIOD = TimeUnit.MINUTES.toMillis(10);

  private static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS = TimeUnit.SECONDS.toMillis(30);

  private static final long DEFAULT_CHECK_POINT_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

  private static final Time DEFAULT_PROCESSING_TIME_WINDOWS = Time.minutes(5);

  private static final int DEFAULT_MAX_CONCURRENT_CHECK_POINTS = 1;

  public static void main(String[] args) throws Exception {
    BehaviorEventDailyDoneApp transformApp = new BehaviorEventDailyDoneApp();
    transformApp.run();
  }

  @SuppressWarnings("rawtypes")
  private static LocalDateTime toLocalDateTime(Map value) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) value.get(EVENTTIMESTAMP)), ZoneId.systemDefault());
  }

  private static LocalDateTime compare(LocalDateTime value1, LocalDateTime value2) {
    return value1.isBefore(value2) ? value1 : value2;
  }

  @SuppressWarnings("rawtypes")
  protected void run() throws Exception {
    Properties properties = PropertyMgr.getInstance().loadProperty(PropertyConstants.BEHAVIOR_EVENT_DAILY_DONE_APP_HDFS_PROPERTIES);
    String dataPath = properties.getProperty(PropertyConstants.DATA_PATH);
    String donePath = properties.getProperty(PropertyConstants.DONE_PATH);
    String doneFilePrefix = properties.getProperty(PropertyConstants.DONE_FILE_PREFIX);
    String doneFileSuffix = properties.getProperty(PropertyConstants.DONE_FILE_SUFFIX);

    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    prepareBaseExecutionEnvironment();
    ParquetRowInputFormat parquetRowInputFormat = new ParquetRowInputFormat(new Path(dataPath), new AvroSchemaConverter().convert(BehaviorEvent.getClassSchema()));
    parquetRowInputFormat.setNestedFileEnumeration(true);

    DataStream<Row> inputStream = streamExecutionEnvironment.readFile(parquetRowInputFormat, dataPath, FileProcessingMode.PROCESS_CONTINUOUSLY, TimeUnit.MINUTES.toMillis(5));

    inputStream.map((MapFunction<Row, LocalDateTime>) value -> {
      int pos = BehaviorEvent.getClassSchema().getField(EVENTTIMESTAMP).pos();
      return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) value.getField(pos)), ZoneId.systemDefault());
    })
            .windowAll(TumblingProcessingTimeWindows.of(DEFAULT_PROCESSING_TIME_WINDOWS))
            .reduce((ReduceFunction<LocalDateTime>) (value1, value2) -> {
              return value1.isBefore(value2) ? value1 : value2;
            })
            .addSink(new DailyDoneSinkFunction(STATE_NAME, donePath, doneFilePrefix, doneFileSuffix));

    streamExecutionEnvironment.execute(this.getClass().getSimpleName());
  }

  protected void prepareBaseExecutionEnvironment() {
    streamExecutionEnvironment.enableCheckpointing(DEFAULT_CHECK_POINT_PERIOD);
    streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(DEFAULT_CHECK_POINT_TIMEOUT);
    streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(DEFAULT_MAX_CONCURRENT_CHECK_POINTS);
    streamExecutionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
  }

}
