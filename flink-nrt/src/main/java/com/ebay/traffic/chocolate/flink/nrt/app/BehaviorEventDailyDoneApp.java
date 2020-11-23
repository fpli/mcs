package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.BehaviorEvent;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.function.DailyDoneSinkFunction;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.core.fs.Path;
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
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Monitor Behavior event sink files and generate daily done files.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class BehaviorEventDailyDoneApp {
  private static final String EVENTTIMESTAMP = "eventtimestamp";
  private static final String STATE_NAME = "behavior-event-current-daily-done";
  private StreamExecutionEnvironment streamExecutionEnvironment;

  // Time interval between state checkpoints in milliseconds
  private static final long DEFAULT_CHECK_POINT_PERIOD = TimeUnit.MINUTES.toMillis(10);

  // The minimal pause before the next checkpoint is triggered
  private static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS = TimeUnit.SECONDS.toMillis(30);

  /// The checkpoint timeout, in milliseconds
  private static final long DEFAULT_CHECK_POINT_TIMEOUT = TimeUnit.MINUTES.toMillis(5);

  // The size of the generated windows
  private static final Time DEFAULT_PROCESSING_TIME_WINDOWS = Time.minutes(5);

  // The maximum number of concurrent checkpoint attempts
  private static final int DEFAULT_MAX_CONCURRENT_CHECK_POINTS = 1;

  public static void main(String[] args) throws Exception {
    BehaviorEventDailyDoneApp transformApp = new BehaviorEventDailyDoneApp();
    transformApp.run();
  }

  /**
   * Monitor the input files, and find the earliest timestamp
   * @throws Exception which occurs during job execution.
   */
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

    // Periodically scans the path for new data
    DataStream<Row> inputStream = streamExecutionEnvironment.readFile(parquetRowInputFormat, dataPath, FileProcessingMode.PROCESS_CONTINUOUSLY, TimeUnit.MINUTES.toMillis(5));

    inputStream.map(new LocalDateTimeMapFunction())
            .windowAll(TumblingProcessingTimeWindows.of(DEFAULT_PROCESSING_TIME_WINDOWS))
            .reduce(new LocalDateTimeReduceFunction())
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

  protected static class LocalDateTimeReduceFunction implements ReduceFunction<LocalDateTime> {
    @Override
    public LocalDateTime reduce(LocalDateTime value1, LocalDateTime value2) throws Exception {
      return value1.isBefore(value2) ? value1 : value2;
    }
  }

  protected static class LocalDateTimeMapFunction implements MapFunction<Row, LocalDateTime> {
    @Override
    public LocalDateTime map(Row value) throws Exception {
      int pos = BehaviorEvent.getClassSchema().getField(EVENTTIMESTAMP).pos();
      return LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) value.getField(pos)), ZoneId.systemDefault());
    }
  }
}
