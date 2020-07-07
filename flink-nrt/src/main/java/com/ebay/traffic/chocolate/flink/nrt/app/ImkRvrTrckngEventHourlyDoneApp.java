package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.app.raptor.chocolate.avro.ImkRvrTrckngEventMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.DateConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetMapInputFormat;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Monitor HDFS files and generate hourly done files.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class ImkRvrTrckngEventHourlyDoneApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImkRvrTrckngEventHourlyDoneApp.class);

  protected StreamExecutionEnvironment streamExecutionEnvironment;

  private static final long DEFAULT_CHECK_POINT_PERIOD = TimeUnit.MINUTES.toMillis(3);

  private static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECK_POINTS = TimeUnit.SECONDS.toMillis(30);

  private static final long DEFAULT_CHECK_POINT_TIMEOUT = TimeUnit.SECONDS.toMillis(10);

  private static final int DEFAULT_MAX_CONCURRENT_CHECK_POINTS = 1;

  private static String donePath;

  private static String doneFilePrefix;

  private static String doneFileSuffix;

  public static void main(String[] args) throws Exception {
    ImkRvrTrckngEventHourlyDoneApp transformApp = new ImkRvrTrckngEventHourlyDoneApp();
    transformApp.run();
  }

  @SuppressWarnings("rawtypes")
  protected void run() throws Exception {
    Properties properties = PropertyMgr.getInstance().loadProperty(PropertyConstants.IMK_RVR_TRCKNG_EVENT_HOURLY_DONE_APP_HDFS_PROPERTIES);
    String dataPath = properties.getProperty(PropertyConstants.DATA_PATH);
    donePath = properties.getProperty(PropertyConstants.DONE_PATH);
    doneFilePrefix = properties.getProperty(PropertyConstants.DONE_FILE_PREFIX);
    doneFileSuffix = properties.getProperty(PropertyConstants.DONE_FILE_SUFFIX);

    streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
    prepareBaseExecutionEnvironment();
    ParquetMapInputFormat parquetRowInputFormat = new ParquetMapInputFormat(new Path(dataPath), new AvroSchemaConverter().convert(ImkRvrTrckngEventMessage.getClassSchema()));
    parquetRowInputFormat.setNestedFileEnumeration(true);
    DataStream<Map> inputStream = streamExecutionEnvironment.readFile(parquetRowInputFormat, dataPath, FileProcessingMode.PROCESS_CONTINUOUSLY, TimeUnit.MINUTES.toMillis(5));
    inputStream.map((MapFunction<Map, LocalDateTime>) value -> LocalDateTime.parse((String) value.get(TransformerConstants.EVENT_TS),
            DateTimeFormatter.ofPattern(DateConstants.YYYY_MM_DD_HH_MM_SS_SSS))).windowAll(TumblingProcessingTimeWindows.of(Time.minutes(5))).reduce(new ReduceFunction<LocalDateTime>() {
      @Override
      public LocalDateTime reduce(LocalDateTime value1, LocalDateTime value2) {
        return value1.isBefore(value2) ? value1 : value2;
      }
    }).addSink(new HourlyDoneCustomSinkFunction());

    streamExecutionEnvironment.execute(this.getClass().getSimpleName());
  }

  private static class HourlyDoneCustomSinkFunction extends RichSinkFunction<LocalDateTime> implements CheckpointedFunction {
    private static final Path PATH = new Path(donePath);
    public static final String LAST_HOURLY_DONE_STATE = "last-hourly-done-state";
    private transient FileSystem fileSystem;

    private transient DateTimeFormatter doneFileDatetimeFormatter;

    private transient DateTimeFormatter dayFormatter;

    private transient ListState<LocalDateTime> lastHourlyDoneState;

    private transient LocalDateTime lastHourlyDone;

    private String getDoneFileName(String doneDir, LocalDateTime doneFileDatetime) {
      return doneDir + StringConstants.SLASH + doneFilePrefix + doneFileDatetime.format(doneFileDatetimeFormatter) + doneFileSuffix;
    }

    private String getDoneDir(LocalDateTime dateTime) {
      return donePath + StringConstants.SLASH + dateTime.format(dayFormatter);
    }

    @Override
    public void invoke(LocalDateTime value, Context context) {
      LOGGER.info("min event ts {}", value);
      long delays = ChronoUnit.HOURS.between(lastHourlyDone, value);
      Stream<LocalDateTime> localDateTimeStream = LongStream.range(1, delays).mapToObj(lastHourlyDone::plusHours);
      localDateTimeStream.forEach(localDateTime -> {
        lastHourlyDone = localDateTime;
        String doneDir = getDoneDir(localDateTime);
        try {
          String doneFile = getDoneFileName(doneDir, localDateTime);
          LOGGER.info("try to generate done file {}", doneFile);
          Path doneDirPath = new Path(doneDir);
          Path doneFilePath = new Path(doneFile);
          if (fileSystem.exists(doneDirPath) && !fileSystem.exists(doneFilePath)) {
            fileSystem.create(doneFilePath, FileSystem.WriteMode.NO_OVERWRITE);
          } else {
            fileSystem.mkdirs(doneDirPath);
            fileSystem.create(doneFilePath, FileSystem.WriteMode.NO_OVERWRITE);
          }
        } catch (IOException e) {
          LOGGER.error(e.getMessage());
        }
      });
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
      LOGGER.info("snapshot state {}", lastHourlyDone);
      lastHourlyDoneState.clear();
      lastHourlyDoneState.add(lastHourlyDone);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
      fileSystem = PATH.getFileSystem();
      doneFileDatetimeFormatter = DateTimeFormatter.ofPattern(DateConstants.YYYYMMDDHH);
      dayFormatter = DateTimeFormatter.ofPattern(DateConstants.YYYYMMDD);

      ListStateDescriptor<LocalDateTime> descriptor = new ListStateDescriptor<>(
              LAST_HOURLY_DONE_STATE,
              TypeInformation.of(LocalDateTime.class));
      lastHourlyDoneState = context.getOperatorStateStore().getListState(descriptor);
      if (context.isRestored()) {
        Iterable<LocalDateTime> localDateTimes = lastHourlyDoneState.get();
        Iterator<LocalDateTime> iterator = localDateTimes.iterator();
        if (iterator.hasNext()) {
          lastHourlyDone = iterator.next();
          LOGGER.info("restored last hourly done {}", lastHourlyDone);
        } else {
          lastHourlyDone = getLastHourlyDone();
          LOGGER.info("no restored state, new last hourly done {}", lastHourlyDone);
        }
      } else {
        lastHourlyDone = getLastHourlyDone();
        LOGGER.info("new last hourly done {}", lastHourlyDone);
      }
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private LocalDateTime getLastHourlyDone() throws Exception {
      FileStatus lastDoneDirFileStatus = Arrays.stream(fileSystem.listStatus(PATH)).filter(FileStatus::isDir).max((o1, o2) -> {
        String name1 = o1.getPath().getName();
        String name2 = o2.getPath().getName();
        LocalDate date1 = LocalDate.parse(name1, dayFormatter);
        LocalDate date2 = LocalDate.parse(name2, dayFormatter);
        return date1.compareTo(date2);
      }).get();
      return Arrays.stream(fileSystem.listStatus(lastDoneDirFileStatus.getPath()))
              .map(doneFileStatus -> doneFileStatus.getPath().getName())
              .filter(doneFileName -> doneFileName.startsWith(doneFilePrefix)).map(doneFileName -> {
                String doneFileDatetime = doneFileName.substring(doneFilePrefix.length(), doneFileName.length() - doneFileSuffix.length());
                return LocalDateTime.parse(doneFileDatetime, doneFileDatetimeFormatter);
              }).max(LocalDateTime::compareTo).get();
    }
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
