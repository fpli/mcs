package com.ebay.traffic.chocolate.flink.nrt.function;

import com.ebay.traffic.chocolate.flink.nrt.constant.DateConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Generate daily done files base on current event timestamp and last done file.
 * If the current timestamp is 2020-01-01 00:01:00, and the last done file is 2020-12-29, that means all data before
 * 2020-01-01 00:01:00 is ready, so it's safe to generate done files for 2020-12-30 and 2020-12-31.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/18
 */
public class DailyDoneSinkFunction extends RichSinkFunction<LocalDateTime> implements CheckpointedFunction {
  private static final Logger LOGGER = LoggerFactory.getLogger(DailyDoneSinkFunction.class);

  private transient FileSystem fileSystem;

  private transient DateTimeFormatter doneFileDatetimeFormatter;

  private transient ListState<LocalDate> lastDailyDoneState;

  private transient LocalDate lastDailyDone;

  private final String lastDailyDoneStateName;

  private final String doneDirStr;

  private final String doneFilePrefix;

  private final String doneFileSuffix;

  private final Path doneDirPath;

  public DailyDoneSinkFunction(String lastDailyDoneStateName, String donePathStr, String doneFilePrefix, String doneFileSuffix) {
    this.lastDailyDoneStateName = lastDailyDoneStateName;
    this.doneDirStr = donePathStr;
    this.doneFilePrefix = doneFilePrefix;
    this.doneFileSuffix = doneFileSuffix;
    this.doneDirPath = new Path(donePathStr);
  }

  private String getDoneFileName(String doneDir, LocalDate doneFileDatetime) {
    return doneDir + StringConstants.SLASH + doneFilePrefix + doneFileDatetime.format(doneFileDatetimeFormatter) + doneFileSuffix;
  }

  @Override
  public void invoke(LocalDateTime value, Context context) {
    LOGGER.info("min event ts {}", value);
    System.out.println("min event ts " + value);
    long delays = ChronoUnit.DAYS.between(lastDailyDone, value);
    Stream<LocalDate> localDateTimeStream = LongStream.range(1, delays).mapToObj(lastDailyDone::plusDays);
    localDateTimeStream.forEach(localDateTime -> {
      lastDailyDone = localDateTime;
      try {
        String doneFile = getDoneFileName(doneDirStr, localDateTime);
        LOGGER.info("try to generate done file {}", doneFile);
        System.out.println("try to generate done file " + doneFile);
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
    LOGGER.info("snapshot state {}", lastDailyDone);
    lastDailyDoneState.clear();
    lastDailyDoneState.add(lastDailyDone);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    fileSystem = doneDirPath.getFileSystem();
    doneFileDatetimeFormatter = DateTimeFormatter.ofPattern(DateConstants.YYYYMMDD);

    ListStateDescriptor<LocalDate> descriptor = new ListStateDescriptor<>(
            lastDailyDoneStateName,
            TypeInformation.of(LocalDate.class));
    lastDailyDoneState = context.getOperatorStateStore().getListState(descriptor);
    if (context.isRestored()) {
      Iterable<LocalDate> localDateTimes = lastDailyDoneState.get();
      Iterator<LocalDate> iterator = localDateTimes.iterator();
      if (iterator.hasNext()) {
        lastDailyDone = iterator.next();
        LOGGER.info("restored daily daily done {}", lastDailyDone);
      } else {
        lastDailyDone = getLastDailyDone();
        LOGGER.info("no restored state, new last daily done {}", lastDailyDone);
      }
    } else {
      lastDailyDone = getLastDailyDone();
      LOGGER.info("new last daily done {}", lastDailyDone);
    }
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  private LocalDate getLastDailyDone() throws Exception {
    return Arrays.stream(fileSystem.listStatus(doneDirPath))
            .map(doneFileStatus -> doneFileStatus.getPath().getName())
            .filter(doneFileName -> doneFileName.startsWith(doneFilePrefix)).map(doneFileName -> {
              String doneFileDatetime = doneFileName.substring(doneFilePrefix.length(), doneFileName.length() - doneFileSuffix.length());
              return LocalDate.parse(doneFileDatetime, doneFileDatetimeFormatter);
            }).max(LocalDate::compareTo).get();
  }
}
