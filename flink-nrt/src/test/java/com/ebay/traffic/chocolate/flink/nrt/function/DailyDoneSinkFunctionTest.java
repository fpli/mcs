package com.ebay.traffic.chocolate.flink.nrt.function;

import com.ebay.traffic.chocolate.flink.nrt.constant.DateConstants;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class DailyDoneSinkFunctionTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private String donePathStr;
  private final String lastDailyDoneStateName = "daily-done";
  private final String doneFilePrefix = "prefix_";
  private final String doneFileSuffix = ".done";
  private static LocalFileSystem lfs;

  private static DateTimeFormatter doneFileDatetimeFormatter = DateTimeFormatter.ofPattern(DateConstants.YYYYMMDD);

  private OneInputStreamOperatorTestHarness<LocalDateTime, Object> testHarness;
  private DailyDoneSinkFunction dailyDoneSinkFunction;

  @Before
  public void setUp() throws Exception {
    lfs = new LocalFileSystem();
    donePathStr = new File(TEMP_FOLDER.getRoot(), "daily-done").getPath();
    String lastDone = LocalDateTime.of(2020, 1, 1, 0, 0).format(doneFileDatetimeFormatter);
    lfs.create(new Path(donePathStr + "/" + doneFilePrefix + lastDone + doneFileSuffix), FileSystem.WriteMode.NO_OVERWRITE);
    dailyDoneSinkFunction = new DailyDoneSinkFunction(lastDailyDoneStateName, donePathStr, doneFilePrefix, doneFileSuffix);
  }

  @Test
  public void test() throws Exception {
    OperatorSubtaskState snapshot;
    Set<String> actual;
    Set<String> expected;

    OneInputStreamOperatorTestHarness<LocalDateTime, Object> testHarness1 = new OneInputStreamOperatorTestHarness<>(new StreamSink<>(dailyDoneSinkFunction));
    testHarness1.open();

    LocalDateTime current = LocalDateTime.of(2020, 1, 5, 2, 0);

    testHarness1.processElement(new StreamRecord<>(current, System.currentTimeMillis()));
    snapshot = testHarness1.snapshot(1L, System.currentTimeMillis());

    actual = Arrays.stream(lfs.listStatus(new Path(donePathStr))).map(fileStatus -> fileStatus.getPath().getName()).collect(Collectors.toSet());
    expected = new HashSet<>();

    expected.add(doneFilePrefix + "20200101" + doneFileSuffix);
    expected.add(doneFilePrefix + "20200102" + doneFileSuffix);
    expected.add(doneFilePrefix + "20200103" + doneFileSuffix);
    expected.add(doneFilePrefix + "20200104" + doneFileSuffix);
    assertEquals(expected, actual);

    OneInputStreamOperatorTestHarness<LocalDateTime, Object> testHarness2 = new OneInputStreamOperatorTestHarness<>(new StreamSink<>(dailyDoneSinkFunction));
    testHarness2.initializeState(snapshot);
    testHarness2.open();

    LocalDateTime next = LocalDateTime.of(2020, 1, 7, 2, 0);

    testHarness2.processElement(new StreamRecord<>(next, System.currentTimeMillis()));
    testHarness2.snapshot(2L, System.currentTimeMillis());

    actual = Arrays.stream(lfs.listStatus(new Path(donePathStr))).map(fileStatus -> fileStatus.getPath().getName()).collect(Collectors.toSet());
    expected = new HashSet<>();

    expected.add(doneFilePrefix + "20200101" + doneFileSuffix);
    expected.add(doneFilePrefix + "20200102" + doneFileSuffix);
    expected.add(doneFilePrefix + "20200103" + doneFileSuffix);
    expected.add(doneFilePrefix + "20200104" + doneFileSuffix);
    expected.add(doneFilePrefix + "20200105" + doneFileSuffix);
    expected.add(doneFilePrefix + "20200106" + doneFileSuffix);
    assertEquals(expected, actual);

  }
}