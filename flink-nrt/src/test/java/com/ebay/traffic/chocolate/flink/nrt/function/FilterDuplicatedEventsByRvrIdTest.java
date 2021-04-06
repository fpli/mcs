package com.ebay.traffic.chocolate.flink.nrt.function;

import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingImkMessage;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FilterDuplicatedEventsByRvrIdTest {
  private FilterDuplicatedEventsByRvrId filterDuplicatedEventsByRvrId;
  private OutputTag<UnifiedTrackingImkMessage> dupTag;
  private final String jobName = "UT";

  @Before
  public void setUp() throws Exception {
    dupTag = new OutputTag<UnifiedTrackingImkMessage>("dup"){};
  }

  @Test
  public void enableDedupe() throws Exception {
    filterDuplicatedEventsByRvrId = new FilterDuplicatedEventsByRvrId(jobName, true, dupTag);

    UnifiedTrackingImkMessage paidSearch = new UnifiedTrackingImkMessage();
    paidSearch.setRvrId(1L);
    paidSearch.setRvrChnlTypeCd("2");

    UnifiedTrackingImkMessage display = new UnifiedTrackingImkMessage();
    display.setRvrId(1L);
    display.setRvrChnlTypeCd("4");

    OneInputStreamOperatorTestHarness<UnifiedTrackingImkMessage, UnifiedTrackingImkMessage> testHarness = processElements(Arrays.asList(paidSearch, display));

    Assert.assertEquals(Collections.singletonList(new StreamRecord<>(paidSearch)), testHarness.extractOutputStreamRecords());
    ConcurrentLinkedQueue<StreamRecord<UnifiedTrackingImkMessage>> sideOutput = testHarness.getSideOutput(dupTag);
    Assert.assertEquals(display, sideOutput.iterator().next().getValue());
  }

  private OneInputStreamOperatorTestHarness<UnifiedTrackingImkMessage, UnifiedTrackingImkMessage> processElements(List<UnifiedTrackingImkMessage> elements) throws Exception {
    OneInputStreamOperatorTestHarness<UnifiedTrackingImkMessage, UnifiedTrackingImkMessage> testHarness =
            new KeyedOneInputStreamOperatorTestHarness<>(
                    new KeyedProcessOperator<>(filterDuplicatedEventsByRvrId), x -> 1L, Types.LONG);

    testHarness.open();
    elements.forEach(element -> {
      try {
        testHarness.processElement(new StreamRecord<>(element));
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    return testHarness;
  }

  @Test
  public void disableDedupe() throws Exception {
    filterDuplicatedEventsByRvrId = new FilterDuplicatedEventsByRvrId(jobName, false, dupTag);

    UnifiedTrackingImkMessage paidSearch = new UnifiedTrackingImkMessage();
    paidSearch.setRvrId(1L);
    paidSearch.setRvrChnlTypeCd("2");

    UnifiedTrackingImkMessage display = new UnifiedTrackingImkMessage();
    display.setRvrId(1L);
    display.setRvrChnlTypeCd("4");

    OneInputStreamOperatorTestHarness<UnifiedTrackingImkMessage, UnifiedTrackingImkMessage> testHarness = processElements(Arrays.asList(paidSearch, display));


    Assert.assertEquals(Arrays.asList(new StreamRecord<>(paidSearch), new StreamRecord<>(display)), testHarness.extractOutputStreamRecords());
  }
}