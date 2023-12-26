package com.ebay.traffic.chocolate.flink.nrt.provider;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV7;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AsyncDataRequestTest {
  private static final long TIMEOUT = 1000L;

  @Rule
  public Timeout timeoutRule = new Timeout(1, TimeUnit.MINUTES);

  @Before
  public void setUp() throws Exception {

  }

  private FilterMessageV7 createSourceRecord(String json) throws IOException {
    return FilterMessage.readFromJSON(json);
  }

  private static <OUT> OneInputStreamOperatorTestHarness<Integer, OUT> createTestHarness(
          AsyncFunction<Integer, OUT> function,
          long timeout,
          int capacity,
          AsyncDataStream.OutputMode outputMode) throws Exception {

    return new OneInputStreamOperatorTestHarness<>(
            new AsyncWaitOperatorFactory<>(function, timeout, capacity, outputMode),
            IntSerializer.INSTANCE);
  }

  @Test
  public void test() throws Exception {
    final OneInputStreamOperatorTestHarness<FilterMessageV7, FilterMessageV7> testHarness =
            new OneInputStreamOperatorTestHarness<>(
                    (OneInputStreamOperator<FilterMessageV7, FilterMessageV7>) new AsyncWaitOperatorFactory<>(new AsyncDataRequest(), TIMEOUT, 1, AsyncDataStream.OutputMode.UNORDERED),
                    new AvroSerializer<>(FilterMessageV7.class));
    testHarness.open();

    String json = PropertyMgr.getInstance().loadFile("filter-message.json");

    FilterMessageV7 filterMessage = createSourceRecord(json);
    filterMessage.setUserId(-1L);

    long initialTime = System.currentTimeMillis();
    synchronized (testHarness.getCheckpointLock()) {
      testHarness.processElement(new StreamRecord<>(filterMessage, initialTime + 1));
    }

    synchronized (testHarness.getCheckpointLock()) {
      testHarness.close();
    }

    ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
    StreamRecord<FilterMessageV7> poll = (StreamRecord<FilterMessageV7>) output.poll();
    assertNotNull(poll);
    assertEquals(0l, poll.getValue().getUserId());
  }
}
