package com.ebay.traffic.chocolate.flink.nrt.provider.ersxid;

import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingImkMessage;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.typeutils.AvroSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.asynchttpclient.Response;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ErsXidRequestTest {
  private static final long TIMEOUT = 1000L;

  @Rule
  public Timeout timeoutRule = new Timeout(1, TimeUnit.MINUTES);

  @Test
  public void test() throws Exception {
    final OneInputStreamOperatorTestHarness<UnifiedTrackingImkMessage, UnifiedTrackingImkMessage> testHarness =
            new OneInputStreamOperatorTestHarness<>(
                    new AsyncWaitOperatorFactory<>(new ErsXidRequest(), TIMEOUT, 1, AsyncDataStream.OutputMode.UNORDERED),
                    new AvroSerializer<>(UnifiedTrackingImkMessage.class));
    testHarness.open();

    UnifiedTrackingImkMessage unifiedTrackingImkMessage = new UnifiedTrackingImkMessage();
    unifiedTrackingImkMessage.setUserId(-1L);
    unifiedTrackingImkMessage.setGuid("19db4ef516b9f32758b5e1d001104504");
    unifiedTrackingImkMessage.setRvrCmndTypeCd("1");

    long initialTime = System.currentTimeMillis();
    synchronized (testHarness.getCheckpointLock()) {
      testHarness.processElement(new StreamRecord<>(unifiedTrackingImkMessage, initialTime + 1));
    }

    synchronized (testHarness.getCheckpointLock()) {
      testHarness.close();
    }

    ConcurrentLinkedQueue<Object> output = testHarness.getOutput();
    StreamRecord<UnifiedTrackingImkMessage> poll = (StreamRecord<UnifiedTrackingImkMessage>) output.poll();
    assertNotNull(poll);
    assertEquals(Long.valueOf(0L), poll.getValue().getUserId());
  }

  @Test
  public void parseAccountId() throws Exception {
    Response response = Mockito.mock(Response.class);
    ErsXidRequest ersXidRequest = new ErsXidRequest();

    final String taskName = "foobarTask";
    final MetricGroup metricGroup = new UnregisteredMetricsGroup();
    final int numberOfParallelSubtasks = 42;
    final int indexOfSubtask = 43;
    final int attemptNumber = 1337;
    final String taskNameWithSubtask = "barfoo";
    final ExecutionConfig executionConfig = mock(ExecutionConfig.class);
    final ClassLoader userCodeClassLoader = mock(ClassLoader.class);

    RuntimeContext mockedRuntimeContext = mock(RuntimeContext.class);

    when(mockedRuntimeContext.getTaskName()).thenReturn(taskName);
    when(mockedRuntimeContext.getMetricGroup()).thenReturn(metricGroup);
    when(mockedRuntimeContext.getNumberOfParallelSubtasks()).thenReturn(numberOfParallelSubtasks);
    when(mockedRuntimeContext.getIndexOfThisSubtask()).thenReturn(indexOfSubtask);
    when(mockedRuntimeContext.getAttemptNumber()).thenReturn(attemptNumber);
    when(mockedRuntimeContext.getTaskNameWithSubtasks()).thenReturn(taskNameWithSubtask);
    when(mockedRuntimeContext.getExecutionConfig()).thenReturn(executionConfig);
    when(mockedRuntimeContext.getUserCodeClassLoader()).thenReturn(userCodeClassLoader);

    ersXidRequest.setRuntimeContext(mockedRuntimeContext);

    ersXidRequest.open(new Configuration());

    Mockito.when(response.getStatusCode()).thenReturn(400);
    assertEquals(0L, ersXidRequest.parseAccountId(response));

    Mockito.when(response.getResponseBody()).thenReturn("{\n" +
            "}");
    Mockito.when(response.getStatusCode()).thenReturn(200);
    assertEquals(0L, ersXidRequest.parseAccountId(response));

    Mockito.when(response.getResponseBody()).thenReturn("{\n" +
            "    \"idMap\": [\n" +
            "        {\n" +
            "            \"xid\": {\n" +
            "                \"id\": \"100000000001122327605\",\n" +
            "                \"dqScore\": 2\n" +
            "            }\n" +
            "        }\n" +
            "    ]\n" +
            "}");
    Mockito.when(response.getStatusCode()).thenReturn(200);
    assertEquals(0L, ersXidRequest.parseAccountId(response));

    Mockito.when(response.getResponseBody()).thenReturn("{\n" +
            "    \"idMap\": [\n" +
            "        {\n" +
            "            \"accounts\": [\n" +
            "                {\n" +
            "                    \"lastSeenTime\": 1606435200\n" +
            "                },\n" +
            "                {\n" +
            "                    \"lastSeenTime\": 1606435201\n" +
            "                }\n" +
            "            ]\n" +
            "        }\n" +
            "    ]\n" +
            "}");
    Mockito.when(response.getStatusCode()).thenReturn(200);
    assertEquals(0L, ersXidRequest.parseAccountId(response));

    Mockito.when(response.getResponseBody()).thenReturn("{\n" +
            "    \"idMap\": [\n" +
            "        {\n" +
            "            \"accounts\": [\n" +
            "                {\n" +
            "                    \"id\": \"abcd\",\n" +
            "                    \"lastSeenTime\": 1606435200\n" +
            "                }\n" +
            "            ]\n" +
            "        }\n" +
            "    ]\n" +
            "}");
    Mockito.when(response.getStatusCode()).thenReturn(200);
    assertEquals(0L, ersXidRequest.parseAccountId(response));

    Mockito.when(response.getResponseBody()).thenReturn("{\n" +
            "    \"idMap\": [\n" +
            "        {\n" +
            "            \"accounts\": [\n" +
            "                {\n" +
            "                    \"id\": \"1122327605\",\n" +
            "                    \"lastSeenTime\": 1606435200\n" +
            "                },\n" +
            "                {\n" +
            "                    \"id\": \"1122327606\",\n" +
            "                    \"lastSeenTime\": 1606435201\n" +
            "                }\n" +
            "            ]\n" +
            "        }\n" +
            "    ]\n" +
            "}");
    Mockito.when(response.getStatusCode()).thenReturn(200);
    assertEquals(1122327606L, ersXidRequest.parseAccountId(response));
  }

}