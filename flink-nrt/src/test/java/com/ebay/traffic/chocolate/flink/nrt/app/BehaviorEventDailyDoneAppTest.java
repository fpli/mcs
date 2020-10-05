package com.ebay.traffic.chocolate.flink.nrt.app;

import org.apache.flink.types.Row;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

public class BehaviorEventDailyDoneAppTest {

  @Test
  public void localDateTimeReduceFunction() throws Exception {
    BehaviorEventDailyDoneApp.LocalDateTimeReduceFunction function = new BehaviorEventDailyDoneApp.LocalDateTimeReduceFunction();
    LocalDateTime first = LocalDateTime.of(2018, 1, 1, 1, 0);
    LocalDateTime second = LocalDateTime.of(2018, 1, 1, 2, 0);
    assertEquals(first, function.reduce(first, second));
  }

  @Test
  public void localDateTimeMapFunction() throws Exception {
    BehaviorEventDailyDoneApp.LocalDateTimeMapFunction function = new BehaviorEventDailyDoneApp.LocalDateTimeMapFunction();
    LocalDateTime now = LocalDateTime.of(2018, 1, 1, 1, 0);
    long millis = now.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    Row row = Mockito.mock(Row.class);
    Mockito.when(row.getField(Mockito.anyInt())).thenReturn(millis);
    assertEquals(now, function.map(row));
  }

}