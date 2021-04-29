package com.ebay.traffic.chocolate.flink.nrt.function;

import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingImkMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

public class FilterDuplicatedEventsByRvrId extends KeyedProcessFunction<Long, UnifiedTrackingImkMessage, UnifiedTrackingImkMessage> {
  private transient ValueState<Long> count;
  private final String jobName;
  private final boolean enableDedupe;
  private final OutputTag<UnifiedTrackingImkMessage> dupTag;

  public FilterDuplicatedEventsByRvrId(String jobName, boolean enableDedupe, OutputTag<UnifiedTrackingImkMessage> dupTag) {
    this.jobName = jobName;
    this.enableDedupe = enableDedupe;
    this.dupTag = dupTag;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void processElement(UnifiedTrackingImkMessage value, Context ctx, Collector<UnifiedTrackingImkMessage> out) throws Exception {
    if (!enableDedupe) {
      out.collect(value);
      return;
    }
    Long currentCount = count.value();
    if (currentCount == null) {
      count.update(0L);
      out.collect(value);
    } else {
      currentCount++;
      count.update(currentCount);
      ctx.output(dupTag, value);
    }
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(24))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();
    ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("rvrId", Long.class);
    descriptor.enableTimeToLive(ttlConfig);
    count = getRuntimeContext().getState(descriptor);
  }
}
