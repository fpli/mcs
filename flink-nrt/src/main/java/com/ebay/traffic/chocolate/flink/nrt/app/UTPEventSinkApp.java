package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.sink.AbstractEventDateTimeBucketAssigner;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Receive utp messages, and sink parquet files to HDFS directly.
 *
 * @author Zhiyuan Wang
 * @since 2020/11/18
 */
public class UTPEventSinkApp extends AbstractRheosEventSinkApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(UTPEventSinkApp.class);

  public static void main(String[] args) throws Exception {
    UTPEventSinkApp sinkApp = new UTPEventSinkApp();
    sinkApp.run();
  }

  @Override
  protected void loadProperty() {
    this.config = PropertyMgr.getInstance().loadYaml("utp-event-sink-app.yaml");
  }

  @Override
  protected BucketAssigner<GenericRecord, String> getSinkBucketAssigner() {
    return new AbstractEventDateTimeBucketAssigner<GenericRecord>() {
      @Override
      protected long getEventTimestamp(GenericRecord element) {
        return (Long) element.get("producerEventTs");
      }
    };
  }
}
