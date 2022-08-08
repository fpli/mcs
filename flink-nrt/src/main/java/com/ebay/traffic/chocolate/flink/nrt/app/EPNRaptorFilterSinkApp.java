package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.flink.nrt.sink.AbstractEventDateBucketAssigner;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Receive utp messages, and sink parquet files to HDFS directly.
 *
 * @author Zhiyuan Wang
 * @since 2020/11/18
 */
public class EPNRaptorFilterSinkApp extends AbstractRheosEventSinkApp {
  private static final Logger LOGGER = LoggerFactory.getLogger(EPNRaptorFilterSinkApp.class);

  public static void main(String[] args) throws Exception {
    EPNRaptorFilterSinkApp sinkApp = new EPNRaptorFilterSinkApp();
    sinkApp.run();
  }

  @Override
  protected void loadProperty() {
    this.config = PropertyMgr.getInstance().loadYaml("epn-raptor-filter-sink-app.yaml");
  }

  @Override
  protected BucketAssigner<GenericRecord, String> getSinkBucketAssigner() {
    return new AbstractEventDateBucketAssigner<GenericRecord>() {
      @Override
      protected long getEventTimestamp(GenericRecord element) {
        return (Long) element.get("timestamp");
      }
    };
  }
}
