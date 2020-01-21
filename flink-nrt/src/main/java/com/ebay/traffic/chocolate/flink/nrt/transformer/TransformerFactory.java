package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.FlatMessage;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * This class
 *
 * @author Zhiyuan Wang
 * @since 2019/12/8
 */
public class TransformerFactory {
  public static BaseTransformer getConcreteTransformer(FilterMessage soureRecord) {
    ChannelType channelType = soureRecord.getChannelType();
    switch (channelType) {
      case DISPLAY:
        return new DisplayTransformer(soureRecord);
      case ROI:
        return new RoiTransformer(soureRecord);
      default:
        return new BaseTransformer(soureRecord);
    }
  }
}
