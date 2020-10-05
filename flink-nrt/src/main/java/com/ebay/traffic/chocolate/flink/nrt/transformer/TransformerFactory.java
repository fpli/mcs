package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV4;

/**
 * Determine the specific transformer.
 *
 * @author Zhiyuan Wang
 * @since 2019/12/8
 */
public class TransformerFactory {
  public static BaseTransformer getConcreteTransformer(FilterMessageV4 sourceRecord) {
    ChannelType channelType = sourceRecord.getChannelType();
    if(channelType == null) {
      return new BaseTransformer(sourceRecord);
    }
    else {
      switch (channelType) {
        case DISPLAY:
          return new DisplayTransformer(sourceRecord);
        case ROI:
          return new RoiTransformer(sourceRecord);
        default:
          return new BaseTransformer(sourceRecord);
      }
    }
  }
}
