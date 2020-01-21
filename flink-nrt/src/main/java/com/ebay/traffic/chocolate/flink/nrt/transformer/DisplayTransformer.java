package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.FlatMessage;
import org.apache.avro.specific.SpecificRecordBase;

public class DisplayTransformer extends BaseTransformer {
  public DisplayTransformer(final FilterMessage sourceRecord) {
    super(sourceRecord);
  }

  @Override
  protected String getMgvalue() {
    return super.getMgvalue();
  }

  @Override
  protected Integer getMgvalueRsnCd() {
    return super.getMgvalueRsnCd();
  }
}
