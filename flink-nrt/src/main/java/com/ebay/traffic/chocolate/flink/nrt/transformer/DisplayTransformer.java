package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.FlatMessage;
import com.ebay.kernel.patternmatch.dawg.Dawg;
import com.ebay.kernel.patternmatch.dawg.DawgDictionary;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

public class DisplayTransformer extends BaseTransformer {
  public DisplayTransformer(final FilterMessage sourceRecord) {
    super(sourceRecord);
  }

  @Override
  protected String getMgvalue() {
    return StringConstants.ZERO;
  }

  @Override
  protected String getMgvaluereason() {
    String brwsrName = getBrwsrName();
    String clntRemoteIp = getClntRemoteIp();

    if (isBotByUserAgent(brwsrName, userAgentBotDawgDictionary) || isBotByIp(clntRemoteIp, ipBotDawgDictionary)) {
      return StringConstants.FOUR;
    } else {
      return StringConstants.EMPTY;
    }
  }

  @Override
  protected String getMgvalueRsnCd() {
    return getMgvaluereason();
  }
}
