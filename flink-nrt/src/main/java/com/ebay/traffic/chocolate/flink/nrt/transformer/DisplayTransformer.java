package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;

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
