package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV4;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import org.apache.avro.generic.GenericRecord;

/**
 * Transformer for display events.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/8
 */
public class DisplayTransformer extends BaseTransformer {
  public DisplayTransformer(final GenericRecord sourceRecord) {
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
