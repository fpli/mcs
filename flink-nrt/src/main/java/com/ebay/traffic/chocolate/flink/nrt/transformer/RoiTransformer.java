package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import org.apache.commons.lang3.StringUtils;

import java.net.MalformedURLException;
import java.net.URL;

public class RoiTransformer extends BaseTransformer {

  public RoiTransformer(final FilterMessage sourceRecord) {
    super(sourceRecord);
  }

  @Override
  public Integer getDstClientId() throws MalformedURLException {
    String uri = (String) sourceRecord.get(TransformerConstants.URI);
    return getClientIdFromRoverUrl(uri);
  }

  @Override
  protected String getEbaySiteId() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, TransformerConstants.SITE_ID);
  }

  @Override
  protected String getCartId() {
    String tempUriQuery = getTempUriQuery();
    return getIdFromUrlQuery(tempUriQuery, 3);
  }

  @Override
  protected String getTransactionId() {
    String tempUriQuery = getTempUriQuery();
    return getIdFromUrlQuery(tempUriQuery, 2);
  }

  @Override
  protected String getTransactionType() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, TransformerConstants.TRAN_TYPE);
  }

  // TODO weird logic
  @Override
  protected String getItemId() {
    String itemId = getRoiItemId();
    String roiItemId = getRoiItemId();
    if (StringUtils.isNotEmpty(roiItemId) && StringUtils.isNumeric(roiItemId) && Long.parseLong(roiItemId) != -999L) {
      return roiItemId;
    } else if (StringUtils.isNotEmpty(itemId) && itemId.length() <= 18) {
      return itemId;
    } else{
      return StringConstants.EMPTY;
    }
  }

  @Override
  protected String getRoiItemId() {
    String tempUriQuery = getTempUriQuery();
    return getIdFromUrlQuery(tempUriQuery, 1);
  }

  private String getIdFromUrlQuery(String query, Integer index) {
    String mupid = getParamValueFromQuery(query, TransformerConstants.MPUID);
    String[] ids = mupid.split(StringConstants.SEMICOLON);
    if(ids.length > index) {
      return ids[index];
    } else {
      return StringConstants.ZERO;
    }
  }

  private Integer getClientIdFromRoverUrl(String uri) throws MalformedURLException {
    String path = new URL(uri).getPath();
    if (StringUtils.isNotEmpty(path)) {
      String[] pathArray = path.split(StringConstants.SLASH);
      if (pathArray.length > 3) {
        return Integer.valueOf(pathArray[3].split("\\?")[0].split(StringConstants.HYPHEN)[0]);
      }
    }
    return 0;
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
