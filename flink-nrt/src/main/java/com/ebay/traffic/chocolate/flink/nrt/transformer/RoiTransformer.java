package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;

import java.net.MalformedURLException;
import java.net.URL;

public class RoiTransformer extends BaseTransformer {
  public RoiTransformer(final FilterMessage sourceRecord) {
    super(sourceRecord);
  }

  @Override
  public Integer getDstClientId() {
    String uri = (String) sourceRecord.get(TransformerConstants.URI);
    return getClientIdFromRoverUrl(uri);
  }

  @Override
  protected String getEbaySiteId() {
    String tempUriQuery = getTempUriQuery();
    return getParamValueFromQuery(tempUriQuery, "siteId");
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
    return getParamValueFromQuery(tempUriQuery, "tranType");
  }

  @Override
  protected String getItemId() {
    return getRoiItemId();
  }

  @Override
  protected String getRoiItemId() {
    String tempUriQuery = getTempUriQuery();
    return getIdFromUrlQuery(tempUriQuery, 1);
  }

  private String getIdFromUrlQuery(String query, Integer index) {
    String mupid = getParamValueFromQuery(query, "mpuid");
    String[] ids = mupid.split(";");
    if(ids.length > index) {
      return ids[index];
    }
    else {
      return "0";
    }
  }

  @Override
  protected String getMgvalue() {
    return super.getMgvalue();
  }

  @Override
  protected Integer getMgvalueRsnCd() {
    return super.getMgvalueRsnCd();
  }

  private Integer getClientIdFromRoverUrl(String uri) {
    String path = null;
    try {
      path = new URL(uri).getPath();
    } catch (MalformedURLException e) {
      // TODO
    }
    if (path != null && path != "") {
      String[] pathArray = path.split("/");
      if (pathArray.length > 3) {
        return Integer.valueOf(pathArray[3].split("\\?")[0].split("-")[0]);
      }
    }
    return 0;
  }
}
