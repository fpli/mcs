package com.ebay.traffic.chocolate.flink.nrt.transformer.utp;

import com.ebay.app.raptor.chocolate.avro.versions.UnifiedTrackingRheosMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import com.ebay.traffic.chocolate.utp.common.ServiceEnum;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Transformer for roi events.
 *
 * @author Zhiyuan Wang
 * @since 2020/1/8
 */
public class UTPRoiTransformer extends UTPImkTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(UTPImkTransformer.class);

  public UTPRoiTransformer(final UnifiedTrackingRheosMessage sourceRecord, String channelType, String actionType, Map<String, Meter> etlMetrics) {
    super(sourceRecord, channelType, actionType, etlMetrics);
  }

  @Override
  protected String parseUrl() {
    String url = super.parseUrl();
    String service = this.sourceRecord.getService();
    if (ServiceEnum.ROVER.getValue().equals(service)) {
      try {
        url = URLDecoder.decode(url, StandardCharsets.UTF_8.name());
      } catch (Exception e) {
        etlMetrics.get(UTPImkTransformerMetrics.NUM_ERROR_MALFORMED_RATE).markEvent();
        LOGGER.warn(MALFORMED_URL, e);
      }
    }
    return url;
  }

  /**
   * For ROI, set default value as 0 to keep consistent with MCS and imkETL
   * @return rotation id
   */
  @Override
  @SuppressWarnings("UnstableApiUsage")
  protected Long parseRotationId() {
    String rid = sourceRecord.getRotationId();
    if (StringUtils.isEmpty(rid)) {
      return 0L;
    }
    Long aLong = Longs.tryParse(rid);
    if (aLong == null) {
      return 0L;
    }
    return aLong;
  }

  @Override
  protected Integer getDstClientId() {
    return 0;
  }

  @Override
  protected String getEbaySiteId() {
    return getParamValueFromQuery(query, TransformerConstants.SITE_ID);
  }

  @Override
  protected String getCartId() {
    return getIdFromUrlQuery(query, 3);
  }

  @Override
  protected String getTransactionId() {
    return getIdFromUrlQuery(query, 2);
  }

  @Override
  protected String getTransactionType() {
    return getParamValueFromQuery(query, TransformerConstants.TRAN_TYPE);
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
    return getIdFromUrlQuery(query, 1);
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
}
