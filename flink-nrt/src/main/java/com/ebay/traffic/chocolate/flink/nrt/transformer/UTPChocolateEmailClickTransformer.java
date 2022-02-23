package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.utp.UepPayloadHelper;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.GenericRecordUtils;
import com.ebay.traffic.chocolate.flink.nrt.util.PulsarParseUtils;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum;
import com.ebay.traffic.chocolate.utp.common.EmailPartnerIdEnum;
import com.ebay.traffic.chocolate.utp.common.ServiceEnum;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.flink.metrics.Meter;
import org.springframework.util.CollectionUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

import static com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum.*;

public class UTPChocolateEmailClickTransformer {
  private final String sourceTopic;
  private final int partition;
  private final long offset;
  private final GenericRecord sourceRecord;
  private final RheosEvent sourceRheosEvent;
  private final String schemaVersion;
  private final Meter numInvalidDateInRate;
  private final Meter numNoPageIdInRate;
  private final Meter numNoChnlInRate;
  private final Meter numNotChocolateClickInRate;
  private Integer pageId;
  protected Map<String, String> applicationPayload;
  protected Map<String, String> clientData;
  private String url;
  private long eventTs;
  private MultiValueMap<String, String> queryParams;
  private ChannelTypeEnum channelType;
  private ActionTypeEnum actionTypeEnum;
  private Map<String, String> sojTags;
  private final boolean isValid;

  private static final String USER_ID = "userId";
  private static final int PAGE_ID_CHOCOLATE_CLICK = 2547208;
  private static final String SITE_EMAIL_CHANNEL_ID = "7";
  private static final String SITE_MESSAGE_CENTER_CHANNEL_ID = "26";
  private static final String MRKT_EMAIL_CHANNEL_ID = "8";
  private static final String MRKT_MESSAGE_CENTER_CHANNEL_ID = "27";
  private static final String GCX_EMAIL_CHANNEL_ID = "29";
  private static final String GCX_MESSAGE_CENTER_CHANNEL_ID = "30";
  private static final String MRKT_SMS_CHANNEL_ID = "24";
  private static final String SITE_SMS_CHANNEL_ID = "25";
  public static final String BEHAVIOR_PULSAR_MISC_BOT = "behavior.pulsar.customized.marketing-tracking.bot";
  public static final String REFERER = "Referer";
  public static final String TOPIC = "topic";

  private static final String REMOTE_IP = "RemoteIP";
  private static final String FORWARDED_FOR = "ForwardedFor";

  private static final ImmutableMap<String, String> EMAIL_TAG_PARAM_MAP = new ImmutableMap.Builder<String, String>()
          .put("adcamp_landingpage", "adcamp_landingpage")
          .put("adcamp_locationsrc", "adcamp_locationsrc")
          .put("adcamppu", "pu")
          .put("bu", "bu")
          .put("cbtrack", "cbtrack")
          .put("chnl", "mkcid")
          .put("crd", "crd")
          .put("cs", "cs")
          .put("ec", "ec")
          .put("emid", "bu")
          .put("emsid", "emsid")
          .put("es", "es")
          .put("euid", "euid")
          .put("exe", "exe")
          .put("ext", "ext")
          .put("trkId", "trkId")
          .put("nqc", "nqc")
          .put("nqt", "nqt")
          .put("osub", "osub")
          .put("placement-type", "placement-type")
          .put("rank", "rank")
          .put("rpp_cid", "rpp_cid")
          .put("segname", "segname")
          .put("sid", "emsid")
          .put("yminstc", "yminstc")
          .put("ymmmid", "ymmmid")
          .put("ymsid", "ymsid")
          .put("isUFESRedirect", "isUFESRedirect")
          .put("isUfes", "isUfes")
          .put("statusCode","statusCode")
          .put("xt", "xt")
          .put("X-UFES-EDGTRKSVC-INT", "X-UFES-EDGTRKSVC-INT")
          .build();

  private static final String GET_METHOD_PREFIX = "get";

  /**
   * Used to cache method object to improve reflect performance
   */
  private static final Map<String, Method>  FIELD_GET_METHOD_CACHE = new ConcurrentHashMap<>(16);

  /**
   * Map field name to get method name, eg. batch_id -> getBatchId
   */
  private static final UnaryOperator<String> FIELD_GET_METHOD_MAP_FUNCTION = fieldName -> {
    String upperCamelCase = CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, fieldName);
    return String.format("%s%s", GET_METHOD_PREFIX, upperCamelCase);
  };

  public UTPChocolateEmailClickTransformer(String sourceTopic, int partition, long offset, GenericRecord sourceRecord, RheosEvent sourceRheosEvent, String schemaVersion, Meter numInvalidDateInRate, Meter numNoPageIdInRate, Meter numNoChnlInRate, Meter numNotChocolateClickInRate) {
    this.sourceTopic = sourceTopic;
    this.partition = partition;
    this.offset = offset;
    this.sourceRecord = sourceRecord;
    this.sourceRheosEvent = sourceRheosEvent;
    this.schemaVersion = schemaVersion;
    this.numInvalidDateInRate = numInvalidDateInRate;
    this.numNoPageIdInRate = numNoPageIdInRate;
    this.numNoChnlInRate = numNoChnlInRate;
    this.numNotChocolateClickInRate = numNotChocolateClickInRate;
    this.isValid = validate();
    if (this.isValid) {
      initFields();
    }
  }

  public boolean isValid() {
    return isValid;
  }

  private boolean validate() {
    eventTs = parseEventTs();
    pageId = (Integer) sourceRecord.get(TransformerConstants.PAGE_ID);
    if (pageId == null) {
      numNoPageIdInRate.markEvent();
      return false;
    }
    if (pageId != PAGE_ID_CHOCOLATE_CLICK) {
      numNotChocolateClickInRate.markEvent();
      return false;
    }

    applicationPayload = GenericRecordUtils.getMap(sourceRecord, TransformerConstants.APPLICATION_PAYLOAD);
    clientData = GenericRecordUtils.getMap(sourceRecord, TransformerConstants.CLIENT_DATA);
    url = parseUrl();

    queryParams = parseQueryParams();

    String channelId = applicationPayload.get("chnl");
    if (StringUtils.isEmpty(channelId)) {
      numNoChnlInRate.markEvent();
      return false;
    }

    channelType = parseChannelType(channelId);
    return channelType != null;
  }

  private MultiValueMap<String, String> parseQueryParams() {
    try {
      return UriComponentsBuilder.fromUriString(url).build().getQueryParams();
    } catch (Exception e) {
      return CollectionUtils.unmodifiableMultiValueMap(new LinkedMultiValueMap<>());
    }
  }

  protected long parseEventTs() {
    return (Long) sourceRecord.get(TransformerConstants.EVENT_TIMESTAMP);
  }

  private String parseUrl() {
    String url = applicationPayload.getOrDefault("url_mpre", StringUtils.EMPTY);
    if (StringUtils.isNotEmpty(url)) {
      try {
        url = URLDecoder.decode(url, StandardCharsets.UTF_8.name());
      } catch (UnsupportedEncodingException ignored) {
      }
    }
    return url;
  }

  private ChannelTypeEnum parseChannelType(String channelId) {
    if (channelId == null) {
      return null;
    }
    if (SITE_EMAIL_CHANNEL_ID.equals(channelId)) {
      return SITE_EMAIL;
    }
    if (MRKT_EMAIL_CHANNEL_ID.equals(channelId)) {
      return MRKT_EMAIL;
    }
    if (SITE_MESSAGE_CENTER_CHANNEL_ID.equals(channelId)) {
      return SITE_MESSAGE_CENTER;
    }
    if (MRKT_MESSAGE_CENTER_CHANNEL_ID.equals(channelId)) {
      return MRKT_MESSAGE_CENTER;
    }
    if (GCX_EMAIL_CHANNEL_ID.equals(channelId)) {
      return GCX_EMAIL;
    }
    if (GCX_MESSAGE_CENTER_CHANNEL_ID.equals(channelId)) {
      return GCX_MESSAGE_CENTER;
    }
    if (MRKT_SMS_CHANNEL_ID.equals(channelId)) {
      return MRKT_SMS;
    }
    if (SITE_SMS_CHANNEL_ID.equals(channelId)) {
      return SITE_SMS;
    }
    return null;
  }

  private ActionTypeEnum parseActionType() {
    if (pageId == PAGE_ID_CHOCOLATE_CLICK) {
      return ActionTypeEnum.CLICK;
    }
    throw new IllegalArgumentException(String.format("Invalid pageId %d", pageId));
  }

  /**
   * Init fields that will be used in multi methods.
   */
  private void initFields() {
    actionTypeEnum = parseActionType();
    sojTags = PulsarParseUtils.getSojTags(queryParams);
  }

  public void transform(SpecificRecordBase trackingEvent) {
    for (Schema.Field field : trackingEvent.getSchema().getFields()) {
      final String fieldName = field.name();
      if (fieldName.equals(TransformerConstants.RHEOS_HEADER)) {
        continue;
      }
      Object value = getField(fieldName);
      trackingEvent.put(fieldName, value);
    }
  }

  /**
   * Get field from source record
   *
   * @param fieldName field name
   * @return value
   */
  protected Object getField(String fieldName) {
    Method method = findMethod(fieldName);
    Object value;
    try {
      value = method.invoke(this);
    } catch (NullPointerException | IllegalAccessException | InvocationTargetException e) {
      String message = String.format("%s invoke method %s failed, raw message %s", fieldName, method, this.sourceRecord);
      throw new IllegalArgumentException(message, e);
    }

    return value;
  }

  private Method findMethod(String fieldName) {
    if (FIELD_GET_METHOD_CACHE.containsKey(fieldName)) {
      return FIELD_GET_METHOD_CACHE.get(fieldName);
    }
    String methodName = FIELD_GET_METHOD_MAP_FUNCTION.apply(fieldName);
    Method method = findMethodByName(methodName);
    Validate.notNull(method, String.format("cannot find method %s, raw message %s", methodName, this.sourceRecord));
    FIELD_GET_METHOD_CACHE.put(fieldName, method);
    return FIELD_GET_METHOD_CACHE.get(fieldName);
  }

  private Method findMethodByName(String getMethodName) {
    Method getMethod = null;
    for (Method declaredMethod : UTPChocolateEmailClickTransformer.class.getDeclaredMethods()) {
      if (declaredMethod.getName().equals(getMethodName)) {
        getMethod = declaredMethod;
        break;
      }
    }
    return getMethod;
  }

  protected String getEventId() {
    return applicationPayload.getOrDefault("utpid", StringConstants.EMPTY);
  }

  protected String getProducerEventId() {
    return getEventId();
  }

  protected long getEventTs() {
    return eventTs;
  }

  protected long getProducerEventTs() {
    return getEventTs();
  }

  protected String getRlogId() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.RLOGID);
  }

  protected String getTrackingId() {
    return queryParams.getFirst(TransformerConstants.TRACKING_ID);
  }

  @SuppressWarnings("UnstableApiUsage")
  protected long getUserId() {
    Utf8 userId = (Utf8) sourceRecord.get(USER_ID);
    if (userId == null) {
      return 0L;
    }
    Long parse = Longs.tryParse(userId.toString());
    return parse != null ? parse : 0L;
  }

  protected String getPublicUserId() {
    return null;
  }

  @SuppressWarnings("UnstableApiUsage")
  protected long getEncryptedUserId() {
    String encryptedUserId = sojTags.get(TransformerConstants.EMID);
    if (encryptedUserId == null) {
      encryptedUserId = queryParams.getFirst(TransformerConstants.BEST_GUESS_USER_ID);
    }

    if (encryptedUserId == null) {
      return 0L;
    }

    Long parse = Longs.tryParse(encryptedUserId);
    return parse != null ? parse : 0L;
  }

  protected String getGuid() {
    Utf8 guid = (Utf8) sourceRecord.get(TransformerConstants.GUID);
    return guid.toString();
  }

  protected String getIdfa() {
    return applicationPayload.get(TransformerConstants.IDFA);
  }

  protected String getGadid() {
    return applicationPayload.get(TransformerConstants.GADID);
  }

  protected String getDeviceId() {
    return applicationPayload.get(TransformerConstants.UDID);
  }

  protected String getChannelType() {
    return channelType.getValue();
  }

  protected String getActionType() {
    return actionTypeEnum.getValue();
  }

  protected String getPartner() {
    String mkpid = queryParams.getFirst("mkpid");
    if (StringUtils.isEmpty(mkpid)) {
      return null;
    }
    return EmailPartnerIdEnum.parse(mkpid);
  }

  protected String getCampaignId() {
    if (channelType == MRKT_EMAIL || channelType == MRKT_MESSAGE_CENTER) {
      return applicationPayload.get(TransformerConstants.SEGNAME);
    }
    if (channelType == GCX_EMAIL || channelType == GCX_MESSAGE_CENTER) {
      return StringConstants.EMPTY;
    }
    return PulsarParseUtils.substring(applicationPayload.get("emsid"), "e", ".mle");
  }

  protected String getRotationId() {
    return null;
  }

  @SuppressWarnings("UnstableApiUsage")
  protected int getSiteId() {
    Utf8 siteId = (Utf8) sourceRecord.get(TransformerConstants.SITE_ID);
    if (siteId == null) {
      return 0;
    }
    Integer parse = Ints.tryParse(siteId.toString());
    return parse != null ? parse : 0;
  }

  protected String getUrl() {
    return url;
  }

  protected String getReferer() {
    if ("2".equals(schemaVersion)) {
      return GenericRecordUtils.getStringFieldOrNull(sourceRecord, "referrer");
    } else {
      return applicationPayload.get(REFERER);
    }
  }

  protected String getUserAgent() {
    if ("2".equals(schemaVersion)) {
      return clientData.get(TransformerConstants.AGENT);
    } else {
      return applicationPayload.get(TransformerConstants.AGENT);
    }
  }

  protected String getDeviceFamily() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.DEVICE_FAMILY);
  }

  protected String getDeviceType() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.DEVICE_TYPE);
  }

  protected String getBrowserVersion() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.BROWSER_VERSION);
  }

  protected String getBrowserFamily() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.BROWSER_FAMILY);
  }

  protected String getOsFamily() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.OS_FAMILY);
  }

  protected String getOsVersion() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.ENRICHED_OS_VERSION);
  }

  protected String getAppId() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.APP_ID);
  }

  protected String getAppVersion() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.APP_VERSION);
  }

  protected String getService() {
    return ServiceEnum.CHOCOLATE.getValue();
  }

  protected String getServer() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.WEB_SERVER);
  }

  protected String getRemoteIp() {
    if ("2".equals(schemaVersion)) {
      return clientData.getOrDefault(FORWARDED_FOR, StringConstants.EMPTY);
    } else {
      return applicationPayload.getOrDefault(REMOTE_IP, StringConstants.EMPTY);
    }
  }

  protected int getPageId() {
    return pageId;
  }

  @SuppressWarnings("UnstableApiUsage")
  protected int getGeoId() {
    if (!applicationPayload.containsKey(TransformerConstants.UC)) {
      return 0;
    }
    Integer geoId = Ints.tryParse(applicationPayload.get(TransformerConstants.UC));
    return geoId == null ? 0 : geoId;
  }

  protected boolean getIsBot() {
    return sourceTopic.equals(BEHAVIOR_PULSAR_MISC_BOT);
  }

  protected Map<String, String> getPayload() {
    Map<String, String> payload = getDataQualityPayload();
    // drop unused tags
    payload.putAll(getEmailTags());
    // add soj tags
    payload.putAll(sojTags);
    // add uep tags
    Map<String, String> uepPayload =
            UepPayloadHelper.getInstance().getUepPayload(url, actionTypeEnum, channelType);
    if (MapUtils.isNotEmpty(uepPayload)) {
      payload.putAll(uepPayload);
    }
    return payload;
  }

  private Map<String, String> getEmailTags() {
    Map<String, String> payload = new HashMap<>();

    for (String key : EMAIL_TAG_PARAM_MAP.keySet()) {
      if (applicationPayload.containsKey(key)) {
        payload.put(key, applicationPayload.get(key));
      }
    }

    for (String key : Arrays.asList("fbprefetch", "url_mpre", "bs")) {
      if (applicationPayload.containsKey(key)) {
        payload.put(key, applicationPayload.get(key));
      }
    }

    String nonSojTag = "nonSojTag";
    if (applicationPayload.containsKey(nonSojTag)) {
      payload.put(nonSojTag, applicationPayload.get(nonSojTag));
    }

    payload.put("cobrand", GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, TransformerConstants.COBRAND));
    payload.put("seqNum", GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, TransformerConstants.SEQ_NUM));
    Long sessionSkey = GenericRecordUtils.getLongFieldOrNull(sourceRecord, TransformerConstants.SESSION_SKEY);
    if (sessionSkey != null) {
      payload.put("sessionSkey", String.valueOf(sessionSkey));
    }
    payload.put("sessionId", GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, TransformerConstants.SESSION_ID));
    // Get xt from url as this tag hasn't been written to Pulsar
    return payload;
  }

  private Map<String, String> getDataQualityPayload() {
    Map<String, String> dqTags = new HashMap<>();
    dqTags.put("dq-pulsar-topic", String.valueOf(sourceTopic));
    dqTags.put("dq-pulsar-partition", String.valueOf(partition));
    dqTags.put("dq-pulsar-offset", String.valueOf(offset));
    dqTags.put("dq-pulsar-eventCreateTimestamp", String.valueOf(sourceRheosEvent.getEventCreateTimestamp()));
    dqTags.put("dq-pulsar-eventSendTimestamp", String.valueOf(sourceRheosEvent.getEventSentTimestamp()));
    if (sourceRheosEvent.getEventId() != null) {
      dqTags.put("dq-pulsar-eventId", sourceRheosEvent.getEventId());
    }
    return dqTags;
  }

}
