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
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

public class UTPRoverEventTransformer {
  private final String sourceTopic;
  private final long offset;
  private final GenericRecord sourceRecord;
  private final RheosEvent sourceRheosEvent;
  private Integer pageId;
  protected Map<String, String> applicationPayload;
  private String urlQueryString;
  private String channelId;
  private ChannelTypeEnum channelType;
  private ActionTypeEnum actionTypeEnum;
  private Map<String, String> sojTags;
  private final boolean isValid;

  private static final UepPayloadHelper UEP_PAYLOAD_HELPER = new UepPayloadHelper();

  private static final String USER_ID = "userId";
  private static final int PAGE_ID_ROVER_CLICK = 3084;
  private static final int PAGE_ID_EMAIL_OPEN = 3962;
  private static final String PAGE_NAME_ROVER_EMAIL_OPEN = "roveropen";
  private static final String SITE_EMAIL_CHANNEL_ID = "7";
  private static final String MRKT_EMAIL_CHANNEL_ID = "8";
  public static final String BEHAVIOR_PULSAR_MISC_BOT = "behavior.pulsar.misc.bot";
  public static final String REFERER = "Referer";
  private static final String ROVER_HOST = "https://rover.ebay.com";
  public static final String TOPIC = "topic";

  public static final String REMOTE_IP = "RemoteIP";

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

  public UTPRoverEventTransformer(String sourceTopic, long offset, GenericRecord sourceRecord, RheosEvent sourceRheosEvent) {
    this.sourceTopic = sourceTopic;
    this.offset = offset;
    this.sourceRecord = sourceRecord;
    this.sourceRheosEvent = sourceRheosEvent;
    this.isValid = validate();
    if (this.isValid) {
      initFields();
    }
  }

  public boolean isValid() {
    return isValid;
  }

  @SuppressWarnings("unchecked")
  private boolean validate() {
    pageId = (Integer) sourceRecord.get(TransformerConstants.PAGE_ID);
    if (pageId == null) {
      SherlockioMetrics.getInstance().meter("NoPageId", 1, Field.of(TOPIC, sourceTopic));
      return false;
    }
    if (pageId != PAGE_ID_ROVER_CLICK && pageId != PAGE_ID_EMAIL_OPEN) {
      SherlockioMetrics.getInstance().meter("NotEmailClickOpen", 1, Field.of(TOPIC, sourceTopic));
      return false;
    }

    applicationPayload = GenericRecordUtils.getMap(sourceRecord, TransformerConstants.APPLICATION_PAYLOAD);
    urlQueryString = parseUrlQueryString();
    if (urlQueryString == null) {
      SherlockioMetrics.getInstance().meter("NoUrlQueryString", 1, Field.of(TOPIC, sourceTopic));
      return false;
    }

    channelId = PulsarParseUtils.getChannelIdFromUrlQueryString(urlQueryString);
    if (StringUtils.isEmpty(channelId)) {
      SherlockioMetrics.getInstance().meter("NoChannelId", 1, Field.of(TOPIC, sourceTopic));
      return false;
    }

    channelType = parseChannelType();
    if (channelType == null) {
      SherlockioMetrics.getInstance().meter("NotEmail", 1, Field.of(TOPIC, sourceTopic));
      return false;
    }

    String pageName = parsePageName();
    if (pageName == null) {
      SherlockioMetrics.getInstance().meter("NoPageName", 1, Field.of(TOPIC, sourceTopic));
      return false;
    }
    if (pageId == PAGE_ID_EMAIL_OPEN && !PAGE_NAME_ROVER_EMAIL_OPEN.equals(pageName)) {
      SherlockioMetrics.getInstance().meter("NotRoveropen", 1, Field.of(TOPIC, sourceTopic));
      return false;
    }
    return true;
  }

  private String parseUrlQueryString() {
    String querString = applicationPayload.get(TransformerConstants.URL_QUERY_STRING);
    if (querString == null) {
      Utf8 utf8 = (Utf8) sourceRecord.get(TransformerConstants.URL_QUERY_STRING);
      if (utf8 != null) {
        querString = utf8.toString();
      }
    }
    return querString;
  }
  
  private String parsePageName() {
    Utf8 utf8 = (Utf8) sourceRecord.get(TransformerConstants.PAGE_NAME);
    return utf8 == null ? null : utf8.toString();
  }

  private ChannelTypeEnum parseChannelType() {
    if (channelId == null) {
      return null;
    }
    if (SITE_EMAIL_CHANNEL_ID.equals(channelId)) {
      return ChannelTypeEnum.SITE_EMAIL;
    }
    if (MRKT_EMAIL_CHANNEL_ID.equals(channelId)) {
      return ChannelTypeEnum.MRKT_EMAIL;
    }
    return null;
  }

  private ActionTypeEnum parseActionType() {
    return pageId == PAGE_ID_ROVER_CLICK ? ActionTypeEnum.CLICK : ActionTypeEnum.OPEN;
  }

  /**
   * Init fields that will be used in multi methods.
   */
  private void initFields() {
    actionTypeEnum = parseActionType();
    sojTags = PulsarParseUtils.getSojTagsFromUrlQueryString(urlQueryString);
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
    for (Method declaredMethod : UTPRoverEventTransformer.class.getDeclaredMethods()) {
      if (declaredMethod.getName().equals(getMethodName)) {
        getMethod = declaredMethod;
        break;
      }
    }
    return getMethod;
  }

  protected String getEventId() {
    return UUID.randomUUID().toString();
  }

  protected String getProducerEventId() {
    if (channelType == ChannelTypeEnum.SITE_EMAIL) {
      return applicationPayload.getOrDefault(TransformerConstants.EUID, StringConstants.EMPTY);
    }
    return StringConstants.EMPTY;
  }

  protected long getEventTs() {
    return  (Long) sourceRecord.get(TransformerConstants.EVENT_TIMESTAMP);
  }

  protected long getProducerEventTs() {
    return  (Long) sourceRecord.get(TransformerConstants.EVENT_TIMESTAMP);
  }

  protected String getRlogId() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.RLOGID);
  }
  
  protected String getTrackingId() {
    return PulsarParseUtils.getParameterFromUrlQueryString(urlQueryString, TransformerConstants.TRACKING_ID);
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
      encryptedUserId = PulsarParseUtils.getParameterFromUrlQueryString(urlQueryString,
          TransformerConstants.BEST_GUESS_USER_ID);
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
    String partnerId = PulsarParseUtils.getPartnerIdFromUrlQueryString(urlQueryString);
    if (StringUtils.isEmpty(partnerId)) {
      return null;
    }
    return EmailPartnerIdEnum.parse(partnerId);
  }

  protected String getCampaignId() {
    if (channelType == ChannelTypeEnum.MRKT_EMAIL) {
      return applicationPayload.get(TransformerConstants.SEGNAME);
    }
    
    return PulsarParseUtils.substring(applicationPayload.get(TransformerConstants.SID), "e", ".mle");
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
    return String.format("%s%s", ROVER_HOST, urlQueryString);
  }

  protected String getReferer() {
    return applicationPayload.get(REFERER);
  }

  protected String getUserAgent() {
    return applicationPayload.get(TransformerConstants.AGENT);
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
    return ServiceEnum.ROVER.getValue();
  }

  protected String getServer() {
    return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.WEB_SERVER);
  }

  protected String getRemoteIp() {
    return applicationPayload.getOrDefault(REMOTE_IP, StringConstants.EMPTY);
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
    Map<String, String> payload = new HashMap<>();
    // drop unused tags
    payload.putAll(getRequiredPayload());
    // add soj tags
    payload.putAll(sojTags);
    // add uep tags
    Map<String, String> uepPayload = UEP_PAYLOAD_HELPER.getUepPayload(String.format("%s%s", ROVER_HOST, urlQueryString), actionTypeEnum);
    if (MapUtils.isNotEmpty(uepPayload)) {
      payload.putAll(uepPayload);
    }
    // add data quality tags
    Map<String, String> debugPayload = getDataQualityPayload();
    payload.putAll(debugPayload);
    return payload;
  }

  private Map<String, String> getRequiredPayload() {
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

    payload.put("cobrand", GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, TransformerConstants.COBRAND));

    return payload;
  }

  private Map<String, String> getDataQualityPayload() {
    Map<String, String> dqTags = new HashMap<>();
    dqTags.put("dq-pulsar-offset", String.valueOf(offset));
    dqTags.put("dq-pulsar-eventCreateTimestamp", String.valueOf(sourceRheosEvent.getEventCreateTimestamp()));
    dqTags.put("dq-pulsar-eventSendTimestamp", String.valueOf(sourceRheosEvent.getEventSentTimestamp()));
    if (sourceRheosEvent.getEventId() != null) {
      dqTags.put("dq-pulsar-eventId", sourceRheosEvent.getEventId());
    }
    return dqTags;
  }

}
