package com.ebay.traffic.chocolate.flink.nrt.transformer;

import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.utp.UepPayloadHelper;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.platform.raptor.raptordds.parsers.UserAgentParser;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.TransformerConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.DeviceInfoParser;
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
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

public class UTPPathfinderEventTransformer {
    private final String sourceTopic;
    private final int partition;
    private final long offset;
    private final GenericRecord sourceRecord;
    private final RheosEvent sourceRheosEvent;
    private final String schemaVersion;
    private Integer pageId;
    protected Map<String, String> applicationPayload;
    protected Map<String, String> clientData;
    private String urlQueryString;
    private ChannelTypeEnum channelType;
    private ActionTypeEnum actionTypeEnum;
    private final boolean isValid;

    private static final String USER_ID = "userId";

    // 3203626 is only on app download/install (does not mean the user has opened the app)
    private static final int PAGE_ID_APP_DOWNLOAD = 3203626;
    // 3203764 is the first time the user launches the app after an app upgrade
    private static final int PAGE_ID_UPGRADE_LAUNCH = 3203764;
    // 2050535 is only first launch after a download/install
    private static final int PAGE_ID_FIRST_LAUNCH = 2050535;

    private static final String PAGE_NAME_BATCH_TRACK = "Ginger.v1.batchtrack.POST";

    public static final String BEHAVIOR_PULSAR_TRACKING_INSTALL_BOT = "behavior.pulsar.customized.tracking.install.bot";
    public static final String TOPIC = "topic";
    private static final String FORWARDED_FOR = "ForwardedFor";

    private static final String GET_METHOD_PREFIX = "get";

    private static List<String> INSTALL_PAYLOAD_TAG_LIST = Arrays.asList("usecase", "formFactor", "dm", "dn", "rlutype", "deviceAdvertisingOptOut", "ec", "mlch", "mdnd", "maup", "mrollp", "androidid", "referrer", "uit", "carrier", "rq");

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

    public UTPPathfinderEventTransformer(String sourceTopic, int partition, long offset, GenericRecord sourceRecord, RheosEvent sourceRheosEvent, String schemaVersion) {
        this.sourceTopic = sourceTopic;
        this.partition = partition;
        this.offset = offset;
        this.sourceRecord = sourceRecord;
        this.sourceRheosEvent = sourceRheosEvent;
        this.schemaVersion = schemaVersion;
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
        if (pageId != PAGE_ID_APP_DOWNLOAD && pageId != PAGE_ID_FIRST_LAUNCH && pageId != PAGE_ID_UPGRADE_LAUNCH) {
            SherlockioMetrics.getInstance().meter("NotInstallEvent", 1, Field.of(TOPIC, sourceTopic));
            return false;
        }

        applicationPayload = GenericRecordUtils.getMap(sourceRecord, TransformerConstants.APPLICATION_PAYLOAD);
        clientData = GenericRecordUtils.getMap(sourceRecord, TransformerConstants.CLIENT_DATA);
        urlQueryString = parseUrlQueryString();
        if (urlQueryString == null) {
            SherlockioMetrics.getInstance().meter("NoUrlQueryString", 1, Field.of(TOPIC, sourceTopic));
            return false;
        }

        if (pageId == PAGE_ID_APP_DOWNLOAD || pageId == PAGE_ID_FIRST_LAUNCH || pageId == PAGE_ID_UPGRADE_LAUNCH) {
            channelType = ChannelTypeEnum.GENERIC;

            String pageName = parsePageName();
            if (pageName == null) {
                SherlockioMetrics.getInstance().meter("NoPageName", 1, Field.of(TOPIC, sourceTopic));
                return false;
            }
            if (!PAGE_NAME_BATCH_TRACK.equals(pageName)) {
                SherlockioMetrics.getInstance().meter("NotInstallFromBatchTrack", 1, Field.of(TOPIC, sourceTopic));
                return false;
            }
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

    private ActionTypeEnum parseActionType() {
        switch (pageId) {
            case PAGE_ID_APP_DOWNLOAD:
                return ActionTypeEnum.APPDOWNLOAD;
            case PAGE_ID_FIRST_LAUNCH:
                return ActionTypeEnum.LAUNCH;
            case PAGE_ID_UPGRADE_LAUNCH:
                return ActionTypeEnum.UPGRADE_LAUNCH;
            default:
                throw new IllegalArgumentException(String.format("Invalid pageId %d", pageId));
        }
    }

    /**
     * Init fields that will be used in multi methods.
     */
    private void initFields() {
        actionTypeEnum = parseActionType();
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
        for (Method declaredMethod : UTPPathfinderEventTransformer.class.getDeclaredMethods()) {
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
        return StringConstants.EMPTY;
    }

    protected long getEventTs() {
        return  (Long) sourceRecord.get(TransformerConstants.EVENT_TIMESTAMP);
    }

    protected long getProducerEventTs() {
        return getEventTs();
    }

    protected String getRlogId() {
        return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.RLOGID);
    }

    protected String getTrackingId() {
        return null;
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
        String encryptedUserId = applicationPayload.get(TransformerConstants.BEST_GUESS_USER_ID);

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
        return applicationPayload.get(TransformerConstants.MPPID);
    }

    protected String getCampaignId() {
        return null;
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
        return null;
    }

    protected String getReferer() {
        return GenericRecordUtils.getStringFieldOrNull(sourceRecord, TransformerConstants.REFERRER);
    }

    protected String getUserAgent() {
        return clientData.get(TransformerConstants.AGENT);
    }

    protected String getDeviceFamily() {
        return getDeviceInfoParser().getDeviceFamily();
    }

    protected String getDeviceType() {
        return getDeviceInfoParser().getDeviceType();
    }

    protected String getBrowserVersion() {
        return getDeviceInfoParser().getBrowserVersion();
    }

    protected String getBrowserFamily() {
        return getDeviceInfoParser().getBrowserFamily();
    }

    protected String getOsFamily() {
        return getDeviceInfoParser().getOsFamily();
    }

    protected String getOsVersion() {
        return getDeviceInfoParser().getOsVersion();
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
        return clientData.getOrDefault(FORWARDED_FOR, StringConstants.EMPTY);
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
        return sourceTopic.equals(BEHAVIOR_PULSAR_TRACKING_INSTALL_BOT);
    }

    protected Map<String, String> getPayload() {
        Map<String, String> payload = getDataQualityPayload();
        if (actionTypeEnum == actionTypeEnum.APPDOWNLOAD || actionTypeEnum == actionTypeEnum.LAUNCH ||
                actionTypeEnum == actionTypeEnum.UPGRADE_LAUNCH) {
            payload.put("p", String.valueOf(pageId));

            for (String key : INSTALL_PAYLOAD_TAG_LIST) {
                if (applicationPayload.containsKey(key)) {
                    payload.put(key, applicationPayload.get(key));
                }
            }

            payload.put("cobrand", GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, TransformerConstants.COBRAND));
            payload.put("eventFamily", GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, TransformerConstants.EVENT_FAMILY));
            payload.put("eventAction", GenericRecordUtils.getStringFieldOrEmpty(sourceRecord, TransformerConstants.EVENT_ACTION));
        }

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

    private UserAgentInfo getUserAgentInfo() {
        String userAgent = clientData.get(TransformerConstants.AGENT);
        return new UserAgentParser().parse(userAgent);
    }

    private DeviceInfoParser getDeviceInfoParser() {
        UserAgentInfo userAgentInfo = getUserAgentInfo();
        DeviceInfoParser deviceInfoParser = new DeviceInfoParser().parse(userAgentInfo);
        return deviceInfoParser;
    }

}