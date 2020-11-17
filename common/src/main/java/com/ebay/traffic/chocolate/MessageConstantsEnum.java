package com.ebay.traffic.chocolate;

public enum MessageConstantsEnum {
    RECORD_VERSION_MAJOR("record.version.major"),
    RECORD_VERSION_MINOR("record.version.minor"),
    RECORD_TYPE("record.type"), /*com.ebay.messagetracker.optimizereport*/
    ENCRYPTED_USER_ID("encrypted.userid"),
    USER_ID("userid"),
    SITE_ID("siteid"),
    CAMPAIGN_CODE("campaign.code"),
    SEGMENT("segment"),
    RUN_DATE("rundate"),
    INTERACTION_TYPE("interaction.type"), /* push, pull, open, click, hover, view*/
    CHANNEL_NAME("channel.name"),
    STATUS("status"),
    TYPE_ID("typeid"),
    SPEC_ID("specid"),
    TEMPLATE_ID("templateid"),
    TO_EMAIL("to-email"),
    FROM_EMAIL("from-email"),
    ANNOTATION_PREFIX("annotation"),
    SUB_STATUS("EventSubType"),
    REASON_TYPE("ReasonType"),
    TIMESTAMP_CREATED("timestamp.created"),
    TIMESTAMP_UPDATED("timestamp.updated"),
    PLACEMENT_ID("plmt.id"),
    MESSAGE_ID("mesg.id"),
    SEGMENT_ID("segment.id"),
    GUID("guid"),
    EVENT_TIMESTAMP("event.ts"),
    PAGE_ID("page.id"),
    SEQ_NO("seqnum"),
    IMPRERSSION_ID("iid"),
    CLICK_ID("clkid"),
    MOB_TRK_ID("mob.trk.id"),
    IS_UEP("isUEP"),
    CANVAS_ID("cnv.id"),
    CREATIVE_ID("creative.id"),
    CREATIVE_VARIATION_ID("creative.variation.id"),
    MESG_LIST("mesg.list"),
    SESSION_KEY("session.key"),
    MESSAGE_VERSION("version"),
    TRACKING_ID("tracking.id");
    
    private final String mValue;

    MessageConstantsEnum(String value) {
        mValue = value;
    }

    public String getValue() {
        return mValue;
    }
}
