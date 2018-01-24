package com.ebay.traffic.chocolate.cappingrules.dto;

/**
 * Created by yimeng on 11/14/17.
 */
public class EventSchema extends BaseEvent{

  private Long snapshotId;
  private Long campaignId;
  private Long partnerId;
  private String requestHeaders;
  private Long requestTimestamp;
  private String httpMethod;
  private String snid;
  private Integer month;

  private Boolean isMobile;
  private Boolean filterPassed;
  private String filterFailedRule;
  private Boolean isImpressed;
  private byte[] impRowIdentifier;
  private Boolean cappingPassed;
  private String cappingFailedRule;

  public EventSchema() {
    super();
  }

  public EventSchema(byte[] rowIdentifier, Long snapshotId, Long campaignId, Long partnerId, String channelAction,
                     String channelType, String requestHeaders, Long requestTimestamp, String snid, Integer month,
                     Boolean isMobile, Boolean filterPassed, String filterFailedRule, Boolean isImpressed,
                     byte[] impRowIdentifier, Boolean cappingPassed, String cappingFailedRule) {
    super(rowIdentifier, channelType, channelAction);
    this.snapshotId = snapshotId;
    this.campaignId = campaignId;
    this.partnerId = partnerId;
    this.requestHeaders = requestHeaders;
    this.requestTimestamp = requestTimestamp;
    this.snid = snid;
    this.month = month;
    this.isMobile = isMobile;
    this.filterPassed = filterPassed;
    this.filterFailedRule = filterFailedRule;
    this.isImpressed = isImpressed;
    this.impRowIdentifier = impRowIdentifier;
    this.cappingPassed = cappingPassed;
    this.cappingFailedRule = cappingFailedRule;
  }

  public Long getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(Long snapshotId) {
    this.snapshotId = snapshotId;
  }

  public Long getCampaignId() {
    return campaignId;
  }

  public void setCampaignId(Long campaignId) {
    this.campaignId = campaignId;
  }

  public Long getPartnerId() {
    return partnerId;
  }

  public void setPartnerId(Long partnerId) {
    this.partnerId = partnerId;
  }

  public String getRequestHeaders() {
    return requestHeaders;
  }

  public void setRequestHeaders(String requestHeaders) {
    this.requestHeaders = requestHeaders;
  }

  public Long getRequestTimestamp() {
    return requestTimestamp;
  }

  public void setRequestTimestamp(Long requestTimestamp) {
    this.requestTimestamp = requestTimestamp;
  }

  public String getHttpMethod() {
    return httpMethod;
  }

  public void setHttpMethod(String httpMethod) {
    this.httpMethod = httpMethod;
  }

  public String getSnid() {
    return snid;
  }

  public void setSnid(String snid) {
    this.snid = snid;
  }

  public Integer getMonth() {
    return month;
  }

  public void setMonth(Integer month) {
    this.month = month;
  }

  public Boolean getMobile() {
    return isMobile;
  }

  public void setMobile(Boolean mobile) {
    isMobile = mobile;
  }

  public Boolean getFilterPassed() {
    return filterPassed;
  }

  public void setFilterPassed(Boolean filterPassed) {
    this.filterPassed = filterPassed;
  }

  public String getFilterFailedRule() {
    return filterFailedRule;
  }

  public void setFilterFailedRule(String filterFailedRule) {
    this.filterFailedRule = filterFailedRule;
  }

  public Boolean getImpressed() {
    return isImpressed;
  }

  public void setImpressed(Boolean impressed) {
    isImpressed = impressed;
  }

  public byte[] getImpRowIdentifier() {
    return impRowIdentifier;
  }

  public void setImpRowIdentifier(byte[] impRowIdentifier) {
    this.impRowIdentifier = impRowIdentifier;
  }

  public Boolean getCappingPassed() {
    return cappingPassed;
  }

  public void setCappingPassed(Boolean cappingPassed) {
    this.cappingPassed = cappingPassed;
  }

  public String getCappingFailedRule() {
    return cappingFailedRule;
  }

  public void setCappingFailedRule(String cappingFailedRule) {
    this.cappingFailedRule = cappingFailedRule;
  }
}
