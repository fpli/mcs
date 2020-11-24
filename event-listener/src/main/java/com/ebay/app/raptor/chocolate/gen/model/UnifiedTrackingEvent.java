/*
 * service-tracking-events
 * marketing tracking compoent to receive marketing events
 *
 * OpenAPI spec version: 1.0.0
 *
 *
 * NOTE: This class is auto generated by the swagger code generator program.
 * https://github.com/swagger-api/swagger-codegen.git
 * Do not edit the class manually.
 */

package com.ebay.app.raptor.chocolate.gen.model;

import java.util.Objects;
import java.util.Arrays;
import java.io.Serializable;
import io.swagger.annotations.*;

import io.swagger.v3.oas.annotations.media.Schema;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.*;

/**
 * UnifiedTrackingEvent
 */


@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2020-11-13T18:13:47.457+08:00[Asia/Shanghai]")
@JsonPropertyOrder({ "producerEventId","producerEventTs","rlogId","trackingId","userId","publicUserId","encryptedUserId","guid","idfa","gadid","deviceId","channelType","actionType","partner","campaignId","siteId","url","referer","userAgent","service","server","remoteIp","pageId","geoId","payload" })
@JsonIgnoreProperties(ignoreUnknown = true)


public class UnifiedTrackingEvent implements Serializable {

  private static final long serialVersionUID = 1L;



  @JsonProperty("producerEventId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String producerEventId = null;
  @JsonProperty("producerEventTs")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long producerEventTs = null;
  @JsonProperty("rlogId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String rlogId = null;
  @JsonProperty("trackingId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String trackingId = null;
  @JsonProperty("userId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long userId = null;
  @JsonProperty("publicUserId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String publicUserId = null;
  @JsonProperty("encryptedUserId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long encryptedUserId = null;
  @JsonProperty("guid")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String guid = null;
  @JsonProperty("idfa")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String idfa = null;
  @JsonProperty("gadid")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String gadid = null;
  @JsonProperty("deviceId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String deviceId = null;
  @JsonProperty("channelType")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String channelType = null;
  @JsonProperty("actionType")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String actionType = null;
  @JsonProperty("partner")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String partner = null;
  @JsonProperty("campaignId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String campaignId = null;
  @JsonProperty("siteId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Integer siteId = null;
  @JsonProperty("url")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String url = null;
  @JsonProperty("referer")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String referer = null;
  @JsonProperty("userAgent")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String userAgent = null;
  @JsonProperty("service")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String service = null;
  @JsonProperty("server")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String server = null;
  @JsonProperty("remoteIp")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String remoteIp = null;
  @JsonProperty("pageId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Integer pageId = null;
  @JsonProperty("geoId")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Integer geoId = null;
  @JsonProperty("payload")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Map<String, String> payload = null;

  /**
   * Event id at producer level
   * @return producerEventId
   **/
  @ApiModelProperty(example = "18c244e5-0ee3-4b0e-b283-4efb379bee44", value = "Event id at producer level")
  public String getProducerEventId() {
    return producerEventId;
  }

  public void setProducerEventId(String producerEventId) {
    this.producerEventId = producerEventId;
  }
  /**
   * Timestamp when the producer calls the tracking service
   * @return producerEventTs
   **/
  @ApiModelProperty(example = "1604463171000", value = "Timestamp when the producer calls the tracking service")
  public Long getProducerEventTs() {
    return producerEventTs;
  }

  public void setProducerEventTs(Long producerEventTs) {
    this.producerEventTs = producerEventTs;
  }
  /**
   * rlogId
   * @return rlogId
   **/
  @ApiModelProperty(example = "t6klaook%60b0%3D%3C%3Dpieojbnkmcc4%3B(5574425-1759175a140-0x1902", value = "rlogId")
  public String getRlogId() {
    return rlogId;
  }

  public void setRlogId(String rlogId) {
    this.rlogId = rlogId;
  }
  /**
   * Id used to associate the marketing system behavior and the user behavior
   * @return trackingId
   **/
  @ApiModelProperty(example = "0AD3335D-36089192FCC-017529D1F00D-00000000027EEF2B", value = "Id used to associate the marketing system behavior and the user behavior")
  public String getTrackingId() {
    return trackingId;
  }

  public void setTrackingId(String trackingId) {
    this.trackingId = trackingId;
  }
  /**
   * Oracle id
   * @return userId
   **/
  @ApiModelProperty(example = "1626162", value = "Oracle id")
  public Long getUserId() {
    return userId;
  }

  public void setUserId(Long userId) {
    this.userId = userId;
  }
  /**
   * Public user id
   * @return publicUserId
   **/
  @ApiModelProperty(example = "1626162", value = "Public user id")
  public String getPublicUserId() {
    return publicUserId;
  }

  public void setPublicUserId(String publicUserId) {
    this.publicUserId = publicUserId;
  }
  /**
   * Encrypted user id
   * @return encryptedUserId
   **/
  @ApiModelProperty(example = "542525556", value = "Encrypted user id")
  public Long getEncryptedUserId() {
    return encryptedUserId;
  }

  public void setEncryptedUserId(Long encryptedUserId) {
    this.encryptedUserId = encryptedUserId;
  }
  /**
   * guid
   * @return guid
   **/
  @ApiModelProperty(example = "8b34ef1d1740a4d724970d78eec8ee4c", value = "guid")
  public String getGuid() {
    return guid;
  }

  public void setGuid(String guid) {
    this.guid = guid;
  }
  /**
   * ios identifier
   * @return idfa
   **/
  @ApiModelProperty(example = "30255BCE-4CDA-4F62-91DC-4758FDFF8512", value = "ios identifier")
  public String getIdfa() {
    return idfa;
  }

  public void setIdfa(String idfa) {
    this.idfa = idfa;
  }
  /**
   * android ad identifier
   * @return gadid
   **/
  @ApiModelProperty(example = "035911ea-467d-4056-903b-65cf44f5633b", value = "android ad identifier")
  public String getGadid() {
    return gadid;
  }

  public void setGadid(String gadid) {
    this.gadid = gadid;
  }
  /**
   * This is ebay mobile app generated unique id for each app installation. It is usually used in request header X-EBAY3PP-DEVICE-ID for each API call.
   * @return deviceId
   **/
  @ApiModelProperty(example = "035911ea-467d-4056-903b-65cf44f5633b", value = "This is ebay mobile app generated unique id for each app installation. It is usually used in request header X-EBAY3PP-DEVICE-ID for each API call.")
  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }
  /**
   * Channel identifier
   * @return channelType
   **/
  @ApiModelProperty(example = "MRKT_EMAIL", value = "Channel identifier")
  public String getChannelType() {
    return channelType;
  }

  public void setChannelType(String channelType) {
    this.channelType = channelType;
  }
  /**
   * Action identifier
   * @return actionType
   **/
  @ApiModelProperty(example = "CLICK", value = "Action identifier")
  public String getActionType() {
    return actionType;
  }

  public void setActionType(String actionType) {
    this.actionType = actionType;
  }
  /**
   * For epn publisher id, for display partner id. For CM, we have adobe, SMTP, Send Grid...
   * @return partner
   **/
  @ApiModelProperty(example = "16352125", value = "For epn publisher id, for display partner id. For CM, we have adobe, SMTP, Send Grid...")
  public String getPartner() {
    return partner;
  }

  public void setPartner(String partner) {
    this.partner = partner;
  }
  /**
   * Campaign id
   * @return campaignId
   **/
  @ApiModelProperty(example = "2231121", value = "Campaign id")
  public String getCampaignId() {
    return campaignId;
  }

  public void setCampaignId(String campaignId) {
    this.campaignId = campaignId;
  }
  /**
   * eBay site id
   * @return siteId
   **/
  @ApiModelProperty(example = "0", value = "eBay site id")
  public Integer getSiteId() {
    return siteId;
  }

  public void setSiteId(Integer siteId) {
    this.siteId = siteId;
  }
  /**
   * Tracking url
   * @return url
   **/
  @ApiModelProperty(example = "https://www.ebay.com/i/1312121?mkevt=1&mkcid=2", value = "Tracking url")
  public String getUrl() {
    return url;
  }

  public void setUrl(String url) {
    this.url = url;
  }
  /**
   * Referer of the tracking request
   * @return referer
   **/
  @ApiModelProperty(example = "https://www.google.com/", value = "Referer of the tracking request")
  public String getReferer() {
    return referer;
  }

  public void setReferer(String referer) {
    this.referer = referer;
  }
  /**
   * Client full user agent name
   * @return userAgent
   **/
  @ApiModelProperty(example = "Chrome: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36", value = "Client full user agent name")
  public String getUserAgent() {
    return userAgent;
  }

  public void setUserAgent(String userAgent) {
    this.userAgent = userAgent;
  }
  /**
   * Service name of the producer
   * @return service
   **/
  @ApiModelProperty(example = "Chocolate", value = "Service name of the producer")
  public String getService() {
    return service;
  }

  public void setService(String service) {
    this.service = service;
  }
  /**
   * The server which is calling the tracking service
   * @return server
   **/
  @ApiModelProperty(example = "rnochocolis-123.stratus.ebay.com", value = "The server which is calling the tracking service")
  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
  }
  /**
   * End user remote IP
   * @return remoteIp
   **/
  @ApiModelProperty(example = "202.76.240.67", value = "End user remote IP")
  public String getRemoteIp() {
    return remoteIp;
  }

  public void setRemoteIp(String remoteIp) {
    this.remoteIp = remoteIp;
  }
  /**
   * Corresponding page id in sojourner
   * @return pageId
   **/
  @ApiModelProperty(example = "3084", value = "Corresponding page id in sojourner")
  public Integer getPageId() {
    return pageId;
  }

  public void setPageId(Integer pageId) {
    this.pageId = pageId;
  }
  /**
   * Geo id of the end user
   * @return geoId
   **/
  @ApiModelProperty(example = "1", value = "Geo id of the end user")
  public Integer getGeoId() {
    return geoId;
  }

  public void setGeoId(Integer geoId) {
    this.geoId = geoId;
  }
  /**
   * Tracking event payload
   * @return payload
   **/
  @ApiModelProperty(value = "Tracking event payload")
  public Map<String, String> getPayload() {
    return payload;
  }

  public void setPayload(Map<String, String> payload) {
    this.payload = payload;
  }
  @Override
  public boolean equals(java.lang.Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UnifiedTrackingEvent unifiedTrackingEvent = (UnifiedTrackingEvent) o;
    return Objects.equals(this.producerEventId, unifiedTrackingEvent.producerEventId) &&
        Objects.equals(this.producerEventTs, unifiedTrackingEvent.producerEventTs) &&
        Objects.equals(this.rlogId, unifiedTrackingEvent.rlogId) &&
        Objects.equals(this.trackingId, unifiedTrackingEvent.trackingId) &&
        Objects.equals(this.userId, unifiedTrackingEvent.userId) &&
        Objects.equals(this.publicUserId, unifiedTrackingEvent.publicUserId) &&
        Objects.equals(this.encryptedUserId, unifiedTrackingEvent.encryptedUserId) &&
        Objects.equals(this.guid, unifiedTrackingEvent.guid) &&
        Objects.equals(this.idfa, unifiedTrackingEvent.idfa) &&
        Objects.equals(this.gadid, unifiedTrackingEvent.gadid) &&
        Objects.equals(this.deviceId, unifiedTrackingEvent.deviceId) &&
        Objects.equals(this.channelType, unifiedTrackingEvent.channelType) &&
        Objects.equals(this.actionType, unifiedTrackingEvent.actionType) &&
        Objects.equals(this.partner, unifiedTrackingEvent.partner) &&
        Objects.equals(this.campaignId, unifiedTrackingEvent.campaignId) &&
        Objects.equals(this.siteId, unifiedTrackingEvent.siteId) &&
        Objects.equals(this.url, unifiedTrackingEvent.url) &&
        Objects.equals(this.referer, unifiedTrackingEvent.referer) &&
        Objects.equals(this.userAgent, unifiedTrackingEvent.userAgent) &&
        Objects.equals(this.service, unifiedTrackingEvent.service) &&
        Objects.equals(this.server, unifiedTrackingEvent.server) &&
        Objects.equals(this.remoteIp, unifiedTrackingEvent.remoteIp) &&
        Objects.equals(this.pageId, unifiedTrackingEvent.pageId) &&
        Objects.equals(this.geoId, unifiedTrackingEvent.geoId) &&
        Objects.equals(this.payload, unifiedTrackingEvent.payload);
  }

  @Override
  public int hashCode() {
    return Objects.hash(producerEventId, producerEventTs, rlogId, trackingId, userId, publicUserId, encryptedUserId, guid, idfa, gadid, deviceId, channelType, actionType, partner, campaignId, siteId, url, referer, userAgent, service, server, remoteIp, pageId, geoId, payload);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class UnifiedTrackingEvent {\n");

    sb.append("    producerEventId: ").append(toIndentedString(producerEventId)).append("\n");
    sb.append("    producerEventTs: ").append(toIndentedString(producerEventTs)).append("\n");
    sb.append("    rlogId: ").append(toIndentedString(rlogId)).append("\n");
    sb.append("    trackingId: ").append(toIndentedString(trackingId)).append("\n");
    sb.append("    userId: ").append(toIndentedString(userId)).append("\n");
    sb.append("    publicUserId: ").append(toIndentedString(publicUserId)).append("\n");
    sb.append("    encryptedUserId: ").append(toIndentedString(encryptedUserId)).append("\n");
    sb.append("    guid: ").append(toIndentedString(guid)).append("\n");
    sb.append("    idfa: ").append(toIndentedString(idfa)).append("\n");
    sb.append("    gadid: ").append(toIndentedString(gadid)).append("\n");
    sb.append("    deviceId: ").append(toIndentedString(deviceId)).append("\n");
    sb.append("    channelType: ").append(toIndentedString(channelType)).append("\n");
    sb.append("    actionType: ").append(toIndentedString(actionType)).append("\n");
    sb.append("    partner: ").append(toIndentedString(partner)).append("\n");
    sb.append("    campaignId: ").append(toIndentedString(campaignId)).append("\n");
    sb.append("    siteId: ").append(toIndentedString(siteId)).append("\n");
    sb.append("    url: ").append(toIndentedString(url)).append("\n");
    sb.append("    referer: ").append(toIndentedString(referer)).append("\n");
    sb.append("    userAgent: ").append(toIndentedString(userAgent)).append("\n");
    sb.append("    service: ").append(toIndentedString(service)).append("\n");
    sb.append("    server: ").append(toIndentedString(server)).append("\n");
    sb.append("    remoteIp: ").append(toIndentedString(remoteIp)).append("\n");
    sb.append("    pageId: ").append(toIndentedString(pageId)).append("\n");
    sb.append("    geoId: ").append(toIndentedString(geoId)).append("\n");
    sb.append("    payload: ").append(toIndentedString(payload)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(java.lang.Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}