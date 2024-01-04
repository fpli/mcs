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
import com.fasterxml.jackson.annotation.*;

/**
 * AkamaiEvent
 */


@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2024-01-02T17:55:36.565+08:00[Asia/Shanghai]")

@JsonIgnoreProperties(ignoreUnknown = true)


public class AkamaiEvent implements Serializable {

    private static final long serialVersionUID = 1L;



    @JsonProperty("version")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String version = null;
    @JsonProperty("ewUsageInfo")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String ewUsageInfo = null;
    @JsonProperty("ewExecutionInfo")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String ewExecutionInfo = null;
    @JsonProperty("country")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String country = null;
    @JsonProperty("city")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String city = null;
    @JsonProperty("state")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String state = null;
    @JsonProperty("cacheStatus")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String cacheStatus = null;
    @JsonProperty("customField")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String customField = null;
    @JsonProperty("turnAroundTimeMSec")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String turnAroundTimeMSec = null;
    @JsonProperty("transferTimeMSec")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String transferTimeMSec = null;
    @JsonProperty("cliIP")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String cliIP = null;
    @JsonProperty("statusCode")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String statusCode = null;
    @JsonProperty("reqHost")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String reqHost = null;
    @JsonProperty("reqMethod")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String reqMethod = null;
    @JsonProperty("bytes")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String bytes = null;
    @JsonProperty("tlsVersion")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String tlsVersion = null;
    @JsonProperty("UA")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String UA = null;
    @JsonProperty("queryStr")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String queryStr = null;
    @JsonProperty("rspContentLen")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String rspContentLen = null;
    @JsonProperty("rspContentType")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String rspContentType = null;
    @JsonProperty("reqPath")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String reqPath = null;
    @JsonProperty("reqPort")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String reqPort = null;
    @JsonProperty("proto")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String proto = null;
    @JsonProperty("reqTimeSec")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String reqTimeSec = null;
    @JsonProperty("cp")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String cp = null;
    @JsonProperty("reqId")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String reqId = null;
    @JsonProperty("tlsOverheadTimeMSec")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String tlsOverheadTimeMSec = null;
    @JsonProperty("objSize")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String objSize = null;
    @JsonProperty("uncompressedSize")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String uncompressedSize = null;
    @JsonProperty("overheadBytes")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String overheadBytes = null;
    @JsonProperty("totalBytes")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String totalBytes = null;
    @JsonProperty("accLang")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String accLang = null;
    @JsonProperty("cookie")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String cookie = null;
    @JsonProperty("range")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String range = null;
    @JsonProperty("referer")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String referer = null;
    @JsonProperty("xForwardedFor")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String xForwardedFor = null;
    @JsonProperty("maxAgeSec")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String maxAgeSec = null;
    @JsonProperty("reqEndTimeMSec")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String reqEndTimeMSec = null;
    @JsonProperty("errorCode")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String errorCode = null;
    @JsonProperty("dnsLookupTimeMSec")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String dnsLookupTimeMSec = null;
    @JsonProperty("billingRegion")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String billingRegion = null;
    @JsonProperty("edgeIP")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String edgeIP = null;
    @JsonProperty("securityRules")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String securityRules = null;
    @JsonProperty("serverCountry")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String serverCountry = null;
    @JsonProperty("streamId")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String streamId = null;
    @JsonProperty("asn")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String asn = null;

    /**
     * Get version
     * @return version
     **/
    @ApiModelProperty(value = "")
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
    /**
     * Get ewUsageInfo
     * @return ewUsageInfo
     **/
    @ApiModelProperty(value = "")
    public String getEwUsageInfo() {
        return ewUsageInfo;
    }

    public void setEwUsageInfo(String ewUsageInfo) {
        this.ewUsageInfo = ewUsageInfo;
    }
    /**
     * Get ewExecutionInfo
     * @return ewExecutionInfo
     **/
    @ApiModelProperty(value = "")
    public String getEwExecutionInfo() {
        return ewExecutionInfo;
    }

    public void setEwExecutionInfo(String ewExecutionInfo) {
        this.ewExecutionInfo = ewExecutionInfo;
    }
    /**
     * Get country
     * @return country
     **/
    @ApiModelProperty(value = "")
    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }
    /**
     * Get city
     * @return city
     **/
    @ApiModelProperty(value = "")
    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
    /**
     * Get state
     * @return state
     **/
    @ApiModelProperty(value = "")
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
    /**
     * Get cacheStatus
     * @return cacheStatus
     **/
    @ApiModelProperty(value = "")
    public String getCacheStatus() {
        return cacheStatus;
    }

    public void setCacheStatus(String cacheStatus) {
        this.cacheStatus = cacheStatus;
    }
    /**
     * Get customField
     * @return customField
     **/
    @ApiModelProperty(value = "")
    public String getCustomField() {
        return customField;
    }

    public void setCustomField(String customField) {
        this.customField = customField;
    }
    /**
     * Get turnAroundTimeMSec
     * @return turnAroundTimeMSec
     **/
    @ApiModelProperty(value = "")
    public String getTurnAroundTimeMSec() {
        return turnAroundTimeMSec;
    }

    public void setTurnAroundTimeMSec(String turnAroundTimeMSec) {
        this.turnAroundTimeMSec = turnAroundTimeMSec;
    }
    /**
     * Get transferTimeMSec
     * @return transferTimeMSec
     **/
    @ApiModelProperty(value = "")
    public String getTransferTimeMSec() {
        return transferTimeMSec;
    }

    public void setTransferTimeMSec(String transferTimeMSec) {
        this.transferTimeMSec = transferTimeMSec;
    }
    /**
     * Get cliIP
     * @return cliIP
     **/
    @ApiModelProperty(value = "")
    public String getCliIP() {
        return cliIP;
    }

    public void setCliIP(String cliIP) {
        this.cliIP = cliIP;
    }
    /**
     * Get statusCode
     * @return statusCode
     **/
    @ApiModelProperty(value = "")
    public String getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }
    /**
     * Get reqHost
     * @return reqHost
     **/
    @ApiModelProperty(value = "")
    public String getReqHost() {
        return reqHost;
    }

    public void setReqHost(String reqHost) {
        this.reqHost = reqHost;
    }
    /**
     * Get reqMethod
     * @return reqMethod
     **/
    @ApiModelProperty(value = "")
    public String getReqMethod() {
        return reqMethod;
    }

    public void setReqMethod(String reqMethod) {
        this.reqMethod = reqMethod;
    }
    /**
     * Get bytes
     * @return bytes
     **/
    @ApiModelProperty(value = "")
    public String getBytes() {
        return bytes;
    }

    public void setBytes(String bytes) {
        this.bytes = bytes;
    }
    /**
     * Get tlsVersion
     * @return tlsVersion
     **/
    @ApiModelProperty(value = "")
    public String getTlsVersion() {
        return tlsVersion;
    }

    public void setTlsVersion(String tlsVersion) {
        this.tlsVersion = tlsVersion;
    }
    /**
     * Get UA
     * @return UA
     **/
    @ApiModelProperty(value = "")
    public String getUA() {
        return UA;
    }

    public void setUA(String UA) {
        this.UA = UA;
    }
    /**
     * Get queryStr
     * @return queryStr
     **/
    @ApiModelProperty(value = "")
    public String getQueryStr() {
        return queryStr;
    }

    public void setQueryStr(String queryStr) {
        this.queryStr = queryStr;
    }
    /**
     * Get rspContentLen
     * @return rspContentLen
     **/
    @ApiModelProperty(value = "")
    public String getRspContentLen() {
        return rspContentLen;
    }

    public void setRspContentLen(String rspContentLen) {
        this.rspContentLen = rspContentLen;
    }
    /**
     * Get rspContentType
     * @return rspContentType
     **/
    @ApiModelProperty(value = "")
    public String getRspContentType() {
        return rspContentType;
    }

    public void setRspContentType(String rspContentType) {
        this.rspContentType = rspContentType;
    }
    /**
     * Get reqPath
     * @return reqPath
     **/
    @ApiModelProperty(value = "")
    public String getReqPath() {
        return reqPath;
    }

    public void setReqPath(String reqPath) {
        this.reqPath = reqPath;
    }
    /**
     * Get reqPort
     * @return reqPort
     **/
    @ApiModelProperty(value = "")
    public String getReqPort() {
        return reqPort;
    }

    public void setReqPort(String reqPort) {
        this.reqPort = reqPort;
    }
    /**
     * Get proto
     * @return proto
     **/
    @ApiModelProperty(value = "")
    public String getProto() {
        return proto;
    }

    public void setProto(String proto) {
        this.proto = proto;
    }
    /**
     * Get reqTimeSec
     * @return reqTimeSec
     **/
    @ApiModelProperty(value = "")
    public String getReqTimeSec() {
        return reqTimeSec;
    }

    public void setReqTimeSec(String reqTimeSec) {
        this.reqTimeSec = reqTimeSec;
    }
    /**
     * Get cp
     * @return cp
     **/
    @ApiModelProperty(value = "")
    public String getCp() {
        return cp;
    }

    public void setCp(String cp) {
        this.cp = cp;
    }
    /**
     * Get reqId
     * @return reqId
     **/
    @ApiModelProperty(value = "")
    public String getReqId() {
        return reqId;
    }

    public void setReqId(String reqId) {
        this.reqId = reqId;
    }
    /**
     * Get tlsOverheadTimeMSec
     * @return tlsOverheadTimeMSec
     **/
    @ApiModelProperty(value = "")
    public String getTlsOverheadTimeMSec() {
        return tlsOverheadTimeMSec;
    }

    public void setTlsOverheadTimeMSec(String tlsOverheadTimeMSec) {
        this.tlsOverheadTimeMSec = tlsOverheadTimeMSec;
    }
    /**
     * Get objSize
     * @return objSize
     **/
    @ApiModelProperty(value = "")
    public String getObjSize() {
        return objSize;
    }

    public void setObjSize(String objSize) {
        this.objSize = objSize;
    }
    /**
     * Get uncompressedSize
     * @return uncompressedSize
     **/
    @ApiModelProperty(value = "")
    public String getUncompressedSize() {
        return uncompressedSize;
    }

    public void setUncompressedSize(String uncompressedSize) {
        this.uncompressedSize = uncompressedSize;
    }
    /**
     * Get overheadBytes
     * @return overheadBytes
     **/
    @ApiModelProperty(value = "")
    public String getOverheadBytes() {
        return overheadBytes;
    }

    public void setOverheadBytes(String overheadBytes) {
        this.overheadBytes = overheadBytes;
    }
    /**
     * Get totalBytes
     * @return totalBytes
     **/
    @ApiModelProperty(value = "")
    public String getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(String totalBytes) {
        this.totalBytes = totalBytes;
    }
    /**
     * Get accLang
     * @return accLang
     **/
    @ApiModelProperty(value = "")
    public String getAccLang() {
        return accLang;
    }

    public void setAccLang(String accLang) {
        this.accLang = accLang;
    }
    /**
     * Get cookie
     * @return cookie
     **/
    @ApiModelProperty(value = "")
    public String getCookie() {
        return cookie;
    }

    public void setCookie(String cookie) {
        this.cookie = cookie;
    }
    /**
     * Get range
     * @return range
     **/
    @ApiModelProperty(value = "")
    public String getRange() {
        return range;
    }

    public void setRange(String range) {
        this.range = range;
    }
    /**
     * Get referer
     * @return referer
     **/
    @ApiModelProperty(value = "")
    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }
    /**
     * Get xForwardedFor
     * @return xForwardedFor
     **/
    @ApiModelProperty(value = "")
    public String getXForwardedFor() {
        return xForwardedFor;
    }

    public void setXForwardedFor(String xForwardedFor) {
        this.xForwardedFor = xForwardedFor;
    }
    /**
     * Get maxAgeSec
     * @return maxAgeSec
     **/
    @ApiModelProperty(value = "")
    public String getMaxAgeSec() {
        return maxAgeSec;
    }

    public void setMaxAgeSec(String maxAgeSec) {
        this.maxAgeSec = maxAgeSec;
    }
    /**
     * Get reqEndTimeMSec
     * @return reqEndTimeMSec
     **/
    @ApiModelProperty(value = "")
    public String getReqEndTimeMSec() {
        return reqEndTimeMSec;
    }

    public void setReqEndTimeMSec(String reqEndTimeMSec) {
        this.reqEndTimeMSec = reqEndTimeMSec;
    }
    /**
     * Get errorCode
     * @return errorCode
     **/
    @ApiModelProperty(value = "")
    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }
    /**
     * Get dnsLookupTimeMSec
     * @return dnsLookupTimeMSec
     **/
    @ApiModelProperty(value = "")
    public String getDnsLookupTimeMSec() {
        return dnsLookupTimeMSec;
    }

    public void setDnsLookupTimeMSec(String dnsLookupTimeMSec) {
        this.dnsLookupTimeMSec = dnsLookupTimeMSec;
    }
    /**
     * Get billingRegion
     * @return billingRegion
     **/
    @ApiModelProperty(value = "")
    public String getBillingRegion() {
        return billingRegion;
    }

    public void setBillingRegion(String billingRegion) {
        this.billingRegion = billingRegion;
    }
    /**
     * Get edgeIP
     * @return edgeIP
     **/
    @ApiModelProperty(value = "")
    public String getEdgeIP() {
        return edgeIP;
    }

    public void setEdgeIP(String edgeIP) {
        this.edgeIP = edgeIP;
    }
    /**
     * Get securityRules
     * @return securityRules
     **/
    @ApiModelProperty(value = "")
    public String getSecurityRules() {
        return securityRules;
    }

    public void setSecurityRules(String securityRules) {
        this.securityRules = securityRules;
    }
    /**
     * Get serverCountry
     * @return serverCountry
     **/
    @ApiModelProperty(value = "")
    public String getServerCountry() {
        return serverCountry;
    }

    public void setServerCountry(String serverCountry) {
        this.serverCountry = serverCountry;
    }
    /**
     * Get streamId
     * @return streamId
     **/
    @ApiModelProperty(value = "")
    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }
    /**
     * Get asn
     * @return asn
     **/
    @ApiModelProperty(value = "")
    public String getAsn() {
        return asn;
    }

    public void setAsn(String asn) {
        this.asn = asn;
    }
    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AkamaiEvent akamaiEvent = (AkamaiEvent) o;
        return Objects.equals(this.version, akamaiEvent.version) &&
                Objects.equals(this.ewUsageInfo, akamaiEvent.ewUsageInfo) &&
                Objects.equals(this.ewExecutionInfo, akamaiEvent.ewExecutionInfo) &&
                Objects.equals(this.country, akamaiEvent.country) &&
                Objects.equals(this.city, akamaiEvent.city) &&
                Objects.equals(this.state, akamaiEvent.state) &&
                Objects.equals(this.cacheStatus, akamaiEvent.cacheStatus) &&
                Objects.equals(this.customField, akamaiEvent.customField) &&
                Objects.equals(this.turnAroundTimeMSec, akamaiEvent.turnAroundTimeMSec) &&
                Objects.equals(this.transferTimeMSec, akamaiEvent.transferTimeMSec) &&
                Objects.equals(this.cliIP, akamaiEvent.cliIP) &&
                Objects.equals(this.statusCode, akamaiEvent.statusCode) &&
                Objects.equals(this.reqHost, akamaiEvent.reqHost) &&
                Objects.equals(this.reqMethod, akamaiEvent.reqMethod) &&
                Objects.equals(this.bytes, akamaiEvent.bytes) &&
                Objects.equals(this.tlsVersion, akamaiEvent.tlsVersion) &&
                Objects.equals(this.UA, akamaiEvent.UA) &&
                Objects.equals(this.queryStr, akamaiEvent.queryStr) &&
                Objects.equals(this.rspContentLen, akamaiEvent.rspContentLen) &&
                Objects.equals(this.rspContentType, akamaiEvent.rspContentType) &&
                Objects.equals(this.reqPath, akamaiEvent.reqPath) &&
                Objects.equals(this.reqPort, akamaiEvent.reqPort) &&
                Objects.equals(this.proto, akamaiEvent.proto) &&
                Objects.equals(this.reqTimeSec, akamaiEvent.reqTimeSec) &&
                Objects.equals(this.cp, akamaiEvent.cp) &&
                Objects.equals(this.reqId, akamaiEvent.reqId) &&
                Objects.equals(this.tlsOverheadTimeMSec, akamaiEvent.tlsOverheadTimeMSec) &&
                Objects.equals(this.objSize, akamaiEvent.objSize) &&
                Objects.equals(this.uncompressedSize, akamaiEvent.uncompressedSize) &&
                Objects.equals(this.overheadBytes, akamaiEvent.overheadBytes) &&
                Objects.equals(this.totalBytes, akamaiEvent.totalBytes) &&
                Objects.equals(this.accLang, akamaiEvent.accLang) &&
                Objects.equals(this.cookie, akamaiEvent.cookie) &&
                Objects.equals(this.range, akamaiEvent.range) &&
                Objects.equals(this.referer, akamaiEvent.referer) &&
                Objects.equals(this.xForwardedFor, akamaiEvent.xForwardedFor) &&
                Objects.equals(this.maxAgeSec, akamaiEvent.maxAgeSec) &&
                Objects.equals(this.reqEndTimeMSec, akamaiEvent.reqEndTimeMSec) &&
                Objects.equals(this.errorCode, akamaiEvent.errorCode) &&
                Objects.equals(this.dnsLookupTimeMSec, akamaiEvent.dnsLookupTimeMSec) &&
                Objects.equals(this.billingRegion, akamaiEvent.billingRegion) &&
                Objects.equals(this.edgeIP, akamaiEvent.edgeIP) &&
                Objects.equals(this.securityRules, akamaiEvent.securityRules) &&
                Objects.equals(this.serverCountry, akamaiEvent.serverCountry) &&
                Objects.equals(this.streamId, akamaiEvent.streamId) &&
                Objects.equals(this.asn, akamaiEvent.asn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, ewUsageInfo, ewExecutionInfo, country, city, state, cacheStatus, customField, turnAroundTimeMSec, transferTimeMSec, cliIP, statusCode, reqHost, reqMethod, bytes, tlsVersion, UA, queryStr, rspContentLen, rspContentType, reqPath, reqPort, proto, reqTimeSec, cp, reqId, tlsOverheadTimeMSec, objSize, uncompressedSize, overheadBytes, totalBytes, accLang, cookie, range, referer, xForwardedFor, maxAgeSec, reqEndTimeMSec, errorCode, dnsLookupTimeMSec, billingRegion, edgeIP, securityRules, serverCountry, streamId, asn);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("class AkamaiEvent {\n");

        sb.append("    version: ").append(toIndentedString(version)).append("\n");
        sb.append("    ewUsageInfo: ").append(toIndentedString(ewUsageInfo)).append("\n");
        sb.append("    ewExecutionInfo: ").append(toIndentedString(ewExecutionInfo)).append("\n");
        sb.append("    country: ").append(toIndentedString(country)).append("\n");
        sb.append("    city: ").append(toIndentedString(city)).append("\n");
        sb.append("    state: ").append(toIndentedString(state)).append("\n");
        sb.append("    cacheStatus: ").append(toIndentedString(cacheStatus)).append("\n");
        sb.append("    customField: ").append(toIndentedString(customField)).append("\n");
        sb.append("    turnAroundTimeMSec: ").append(toIndentedString(turnAroundTimeMSec)).append("\n");
        sb.append("    transferTimeMSec: ").append(toIndentedString(transferTimeMSec)).append("\n");
        sb.append("    cliIP: ").append(toIndentedString(cliIP)).append("\n");
        sb.append("    statusCode: ").append(toIndentedString(statusCode)).append("\n");
        sb.append("    reqHost: ").append(toIndentedString(reqHost)).append("\n");
        sb.append("    reqMethod: ").append(toIndentedString(reqMethod)).append("\n");
        sb.append("    bytes: ").append(toIndentedString(bytes)).append("\n");
        sb.append("    tlsVersion: ").append(toIndentedString(tlsVersion)).append("\n");
        sb.append("    UA: ").append(toIndentedString(UA)).append("\n");
        sb.append("    queryStr: ").append(toIndentedString(queryStr)).append("\n");
        sb.append("    rspContentLen: ").append(toIndentedString(rspContentLen)).append("\n");
        sb.append("    rspContentType: ").append(toIndentedString(rspContentType)).append("\n");
        sb.append("    reqPath: ").append(toIndentedString(reqPath)).append("\n");
        sb.append("    reqPort: ").append(toIndentedString(reqPort)).append("\n");
        sb.append("    proto: ").append(toIndentedString(proto)).append("\n");
        sb.append("    reqTimeSec: ").append(toIndentedString(reqTimeSec)).append("\n");
        sb.append("    cp: ").append(toIndentedString(cp)).append("\n");
        sb.append("    reqId: ").append(toIndentedString(reqId)).append("\n");
        sb.append("    tlsOverheadTimeMSec: ").append(toIndentedString(tlsOverheadTimeMSec)).append("\n");
        sb.append("    objSize: ").append(toIndentedString(objSize)).append("\n");
        sb.append("    uncompressedSize: ").append(toIndentedString(uncompressedSize)).append("\n");
        sb.append("    overheadBytes: ").append(toIndentedString(overheadBytes)).append("\n");
        sb.append("    totalBytes: ").append(toIndentedString(totalBytes)).append("\n");
        sb.append("    accLang: ").append(toIndentedString(accLang)).append("\n");
        sb.append("    cookie: ").append(toIndentedString(cookie)).append("\n");
        sb.append("    range: ").append(toIndentedString(range)).append("\n");
        sb.append("    referer: ").append(toIndentedString(referer)).append("\n");
        sb.append("    xForwardedFor: ").append(toIndentedString(xForwardedFor)).append("\n");
        sb.append("    maxAgeSec: ").append(toIndentedString(maxAgeSec)).append("\n");
        sb.append("    reqEndTimeMSec: ").append(toIndentedString(reqEndTimeMSec)).append("\n");
        sb.append("    errorCode: ").append(toIndentedString(errorCode)).append("\n");
        sb.append("    dnsLookupTimeMSec: ").append(toIndentedString(dnsLookupTimeMSec)).append("\n");
        sb.append("    billingRegion: ").append(toIndentedString(billingRegion)).append("\n");
        sb.append("    edgeIP: ").append(toIndentedString(edgeIP)).append("\n");
        sb.append("    securityRules: ").append(toIndentedString(securityRules)).append("\n");
        sb.append("    serverCountry: ").append(toIndentedString(serverCountry)).append("\n");
        sb.append("    streamId: ").append(toIndentedString(streamId)).append("\n");
        sb.append("    asn: ").append(toIndentedString(asn)).append("\n");
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
