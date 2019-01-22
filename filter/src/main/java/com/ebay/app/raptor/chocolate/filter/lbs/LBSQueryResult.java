package com.ebay.app.raptor.chocolate.filter.lbs;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class LBSQueryResult {

    private String regionCode;

    private String postalCode;

    private String metroCode;

    private String isoCountryCode2;

    private String stateCode;

    private double longitude;

    private String areaCodes;

    private double latitude;

    private String queryId;

    private String city;

    public String getRegionCode() {
        return regionCode;
    }

    public void setRegionCode(String regionCode) {
        this.regionCode = regionCode;
    }

    public String getPostalCode() {
        return postalCode;
    }

    public void setPostalCode(String postalCode) {
        this.postalCode = postalCode;
    }

    public String getMetroCode() {
        return metroCode;
    }

    public void setMetroCode(String metroCode) {
        this.metroCode = metroCode;
    }

    public String getIsoCountryCode2() {
        return isoCountryCode2;
    }

    public void setIsoCountryCode2(String isoCountryCode2) {
        this.isoCountryCode2 = isoCountryCode2;
    }

    public String getStateCode() {
        return stateCode;
    }

    public void setStateCode(String stateCode) {
        this.stateCode = stateCode;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getAreaCodes() {
        String areacode = areaCodes;
        if (areaCodes != null && areaCodes.indexOf("/") > -1) {
            areacode = areaCodes.substring(0, areaCodes.indexOf("/"));
        }
        return areacode;
    }

    public void setAreaCodes(String areaCodes) {
        this.areaCodes = areaCodes;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}