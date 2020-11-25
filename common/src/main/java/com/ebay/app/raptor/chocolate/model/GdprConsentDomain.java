package com.ebay.app.raptor.chocolate.model;

public class GdprConsentDomain {
    private boolean tcfCompliantMode;

    private boolean allowedSetCookie;
    private boolean allowedShowPersonalizedAds;
    //the implication is contextual fields exclude geo fields
    private boolean allowedUseContextualInfo;
    private boolean allowedUseGeoInfo;
    //GeoCountryCode is legally required here.
    private boolean allowedUseLegallyRequiredField;
    private String consentFlagForDapParam;

    private boolean allowedStoredContextualData;
    private boolean allowedStoredPersonalizedData;

    public boolean isAllowedSetCookie() {
        return allowedSetCookie;
    }

    public void setAllowedSetCookie(boolean allowedSetCookie) {
        this.allowedSetCookie = allowedSetCookie;
    }

    public boolean isAllowedShowPersonalizedAds() {
        return allowedShowPersonalizedAds;
    }

    public void setAllowedShowPersonalizedAds(boolean allowedShowPersonalizedAds) {
        this.allowedShowPersonalizedAds = allowedShowPersonalizedAds;
    }

    public boolean isAllowedUseContextualInfo() {
        return allowedUseContextualInfo;
    }

    public void setAllowedUseContextualInfo(boolean allowedUseContextualInfo) {
        this.allowedUseContextualInfo = allowedUseContextualInfo;
    }

    public String getConsentFlagForDapParam() {
        return consentFlagForDapParam;
    }

    public void setConsentFlagForDapParam(String consentFlagForDapParam) {
        this.consentFlagForDapParam = consentFlagForDapParam;
    }

    public boolean isAllowedStoredContextualData() {
        return allowedStoredContextualData;
    }

    public void setAllowedStoredContextualData(boolean allowedStoredContextualData) {
        this.allowedStoredContextualData = allowedStoredContextualData;
    }

    public boolean isAllowedStoredPersonalizedData() {
        return allowedStoredPersonalizedData;
    }

    public void setAllowedStoredPersonalizedData(boolean allowedStoredPersonalizedData) {
        this.allowedStoredPersonalizedData = allowedStoredPersonalizedData;
    }

    public boolean isTcfCompliantMode() {
        return tcfCompliantMode;
    }

    public void setTcfCompliantMode(boolean tcfCompliantMode) {
        this.tcfCompliantMode = tcfCompliantMode;
    }

    public boolean isAllowedUseGeoInfo() {
        return allowedUseGeoInfo;
    }

    public void setAllowedUseGeoInfo(boolean allowedUseGeoInfo) {
        this.allowedUseGeoInfo = allowedUseGeoInfo;
    }

    public boolean isAllowedUseLegallyRequiredField() {
        return allowedUseLegallyRequiredField;
    }

    public void setAllowedUseLegallyRequiredField(boolean allowedUseLegallyRequiredField) {
        this.allowedUseLegallyRequiredField = allowedUseLegallyRequiredField;
    }

}
