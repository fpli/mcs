package com.ebay.app.raptor.chocolate.component;

import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.GdprConsentConstant;
import com.ebay.kernel.util.Base64;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import com.iabtcf.decoder.TCString;
import com.iabtcf.exceptions.TCStringDecodeException;
import com.iabtcf.utils.IntIterable;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URIUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import sun.net.util.URLUtil;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class GdprConsentHandler {
    private static final String gdprParameter = "gdpr";
    private static final String gdprConsentParameter = "gdpr_consent";

    Logger logger = LoggerFactory.getLogger(GdprConsentHandler.class);
    Metrics metrics = ESMetrics.getInstance();

    /**
     * for adservice ar
     *
     * @param request original request
     * @return
     */
    public GdprConsentDomain handleGdprConsentForAr(HttpServletRequest request) {
        GdprConsentDomain gdprConsentDomain = new GdprConsentDomain();
        String gdprParam = request.getParameter(gdprParameter);
        try {
            if (StringUtils.isNotBlank(gdprParam) && gdprParam.equals("1")) {
                metrics.meter(GdprConsentConstant.TOTAL_TRAFFIC_OF_GDPR);
                gdprConsentDomain.setAllowedSetCookie(false);
                gdprConsentDomain.setAllowedShowPersonalizedAds(false);
                gdprConsentDomain.setAllowedUseContextualInfo(false);
                gdprConsentDomain.setAllowedUseGeoInfo(false);
                gdprConsentDomain.setConsentFlagForDapParam(null);
                gdprConsentDomain.setTcfCompliantMode(true);
                String consentParam = request.getParameter(gdprConsentParameter);
                if (StringUtils.isNotBlank(consentParam)) {
                    TCString tcString = TCString.decode(consentParam);
                    //Purpose consent
                    IntIterable purposesConsent = tcString.getPurposesConsent();
                    //Special Purposes
                    IntIterable specialFeatureOptIns = tcString.getSpecialFeatureOptIns();
                    if (!purposesConsent.isEmpty()) {
                        //construct consent flag for call DAP
                        JSONObject jsonObject = new JSONObject();
                        JSONObject gdpr = new JSONObject();
                        jsonObject.put("gdpr", gdpr);
                        purposesConsent.forEach(integer -> gdpr.put("p" + integer, 1));
                        specialFeatureOptIns.forEach(integer -> gdpr.put("sp" + integer, 1));
                        gdprConsentDomain.setConsentFlagForDapParam(Base64.encode(jsonObject.toString().getBytes()));

                        //to get what we can do base the rules
                        if (!purposesConsent.contains(1)) {
                            return gdprConsentDomain;
                        }

                        if (purposesConsent.contains(2)) {
                            gdprConsentDomain.setAllowedUseGeoInfo(true);
                            gdprConsentDomain.setAllowedUseContextualInfo(true);
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_GEO);
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_CONTEXTUAL);
                        }

                        if (purposesConsent.contains(3)) {
                            gdprConsentDomain.setAllowedUseContextualInfo(true);
                            gdprConsentDomain.setAllowedSetCookie(true);
                            metrics.meter(GdprConsentConstant.ALLOWED_SET_COOKIES);
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_CONTEXTUAL);
                        }

                        if (purposesConsent.contains(4)) {
                            gdprConsentDomain.setAllowedUseContextualInfo(true);
                            gdprConsentDomain.setAllowedShowPersonalizedAds(true);
                            gdprConsentDomain.setAllowedUseGeoInfo(true);
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_GEO);
                            metrics.meter(GdprConsentConstant.ALLOWED_SHOW_PERSONALIZED_ADS);
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_CONTEXTUAL);
                        }

                        if (purposesConsent.contains(7)) {
                            gdprConsentDomain.setAllowedUseContextualInfo(true);
                            gdprConsentDomain.setAllowedShowPersonalizedAds(true);
                            metrics.meter(GdprConsentConstant.ALLOWED_SHOW_PERSONALIZED_ADS);
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_CONTEXTUAL);
                        }
                    }
                    //else treat every user as an anonymous user.
                }
                //else treat every user as an anonymous user.
            } else {
                gdprConsentDomain.setAllowedSetCookie(true);
                gdprConsentDomain.setAllowedShowPersonalizedAds(true);
                gdprConsentDomain.setAllowedUseContextualInfo(true);
                gdprConsentDomain.setAllowedUseGeoInfo(true);
                gdprConsentDomain.setTcfCompliantMode(false);
            }
        } catch (TCStringDecodeException e) {
            metrics.meter(GdprConsentConstant.DECODE_CONSENT_ERROR);
            logger.warn("Occurred Exception when decode Consent, " + e);
        }
        return gdprConsentDomain;
    }

    public GdprConsentDomain handleGdprConsentForMsc(String targetUrl, ChannelIdEnum channel) {
        GdprConsentDomain gdprConsentDomain = new GdprConsentDomain();
        gdprConsentDomain.setAllowedStoredContextualData(true);
        gdprConsentDomain.setAllowedStoredPersonalizedData(true);
        gdprConsentDomain.setTcfCompliantMode(false);
        if (channel != ChannelIdEnum.DAP) {
            return gdprConsentDomain;
        }
        Map<String, String> parametersMap = new HashMap<>();
        try {
            URIBuilder uriBuilder = new URIBuilder(targetUrl);
            List<NameValuePair> queryParams = uriBuilder.getQueryParams();
            parametersMap = queryParams.stream().filter(nameValuePair -> nameValuePair.getName().contains("gdpr"))
                    .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue));
        } catch (URISyntaxException e) {
            logger.warn("target url format incorrect, " + e);
        }
        try {
            if (StringUtils.isNotBlank(parametersMap.get(gdprParameter)) && parametersMap.get(gdprParameter).equals("1")) {
                metrics.meter(GdprConsentConstant.TOTAL_TRAFFIC_OF_GDPR);
                gdprConsentDomain.setAllowedStoredContextualData(false);
                gdprConsentDomain.setAllowedStoredPersonalizedData(false);
                gdprConsentDomain.setTcfCompliantMode(true);

                String gdprConsent = parametersMap.get(gdprConsentParameter);
                if (StringUtils.isNotBlank(gdprConsent)) {

                    TCString tcString = TCString.decode(gdprConsent);
                    //Purpose consent
                    IntIterable purposesConsent = tcString.getPurposesConsent();

                    if (purposesConsent != null && !purposesConsent.isEmpty()) {
                        if (!purposesConsent.contains(1)) {
                            return gdprConsentDomain;
                        }
                        if (purposesConsent.contains(7)) {
                            gdprConsentDomain.setAllowedStoredPersonalizedData(true);
                            gdprConsentDomain.setAllowedStoredContextualData(true);
                            metrics.meter(GdprConsentConstant.ALLOWED_STORED_CONTEXTUAL);
                            metrics.meter(GdprConsentConstant.ALLOWED_STORED_PERSONALIZED);
                        }
                    }
                }
            }
        } catch (TCStringDecodeException e) {
            metrics.meter(GdprConsentConstant.DECODE_CONSENT_ERROR);
            logger.warn("Occurred Exception when decode Consent, " + e);
        }
        return gdprConsentDomain;
    }

    public class GdprConsentDomain {
        private boolean tcfCompliantMode;

        private boolean allowedSetCookie;
        private boolean allowedShowPersonalizedAds;
        //the implication is contextual fields exclude geo fields
        private boolean allowedUseContextualInfo;
        private boolean allowedUseGeoInfo;
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
    }
}
