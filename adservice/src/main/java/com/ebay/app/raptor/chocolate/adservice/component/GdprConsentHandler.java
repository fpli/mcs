package com.ebay.app.raptor.chocolate.adservice.component;

import com.ebay.app.raptor.chocolate.constant.GdprConsentConstant;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import com.ebay.kernel.util.Base64;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import com.iabtcf.decoder.TCString;
import com.iabtcf.exceptions.TCStringDecodeException;
import com.iabtcf.utils.IntIterable;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;

/**
 * For GDPR compliant mode
 *
 * @author yyang28
 */
@Component
public class GdprConsentHandler {
    private static final String gdprParameter = "gdpr";
    private static final String gdprConsentParameter = "gdpr_consent";

    Logger logger = LoggerFactory.getLogger(GdprConsentHandler.class);

    /**
     * for adservice ar
     *
     * @param request original request
     * @return GdprConsentDomain
     */
    public GdprConsentDomain handleGdprConsent(HttpServletRequest request) {
        Metrics metrics = ESMetrics.getInstance();
        GdprConsentDomain gdprConsentDomain = new GdprConsentDomain();
        String gdprParam = request.getParameter(gdprParameter);
        try {
            if (StringUtils.isNotBlank(gdprParam) && gdprParam.equals("1")) {
                metrics.meter(GdprConsentConstant.TOTAL_TRAFFIC_OF_GDPR);
                gdprConsentDomain.setAllowedSetCookie(false);
                gdprConsentDomain.setAllowedShowPersonalizedAds(false);
                gdprConsentDomain.setAllowedUseContextualInfo(false);
                gdprConsentDomain.setAllowedUseGeoInfo(false);
                gdprConsentDomain.setAllowedUseLegallyRequiredField(false);
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
                        //1 is basic of all
                        if (!purposesConsent.contains(1)) {
                            return gdprConsentDomain;
                        }
                        //If Purpose 2 is present, show a non-personalized ad - treat the user as anonymous.
                        if (purposesConsent.contains(2)) {
                            gdprConsentDomain.setAllowedUseGeoInfo(true);
                            gdprConsentDomain.setAllowedUseContextualInfo(true);
                            gdprConsentDomain.setAllowedUseLegallyRequiredField(true);
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_GEO);
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_CONTEXTUAL);
                        }
                        //allowed set the adguid cookie
                        if (purposesConsent.contains(3)) {
                            gdprConsentDomain.setAllowedUseContextualInfo(true);
                            gdprConsentDomain.setAllowedSetCookie(true);
                            gdprConsentDomain.setAllowedUseLegallyRequiredField(true);
                            metrics.meter(GdprConsentConstant.ALLOWED_SET_COOKIES);
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_CONTEXTUAL);
                        }
                        // show a non-personalized ad - pass adguid or guid downstream, we used allowed_show_personalized_ads to agent
                        if (purposesConsent.contains(4)) {
                            gdprConsentDomain.setAllowedUseContextualInfo(true);
                            gdprConsentDomain.setAllowedShowPersonalizedAds(true);
                            gdprConsentDomain.setAllowedUseGeoInfo(true);
                            gdprConsentDomain.setAllowedUseLegallyRequiredField(true);
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_GEO);
                            metrics.meter(GdprConsentConstant.ALLOWED_SHOW_PERSONALIZED_ADS);
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_CONTEXTUAL);
                        }
                        // show a personalized ad and allowed pass contextual fields but Geo
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
                gdprConsentDomain.setAllowedUseLegallyRequiredField(true);
            }
        } catch (TCStringDecodeException e) {
            metrics.meter(GdprConsentConstant.DECODE_CONSENT_ERROR);
            logger.warn("Occurred Exception when decode Consent, " + e);
        }
        return gdprConsentDomain;
    }
}
