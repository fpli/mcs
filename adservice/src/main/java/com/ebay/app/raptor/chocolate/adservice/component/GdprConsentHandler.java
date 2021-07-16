package com.ebay.app.raptor.chocolate.adservice.component;

import com.ebay.app.raptor.chocolate.adservice.util.CouchbaseClient;
import com.ebay.app.raptor.chocolate.constant.CouchbaseKeyConstant;
import com.ebay.app.raptor.chocolate.constant.GdprConsentConstant;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import com.ebay.kernel.util.Base64;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import com.iabtcf.decoder.TCString;
import com.iabtcf.exceptions.TCStringDecodeException;
import com.iabtcf.utils.IntIterable;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/**
 * For GDPR compliant mode
 *
 * @author yyang28
 */
@Component
@DependsOn("AdserviceService")
public class GdprConsentHandler {
    private static final String gdprParameter = "gdpr";
    private static final String gdprConsentParameter = "gdpr_consent";

    Logger logger = LoggerFactory.getLogger(GdprConsentHandler.class);

    private CouchbaseClient couchbaseClient;

    @Autowired
    public void init() {
        couchbaseClient = CouchbaseClient.getInstance();
    }

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
        String enableTcfComplianceModeString = couchbaseClient.get(CouchbaseKeyConstant.ENABLE_TCF_COMPLIANCE_MODE);
        logger.info("enableTcfComplianceMode {}", enableTcfComplianceModeString);
        boolean enableTcfComplianceMode = false;
        if (StringUtils.isNotBlank(enableTcfComplianceModeString)) {
            try {
                enableTcfComplianceMode = new ObjectMapper().readValue(enableTcfComplianceModeString, Boolean.class);
            } catch (IOException e) {
                logger.error("enableTcfComplianceMode format error, take a look please.");
            }
        }
        try {
            if (StringUtils.isNotBlank(gdprParam) && gdprParam.equals("1") && enableTcfComplianceMode) {
                metrics.meter(GdprConsentConstant.TOTAL_TRAFFIC_OF_GDPR_ADSVC);
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
                    IntIterable vendorConsent = tcString.getVendorConsent();
                    String purposeVendorIdString = couchbaseClient.get(CouchbaseKeyConstant.PURPOSE_VENDOR_ID);
                    logger.info("Purpose vendor id list is {} ", purposeVendorIdString);
                    if (StringUtils.isBlank(purposeVendorIdString)) {
                        logger.warn("Can't get purposeVendorID from CB, take a look please.");
                    }
                    if (StringUtils.isNotBlank(purposeVendorIdString) && vendorConsent != null) {
                        List<Integer> vendorIds = new ObjectMapper().readValue(purposeVendorIdString, List.class);
                        boolean containsAll = !vendorIds.stream().map(vendorConsent::contains).collect(Collectors.toSet()).contains(false);
                        //vendor consent have to contain all of purpose vendor ids
                        if (containsAll) {
                            //Purpose consent
                            IntIterable purposesConsent = tcString.getPurposesConsent();
                            //Special Purposes
                            IntIterable specialFeatureOptIns = tcString.getSpecialFeatureOptIns();
                            if (!purposesConsent.isEmpty()) {
                                logger.info("Purpose Consent is {}", purposesConsent.toString());
                                //construct consent flag for call DAP
                                JSONObject jsonObject = new JSONObject();
                                JSONObject gdpr = new JSONObject();
                                jsonObject.put("gdpr", gdpr);
                                purposesConsent.forEach(integer -> gdpr.put("p" + integer, 1));
                                specialFeatureOptIns.forEach(integer -> gdpr.put("sp" + integer, 1));
                                gdprConsentDomain.setConsentFlagForDapParam(Base64.encode(jsonObject.toString().getBytes(StandardCharsets.UTF_8)));

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
                                }
                                //allowed set the adguid cookie
                                if (purposesConsent.contains(3)) {
                                    gdprConsentDomain.setAllowedSetCookie(true);
                                    gdprConsentDomain.setAllowedUseLegallyRequiredField(true);
                                }
                                // show a non-personalized ad - pass adguid or guid downstream, we used allowed_show_personalized_ads to agent
                                if (purposesConsent.contains(4)) {
                                    gdprConsentDomain.setAllowedUseContextualInfo(true);
                                    gdprConsentDomain.setAllowedShowPersonalizedAds(true);
                                    gdprConsentDomain.setAllowedUseGeoInfo(true);
                                    gdprConsentDomain.setAllowedUseLegallyRequiredField(true);
                                }
                                // only basic fields and legally required.
                                if (purposesConsent.contains(7) || purposesConsent.contains(10) || specialFeatureOptIns.contains(2)) {
                                    gdprConsentDomain.setAllowedUseLegallyRequiredField(true);
                                }
                            }
                            //else treat every user as an anonymous user.
                        }
                        //else treat every user as an anonymous user.
                        if (gdprConsentDomain.isAllowedSetCookie()) {
                            metrics.meter(GdprConsentConstant.ALLOWED_SET_COOKIES);
                        }
                        if (gdprConsentDomain.isAllowedUseContextualInfo()) {
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_CONTEXTUAL);
                        }
                        if (gdprConsentDomain.isAllowedUseGeoInfo()) {
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_GEO);
                        }
                        if (gdprConsentDomain.isAllowedShowPersonalizedAds()) {
                            metrics.meter(GdprConsentConstant.ALLOWED_SHOW_PERSONALIZED_ADS);
                        }
                        if (gdprConsentDomain.isAllowedUseLegallyRequiredField()) {
                            metrics.meter(GdprConsentConstant.ALLOWED_USE_LELALLY_REQUIRED);
                        }
                    }
                }
            } else {
                gdprConsentDomain.setAllowedSetCookie(true);
                gdprConsentDomain.setAllowedShowPersonalizedAds(true);
                gdprConsentDomain.setAllowedUseContextualInfo(true);
                gdprConsentDomain.setAllowedUseGeoInfo(true);
                gdprConsentDomain.setTcfCompliantMode(false);
                gdprConsentDomain.setAllowedUseLegallyRequiredField(true);
            }
        } catch (Exception e) {
            metrics.meter(GdprConsentConstant.DECODE_CONSENT_ERROR);
            logger.warn("Occurred Exception when decode Consent, " + e);
        }
        return gdprConsentDomain;
    }
}
