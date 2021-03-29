package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import com.ebay.traffic.chocolate.flink.nrt.transformer.utp.UTPImkTransformerMetrics;
import com.iabtcf.decoder.TCString;
import com.iabtcf.utils.IntIterable;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.Meter;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * For GDPR compliant mode
 *
 * @author yyang28
 */
public class GdprConsentHandler {
    private static final String gdprParameter = "gdpr";
    private static final String gdprConsentParameter = "gdpr_consent";
    private static final int EBAY_VENDOR_ID = 929;

    public static GdprConsentDomain handleGdprConsent(String targetUrl, Map<String, Meter> gdprMetrics) {
        GdprConsentDomain gdprConsentDomain = new GdprConsentDomain();
        gdprConsentDomain.setAllowedStoredContextualData(true);
        gdprConsentDomain.setAllowedStoredPersonalizedData(true);
        gdprConsentDomain.setTcfCompliantMode(false);
        Map<String, String> parametersMap = new HashMap<>();
        try {
            URIBuilder uriBuilder = new URIBuilder(targetUrl);
            List<NameValuePair> queryParams = uriBuilder.getQueryParams();
            parametersMap = queryParams.stream().filter(nameValuePair -> nameValuePair.getName().contains("gdpr"))
                    .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue));
        } catch (Exception e) {
            gdprMetrics.get(UTPImkTransformerMetrics.NUM_GDPR_URL_DECODE_ERROR_RATE).markEvent();
        }
        try {
            if (StringUtils.isNotBlank(parametersMap.get(gdprParameter)) && parametersMap.get(gdprParameter).equals("1")) {
                gdprMetrics.get(UTPImkTransformerMetrics.NUM_GDPR_RECORDS_IN_RATE).markEvent();

                gdprConsentDomain.setAllowedStoredContextualData(false);
                gdprConsentDomain.setAllowedStoredPersonalizedData(false);
                gdprConsentDomain.setTcfCompliantMode(true);

                String gdprConsent = parametersMap.get(gdprConsentParameter);
                if (StringUtils.isNotBlank(gdprConsent)) {
                    TCString tcString = TCString.decode(gdprConsent);
                    //Purpose consent
                    IntIterable purposesConsent = tcString.getPurposesConsent();
                    IntIterable vendorConsent = tcString.getVendorConsent();
                    if (vendorConsent != null) {
                        if (vendorConsent.contains(EBAY_VENDOR_ID)) {
                            if (purposesConsent != null && !purposesConsent.isEmpty()) {
                                //1 is necessary
                                if (!purposesConsent.contains(1)) {
                                    return gdprConsentDomain;
                                }

                                if (purposesConsent.contains(7)) {
                                    gdprConsentDomain.setAllowedStoredPersonalizedData(true);
                                    gdprConsentDomain.setAllowedStoredContextualData(true);
                                    gdprMetrics.get(UTPImkTransformerMetrics.NUM_GDPR_ALLOW_CONTEXTUAL_RATE).markEvent();
                                    gdprMetrics.get(UTPImkTransformerMetrics.NUM_GDPR_ALLOW_PERSONALIZED_RATE).markEvent();
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            gdprMetrics.get(UTPImkTransformerMetrics.NUM_GDPR_CONSENT_DECODE_ERROR_RATE).markEvent();
        }
        return gdprConsentDomain;
    }
}
