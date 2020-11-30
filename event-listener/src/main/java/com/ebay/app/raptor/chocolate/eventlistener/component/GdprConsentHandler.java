package com.ebay.app.raptor.chocolate.eventlistener.component;

import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.CouchbaseKeyConstant;
import com.ebay.app.raptor.chocolate.constant.GdprConsentConstant;
import com.ebay.app.raptor.chocolate.eventlistener.util.CouchbaseClient;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import com.iabtcf.decoder.TCString;
import com.iabtcf.exceptions.TCStringDecodeException;
import com.iabtcf.utils.IntIterable;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    private CouchbaseClient couchbaseClient;

    @Autowired
    public void init() {
        couchbaseClient = CouchbaseClient.getInstance();
    }

    /**
     * some fields not be allowed put into kafka messages based on GDPR consent.
     *
     * @param targetUrl
     * @param channel
     * @return
     */
    public GdprConsentDomain handleGdprConsent(String targetUrl, ChannelIdEnum channel) {
        Metrics metrics = ESMetrics.getInstance();

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
                            if (purposesConsent != null && !purposesConsent.isEmpty()) {
                                logger.info("Purpose Consent is {}", purposesConsent.toString());

                                //1 is necessary
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
                }
            }
        } catch (TCStringDecodeException | IOException e) {
            metrics.meter(GdprConsentConstant.DECODE_CONSENT_ERROR);
            logger.warn("Occurred Exception when decode Consent, " + e);
        }
        return gdprConsentDomain;
    }
}
