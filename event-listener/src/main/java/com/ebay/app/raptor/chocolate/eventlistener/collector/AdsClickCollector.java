package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.component.AdsCollectionSvcClient;
import com.ebay.app.raptor.chocolate.eventlistener.model.AdsCollectionSvcRequest;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import javax.ws.rs.core.MultivaluedMap;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@Component
@DependsOn("EventListenerService")
public class AdsClickCollector {

    @Autowired
    protected AdsCollectionSvcClient adsCollectionSvcClient;
    private static final String EBAY_USER_AGENT = "ebayuseragent";
    private static final String ANDROID = "ebayandroid";
    private static final String IPHONE = "ebayiphone";
    private static final String AMDATA = "amdata";
    private static final String CLICK_TIME = "|tsp:";
    private static final String ONE = "1";
    private static final String ENC = "enc%3A";
    private static final String ENC_PD = "encpd%3A";
    private static final Logger LOGGER = LoggerFactory.getLogger(AdsClickCollector.class);

    public void processPromotedListingClick(IEndUserContext endUserContext, Event event,
                                            MultivaluedMap<String, String> requestHeaders) {
        try {
            MultiValueMap<String, String> queryParams = getQueryParams(event);
            ImmutablePair<String, Boolean> adsSignals = getAdsSignals(queryParams);
            if (isInvokeAdsSvc(endUserContext, adsSignals, queryParams)) {
                AdsCollectionSvcRequest adsCollectionSvcRequest = createAdsRequest(event, adsSignals.left);
                adsCollectionSvcClient.invokeService(adsCollectionSvcRequest, event.getReferrer(), requestHeaders);
            }
        } catch (Exception e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    protected AdsCollectionSvcRequest createAdsRequest(Event event, String amData) {
        AdsCollectionSvcRequest request = new AdsCollectionSvcRequest();
        if (StringUtils.isNotBlank(amData)) {
            StringBuilder amDataSB = new StringBuilder(amData);
            StringBuilder clickTime = new StringBuilder(CLICK_TIME);
            clickTime.append(System.currentTimeMillis());
            try {
                amDataSB.append(URLEncoder.encode(clickTime.toString(), StandardCharsets.UTF_8.toString()));
                request.setAmdata(amDataSB.toString());
            } catch (UnsupportedEncodingException e) {
                LOGGER.error("Error while encoding amdata", e);
            }
        }
        request.setReferrer(event.getReferrer());
        request.setRequestUrl(event.getTargetUrl());
        return request;
    }

    protected MultiValueMap<String, String> getQueryParams(Event event) {
        if (event != null && StringUtils.isNotEmpty(event.getTargetUrl())) {
            return UriComponentsBuilder.fromUriString(event.getTargetUrl()).build().getQueryParams();
        }
        return null;
    }

    protected ImmutablePair<String, Boolean> getAdsSignals(MultiValueMap<String, String> parameters) {
        String amdata = null;
        Boolean offebay = Boolean.FALSE;
        if (MapUtils.isEmpty(parameters)) {
            return new ImmutablePair<>(amdata, offebay);
        }
        if (!CollectionUtils.isEmpty(parameters)) {
            amdata = parameters.getFirst(AMDATA);
            if (ONE.equals(parameters.getFirst(Constants.MKEVT))) {
                offebay = Boolean.TRUE;
            }
        }
        return new ImmutablePair<>(amdata, offebay);
    }

    protected boolean isInvokeAdsSvc(IEndUserContext endUserContext, ImmutablePair<String, Boolean> adsSignals,
                                     MultiValueMap<String, String> queryParams) {
        if (isNative(endUserContext)
                && (amdataPresent(adsSignals.left) || adsSignals.right)) {
            return true;
        }
        return false;
    }

    protected boolean amdataPresent(String amdata) {
        return !StringUtils.isBlank(amdata);
    }

    protected boolean encKeyPresent(MultiValueMap<String, String> queryParams) {
        for (String key: queryParams.keySet()) {
            if (key.contains(ENC) || key.contains(ENC_PD)) {
                return true;
            }
        }
        return false;
    }

    private boolean isNative(IEndUserContext endUserContext) {
        if (endUserContext != null && StringUtils.isNotBlank(endUserContext.getUserAgent())) {
            String userAgentNormalized = endUserContext.getUserAgent().trim().toLowerCase();
            if (userAgentNormalized.startsWith(EBAY_USER_AGENT) || userAgentNormalized.contains(ANDROID)
                    || userAgentNormalized.contains(IPHONE)) {
                return true;
            }
        }
        return false;
    }
}
