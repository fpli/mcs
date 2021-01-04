package com.ebay.app.raptor.chocolate.eventlistener.component;

import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.CouchbaseKeyConstant;
import com.ebay.app.raptor.chocolate.eventlistener.util.CouchbaseClient;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class GdprConsentHandlerTest {

    @Autowired
    GdprConsentHandler gdprConsentHandler;

    @Test
    public void testCanStorePersonalizedAndContextual() {
        CouchbaseClient instance = CouchbaseClient.getInstance();
        instance.put(CouchbaseKeyConstant.PURPOSE_VENDOR_ID, "[22]", 0);
        String targetUrl = "http://www.ebayadservices.com/marketingtracking/v1/ar?mpt=682877911&ff18=mWeb&siteid=0&icep_siteid=0&ipn=admain2&adtype=3&size=300x250&pgroup=532260&gdpr=1&gdpr_consent=CO4REdoO4REdoAOACBENBACoAP%5FAAH%5FAACiQGVtd%5FX9fb2tj%2D%5F599%5Ft0eY1f9%5F63t2wzjgeNs%2D8NyZ%5FX%5FJ4Xv2MyvA34pqYKmR4kunbBAQFtHGncTQgBwIlVqTLsYk2MjzNKJ7JEilsbe2dYGHtPn8VT%2DZCZr06s%5F%5F%5F7v3%5F%5F%5F%5F%5F%5F7oGUEEmGpfAQJCWMBJNmlUKIEIVxIVAGACihGFo0sNCRwU7I4CPUACAAAYgIQIgQYgohZBAAAAAElEQAgAwIBEARAIAAQAjQEIACJAEFgBIGAQACoGhYARRBCBIQYHBUcogQFSLRQTzAAA";
        GdprConsentDomain gdprConsentDomain = gdprConsentHandler.handleGdprConsent(targetUrl, ChannelIdEnum.DAP);
        assert gdprConsentDomain.isAllowedStoredContextualData();
        assert gdprConsentDomain.isAllowedStoredPersonalizedData();
    }

    @Test
    public void testPurpose123456() {
        CouchbaseClient instance = CouchbaseClient.getInstance();
        instance.put(CouchbaseKeyConstant.PURPOSE_VENDOR_ID, "[22]", 0);
        String targetUrl = "http://www.ebayadservices.com/marketingtracking/v1/ar?mpt=682877911&ff18=mWeb&siteid=0&icep_siteid=0&ipn=admain2&adtype=3&size=300x250&pgroup=532260&gdpr=1&gdpr_consent=CO9HbRYO9HbRYMEAAAENAwCAAPwAAAAAAAAAAAAAAAAA.IGLtV_T9fb2vj-_Z99_tkeYwf95y3p-wzhheMs-8NyZeH_B4Wv2MyvBX4JiQKGRgksjLBAQdtHGlcTQgBwIlViTLMYk2MjzNKJrJEilsbO2dYGD9Pn8HT3ZCY70-vv__7v3ff_3g";
        GdprConsentDomain gdprConsentDomain = gdprConsentHandler.handleGdprConsent(targetUrl, ChannelIdEnum.DAP);
        assert !gdprConsentDomain.isAllowedStoredContextualData();
        assert !gdprConsentDomain.isAllowedStoredPersonalizedData();
    }
}
