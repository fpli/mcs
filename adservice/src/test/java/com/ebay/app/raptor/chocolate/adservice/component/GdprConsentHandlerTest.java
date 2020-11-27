package com.ebay.app.raptor.chocolate.adservice.component;

import com.ebay.app.raptor.chocolate.adservice.util.CouchbaseClient;
import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import com.ebay.app.raptor.chocolate.constant.CouchbaseKeyConstant;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class GdprConsentHandlerTest {
    @Autowired
    private GdprConsentHandler gdprConsentHandler;

    @Test
    public void testPurpose123456ForAr() {
        CouchbaseClient instance = CouchbaseClient.getInstance();
        instance.put(CouchbaseKeyConstant.ENABLE_TCF_COMPLIANCE_MODE, "true", 0);
        instance.put(CouchbaseKeyConstant.PURPOSE_VENDOR_ID, "[22]", 0);
        String targetUrl = "http://localhost:8080/marketingtracking/v1/ar?mpt=682877911&ff18=mWeb&siteid=0&icep_siteid=0&ipn=admain2&adtype=3&size=300x250&pgroup=532260&gdpr=1&gdpr_consent=CO9el_uO9el_uHrAAAENAwCAAPwAAAAAAAAAALAAABAAAAAA.IGLtV_T9fb2vj-_Z99_tkeYwf95y3p-wzhheMs-8NyZeH_B4Wv2MyvBX4JiQKGRgksjLBAQdtHGlcTQgBwIlViTLMYk2MjzNKJrJEilsbO2dYGD9Pn8HT3ZCY70-vv__7v3ff_3g";
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest(HttpMethod.GET.name(), targetUrl);
        mockHttpServletRequest.setParameter("gdpr","1");
        mockHttpServletRequest.setParameter("gdpr_consent","CO9el_uO9el_uHrAAAENAwCAAPwAAAAAAAAAALAAABAAAAAA.IGLtV_T9fb2vj-_Z99_tkeYwf95y3p-wzhheMs-8NyZeH_B4Wv2MyvBX4JiQKGRgksjLBAQdtHGlcTQgBwIlViTLMYk2MjzNKJrJEilsbO2dYGD9Pn8HT3ZCY70-vv__7v3ff_3g");
        GdprConsentDomain gdprConsentDomain = gdprConsentHandler.handleGdprConsent(mockHttpServletRequest);
        assert gdprConsentDomain.isAllowedUseGeoInfo();
        assert gdprConsentDomain.isAllowedSetCookie();
        assert gdprConsentDomain.isAllowedUseContextualInfo();
    }

    @Test
    public void testPurpose1ForAr() {
        CouchbaseClient instance = CouchbaseClient.getInstance();
        instance.put(CouchbaseKeyConstant.ENABLE_TCF_COMPLIANCE_MODE, "true", 0);
        instance.put(CouchbaseKeyConstant.PURPOSE_VENDOR_ID, "[22]", 0);
        String targetUrl = "http://localhost:8080/marketingtracking/v1/ar?mpt=682877911&ff18=mWeb&siteid=0&icep_siteid=0&ipn=admain2&adtype=3&size=300x250&pgroup=532260&gdpr=1&gdpr_consent=CO9hOUaO9hOUaL0AAAENAwCAAIAAAAAAAAAAALAAABAAAAAA.IGLtV_T9fb2vj-_Z99_tkeYwf95y3p-wzhheMs-8NyZeH_B4Wv2MyvBX4JiQKGRgksjLBAQdtHGlcTQgBwIlViTLMYk2MjzNKJrJEilsbO2dYGD9Pn8HT3ZCY70-vv__7v3ff_3g";
        MockHttpServletRequest mockHttpServletRequest = new MockHttpServletRequest(HttpMethod.GET.name(), targetUrl);
        mockHttpServletRequest.setParameter("gdpr","1");
        mockHttpServletRequest.setParameter("gdpr_consent","CO9hOUaO9hOUaL0AAAENAwCAAIAAAAAAAAAAALAAABAAAAAA.IGLtV_T9fb2vj-_Z99_tkeYwf95y3p-wzhheMs-8NyZeH_B4Wv2MyvBX4JiQKGRgksjLBAQdtHGlcTQgBwIlViTLMYk2MjzNKJrJEilsbO2dYGD9Pn8HT3ZCY70-vv__7v3ff_3g");
        GdprConsentDomain gdprConsentDomain = gdprConsentHandler.handleGdprConsent(mockHttpServletRequest);
        assert !gdprConsentDomain.isAllowedUseGeoInfo();
        assert !gdprConsentDomain.isAllowedSetCookie();
        assert !gdprConsentDomain.isAllowedUseContextualInfo();
    }
}
