package com.ebay.app.raptor.chocolate.request;

import com.ebay.app.raptor.chocolate.eventlistener.request.CustomizedSchemeRequestHandler;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CustomizedSchemeRequestHandlerTest {

    CustomizedSchemeRequestHandler customizedSchemeRequestHandler = new CustomizedSchemeRequestHandler();

    @Test
    public void testCustomizedSchemeRequestHandler() {
        // XC-1797, extract and decode actual target url from referrer parameter in targetUrl, only accept the url when the domain of referrer parameter belongs to ebay sites
        // valid target url in referrer parameter: ebay://link?
        String targetURL = "ebay://link?nav=home&referrer=https%3A%2F%2Fwww.ebay.com%3Fmkevt%3D1%26mkcid%3D1%26mkrid%3D711-53200-19255-0%26toolid%3D11800%26campid%3D5338433963%26customid%3Dfakesrctok-app-test";
        String referer = "www.google.com";
        Event event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals("https://www.ebay.com?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&toolid=11800&campid=5338433963&customid=fakesrctok-app-test&ff17=referrerdeeplink", event.getTargetUrl());
        assertEquals(referer, event.getReferrer());
        targetURL = "ebay://link?nav=home&referrer=https%3A%2F%2Fwww.ebay.com%2F%3Fmkevt%3D1%26mkcid%3D1%26mkrid%3D711-53200-19255-0%26toolid%3D11800%26campid%3D5338433963%26customid%3Dfakesrctok-app-test";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals("https://www.ebay.com/?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&toolid=11800&campid=5338433963&customid=fakesrctok-app-test&ff17=referrerdeeplink", event.getTargetUrl());
        assertEquals(referer, event.getReferrer());

        // valid target url in referrer parameter: ebay://link/?
        targetURL = "ebay://link/?nav=home&referrer=https%3A%2F%2Fwww.ebay.com%2F%3Fmkevt%3D1%26mkcid%3D1%26mkrid%3D711-53200-19255-0%26toolid%3D11800%26campid%3D5338433963%26customid%3Dfakesrctok-app-test";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals("https://www.ebay.com/?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&toolid=11800&campid=5338433963&customid=fakesrctok-app-test&ff17=referrerdeeplink", event.getTargetUrl());
        assertEquals(referer, event.getReferrer());
        targetURL = "ebay://link/?nav=home&referrer=https%3A%2F%2Fwww.ebay.com%3Fmkevt%3D1%26mkcid%3D1%26mkrid%3D711-53200-19255-0%26toolid%3D11800%26campid%3D5338433963%26customid%3Dfakesrctok-app-test";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals("https://www.ebay.com?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&toolid=11800&campid=5338433963&customid=fakesrctok-app-test&ff17=referrerdeeplink", event.getTargetUrl());
        assertEquals(referer, event.getReferrer());

        // XC-3349, for native uri with Chocolate parameters, re-construct Chocolate url based on native uri and track (only support /itm page)
        //valid Chocolate params in native uri
        targetURL = "ebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=1&mkrid=710-53481-19255-0&campid=5337369893&toolid=11800&customid=test&referrer=https%3A%2F%2Frover.ebay.com%2Frover%2F1%2F711-53200-19255-0%2F1";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals("https://www.ebay.co.uk/itm/154347659933?mkevt=1&mkcid=1&mkrid=710-53481-19255-0&campid=5337369893&toolid=11800&customid=test&ff17=chocodeeplink", event.getTargetUrl());
        assertEquals(referer, event.getReferrer());

        //no valid tracking parameters in deeplink url
        targetURL = "ebay://link?nav=item.view&id=154347659933";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals(null, event);
        targetURL = "ebay://link?nav=item.view&id=154347659933&mkevt=1";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals(null, event);


        targetURL = "ebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=1";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals("ebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=1", event.getTargetUrl());
        assertEquals(referer, event.getReferrer());

        targetURL = "ebay://link?nav=item.view&mkevt=1&mkcid=1&mkrid=710-53481-19255-0";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals("ebay://link?nav=item.view&mkevt=1&mkcid=1&mkrid=710-53481-19255-0", event.getTargetUrl());
        assertEquals(referer, event.getReferrer());

        targetURL = "ebay://link?nav=item.view&id=&mkevt=1&mkcid=1&mkrid=710-53481-19255-0";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals(null, event);

        targetURL = "ebay://link?nav=home&id=154347659933&mkevt=1&mkcid=1&mkrid=710-53481-19255-0";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals(null, event);

        targetURL = "ebay://link?nav=item.view&id=154347659933mkevt=1&mkcid=1&mkrid=12345-53481-19255-0";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals(null, event);

        targetURL = "ebay://link?nav=home&id=154347659933&mkevt=1&mkcid=4&mkrid=710-53481-19255-0";
        event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals("ebay://link?nav=home&id=154347659933&mkevt=1&mkcid=4&mkrid=710-53481-19255-0", event.getTargetUrl());
        assertEquals(referer, event.getReferrer());
    }

    @Test
    public void testCustomizedSchemeRequestHandler_withDuplicatedMkcid() {
        String targetURL = "ebay://link?nav=item.view&id=185038555159&mkcid=16&mkcid=4&mkrid=711-127632-2357-0&mkrid=711-58542-18990-20-3&var=692814868220&ul_alt=store&sabg=770ee4d017f0ab9e2a6aa0e2ffe9083d&sabc=770ee8c217f0a1201592dd40ffaa0be2&mkevt=1&ufes_redirect=true";
        String referer = "www.google.com";
        Event event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals("ebay://link?nav=item.view&id=185038555159&mkcid=16&bannercid=4&mkrid=711-127632-2357-0&bannerrid=711-58542-18990-20-3&var=692814868220&ul_alt=store&sabg=770ee4d017f0ab9e2a6aa0e2ffe9083d&sabc=770ee8c217f0a1201592dd40ffaa0be2&mkevt=1&ufes_redirect=true", event.getTargetUrl());
        assertEquals(referer, event.getReferrer());
    }

    @Test
    public void testCustomizedSchemeRequestHandler_withInvalidMkrid() {
        String targetURL = "ebay://link?nav=item.view&id=185038555159&mkcid=16&mkcid=4&mkrid=711-127632-2357-0&mkrid=711-58542-54321-20-3&var=692814868220&ul_alt=store&sabg=770ee4d017f0ab9e2a6aa0e2ffe9083d&sabc=770ee8c217f0a1201592dd40ffaa0be2&mkevt=1&ufes_redirect=true";
        String referer = "www.google.com";
        Event event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals(targetURL, event.getTargetUrl());
        assertEquals(referer, event.getReferrer());
    }

    @Test
    public void testCustomizedSchemeRequestHandler_withReverseOrder() {
        String targetURL = "ebay://link?nav=item.view&id=185038555159&mkcid=4&mkcid=16&mkrid=711-58542-18990-20-3&mkrid=711-127632-2357-0&var=692814868220&ul_alt=store&sabg=770ee4d017f0ab9e2a6aa0e2ffe9083d&sabc=770ee8c217f0a1201592dd40ffaa0be2&mkevt=1&ufes_redirect=true";
        String referer = "www.google.com";
        Event event = customizedSchemeRequestHandler.parseCustomizedSchemeEvent(targetURL, referer);
        assertEquals("ebay://link?nav=item.view&id=185038555159&bannercid=4&mkcid=16&bannerrid=711-58542-18990-20-3&mkrid=711-127632-2357-0&var=692814868220&ul_alt=store&sabg=770ee4d017f0ab9e2a6aa0e2ffe9083d&sabc=770ee8c217f0a1201592dd40ffaa0be2&mkevt=1&ufes_redirect=true", event.getTargetUrl());
        assertEquals(referer, event.getReferrer());
    }
}