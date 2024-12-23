package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.eventlistener.component.AdsCollectionSvcClient;
import com.ebay.app.raptor.chocolate.eventlistener.model.AdsCollectionSvcRequest;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import static org.junit.Assert.*;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;

public class AdsClickCollectorTest {
    AdsClickCollector adsClickCollector = new AdsClickCollector();

    @Test
    public void nullEvent() {
        ImmutablePair<String, Boolean> adsSignals = adsClickCollector.getAdsSignals(null);
        assertTrue(adsSignals.left == null);
        assertFalse(adsSignals.right);
    }

    @Test
    public void noAmdataQueryParam() {
        Event event = new Event();
        event.setTargetUrl("http://www.ebay.com/itm?ab=1&uu=2");
        ImmutablePair<String, Boolean> adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.left == null);
        assertFalse(adsSignals.right);
    }

    @Test
    public void noQueryParam() {
        Event event = new Event();
        event.setTargetUrl("http://www.ebay.com/itm");
        ImmutablePair<String, Boolean> adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.left == null);
        assertFalse(adsSignals.right);
    }

    @Test
    public void badUrl() {
        Event event = new Event();
        event.setTargetUrl("htt//www.ebay.com/i/174202315485chn=ps&norover=1&mkevt=1&mkrid=711-117182-37290-0" +
                "&mkcid=2&itemid=174202315485&targetid=917373795544&device=m&mktype=pla&googleloc=9026251&poi=&ca" +
                "mpaignid=9343999128&mkgroupid=103102745668&rlsatarget=aud-622524041518:pla-917373795544&abcId=113933" +
                "6&merchantid=6296724&gclid=CjwKCAjw_-D3BRBIEiwAjVMy7B4Jv46jyG-zi3ZwE5NjtbUikf1dId1ikR6X35Pc" +
                "8w4qDxXfaRIPvBoCyBcQAvD_BwE&amdata=enc%3DAQAFAAACYBaobrjLl8XobRIiIML1V4Imu%252Fn%252BzU5L90Z");
        ImmutablePair<String, Boolean> adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.left == null);
        assertFalse(adsSignals.right);
    }

    @Test
    public void encValuePresent() {
        Event event = new Event();
        event.setTargetUrl("https://www.ebay.com/i/174202315485?qparan=enc%3AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw");
        IEndUserContext endUserContext = mock(IEndUserContext.class);
        doReturn("ebayUserAgent/eBayIOS;6.4.0;iOS;13.5.1;Apple;iPhone12_1;Vodafone.de;414x896;2.0")
                .when(endUserContext).getUserAgent();
        ImmutablePair<String, Boolean> adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.getLeft().equals("enc%3AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw"));

        event.setTargetUrl("https://www.ebay.com/i/174202315485?qparan=tkp%3ABlBMUIzmrr6tYw%7Cenc%3AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D");
        adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.getLeft().equals("tkp%3ABlBMUIzmrr6tYw%7Cenc%3AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D"));

        event.setTargetUrl("https://www.ebay.com/i/174202315485?qparan=enc:AQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw");
        adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.getLeft().equals("enc:AQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw"));

        event.setTargetUrl("https://www.ebay.com/i/174202315485?qparan=enc%253AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw");
        adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.getLeft().equals("enc%253AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw"));

        event.setTargetUrl("https://www.ebay.com/i/174202315485?qparan=xyz");
        adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.getLeft() == null);

    }

    @Test
    public void encpdValuePresent() {
        Event event = new Event();
        event.setTargetUrl("https://www.ebay.com/i/174202315485?someparam=encpd%3AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw");
        IEndUserContext endUserContext = mock(IEndUserContext.class);
        doReturn("ebayUserAgent/eBayIOS;6.4.0;iOS;13.5.1;Apple;iPhone12_1;Vodafone.de;414x896;2.0")
                .when(endUserContext).getUserAgent();
        ImmutablePair<String, Boolean> adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.getLeft().equals("encpd%3AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw"));
        event.setTargetUrl("https://www.ebay.com/i/174202315485?someparam2=tkp%3ABlBMUIzmrr6tYw%7Cencpd%3AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D");
        adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.getLeft().equals("tkp%3ABlBMUIzmrr6tYw%7Cencpd%3AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D"));
        event.setTargetUrl("https://www.ebay.com/i/174202315485?someparam=encpd:AQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw");
        adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.getLeft().equals("encpd:AQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw"));
        event.setTargetUrl("https://www.ebay.com/i/174202315485?someparam=encpd%253AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw");
        adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.getLeft().equals("encpd%253AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw"));
    }

    private Event createEvent() {
        Event event = new Event();
        event.setTargetUrl("https://www.ebay.com/i/174202315485?chn=ps&norover=1&mkevt=1&mkrid=711-117182-37290-0&mkcid" +
                "=2&itemid=174202315485&targetid=917373795544&device=m&mktype=pla&googleloc=9026251&poi=&campaignid=" +
                "9343999128&mkgroupid=103102745668&rlsatarget=aud-622524041518:pla-917373795544&abcId=1139336&merch" +
                "antid=6296724&gclid=CjwKCAjw_-D3BRBIEiwAjVMy7B4Jv46jyG-zi3ZwE5NjtbUikf1dId1ikR6X35Pc8w4qDxXfaRIPvBoC" +
                "yBcQAvD_BwE&amdata=enc%3DAQAFAAACYBaobrjLl8XobRIiIML1V4Imu%252Fn%252BzU5L90Z278x5ickkCrLl8erj3ATP5" +
                "raQxjc%252F%252BwHq0YAKAXzfyyCUhptJfd%252FsQ6sFAR1lw0PxvnTyMEWIetCsI2fhQEzI68SnA4vOsjXUl%252FQEqdsU" +
                "FsuOVweoEwcFV2xaSmhGNPtiX9uqNMDFotOqFmrty4w2hCFbdwh2jbsIpNrwp8bcUtTG2dF35c2qeTvC%252Fj1KRbESs7exTbXyD" +
                "tvHJv1I9UYZmW8OKn9oZEbfIZG7Bk9g0UYjgNDsTDmwI%252BnJ%252Bwh1yvPy%252FMYFdozVTP7AdDT67iM9ZMPdIK1JDeu9n" +
                "btmNXy1x3udkkUJghs7jYuI5D4mr4gOCg6JPt5FqQKD1v3kyx2bOXQUYa7KHOPp%252FZEEYnNW1WHUKWwkxvvQCkNfPcRwATEESfA" +
                "jQGPbi38Ua7FHdArbKaP9QI%252F%252FGkYIUpaKj%252BF1DIZyh6QYl40CmNwXJOObQWaalfjvaP4FL5DeQjpBnP67KbpLp" +
                "fzpaX%252F2U9ZgwljXti%252BtJ4XpBZXWpV7b4xdOJuk6Vf9o9JkWNKAijHBSCeLcw2diqmI5Z59o58sw1ygW22Y077hKA2Zm" +
                "VTpEnxYw9cfrDrjPXMES%252BITIue83G58c%252BSfMCGtl%252BvpBDjdbZK3HP0BBSfn0wW1TwW36mauh1QLKTUTlJySrrMEP" +
                "3Y1gm4X2b86avwMnYSgiDjY6DPpxIns60OCxCltCw2luPF82gldw2uuawK7KhG3YFpx1trHSerhPXvJ2Frp%252BVC3cNd2o14" +
                "0glxoU1NPw8OEduGaOnYHnLrD3eTCs%26cksum%3D333013098554c02c005675a946be992fdc824cdeb8be%26ampid%3DPL_CLK%26clp%3D233452");
        return event;

    }

    @Test
    public void plPayloadPresent() {
        Event event = createEvent();
        String expectedPayload = "enc%3DAQAFAAACYBaobrjLl8XobRIiIML1V4Imu%252Fn%252BzU5L90Z278x5ickkCrLl8erj3ATP5raQxjc%252F%252BwHq0YAKAXzfyyCUhptJfd%252FsQ6sFAR1lw0PxvnTyMEWIetCsI2fhQEzI68SnA4vOsjXUl%252FQEqdsUFsuOVweoEwcFV2xaSmhGNPtiX9uqNMDFotOqFmrty4w2hCFbdwh2jbsIpNrwp8bcUtTG2dF35c2qeTvC%252Fj1KRbESs7exTbXyDtvHJv1I9UYZmW8OKn9oZEbfIZG7Bk9g0UYjgNDsTDmwI%252BnJ%252Bwh1yvPy%252FMYFdozVTP7AdDT67iM9ZMPdIK1JDeu9nbtmNXy1x3udkkUJghs7jYuI5D4mr4gOCg6JPt5FqQKD1v3kyx2bOXQUYa7KHOPp%252FZEEYnNW1WHUKWwkxvvQCkNfPcRwATEESfAjQGPbi38Ua7FHdArbKaP9QI%252F%252FGkYIUpaKj%252BF1DIZyh6QYl40CmNwXJOObQWaalfjvaP4FL5DeQjpBnP67KbpLpfzpaX%252F2U9ZgwljXti%252BtJ4XpBZXWpV7b4xdOJuk6Vf9o9JkWNKAijHBSCeLcw2diqmI5Z59o58sw1ygW22Y077hKA2ZmVTpEnxYw9cfrDrjPXMES%252BITIue83G58c%252BSfMCGtl%252BvpBDjdbZK3HP0BBSfn0wW1TwW36mauh1QLKTUTlJySrrMEP3Y1gm4X2b86avwMnYSgiDjY6DPpxIns60OCxCltCw2luPF82gldw2uuawK7KhG3YFpx1trHSerhPXvJ2Frp%252BVC3cNd2o140glxoU1NPw8OEduGaOnYHnLrD3eTCs%26cksum%3D333013098554c02c005675a946be992fdc824cdeb8be%26ampid%3DPL_CLK%26clp%3D233452";
        assertTrue(adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event)).left.equals(expectedPayload));
    }

    private MultiValueMap<String, String> createMultiValueMap() {
        MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
        queryParams.add("key1", "value1");
        queryParams.add("amdata", "value2");
        return queryParams;
    }
    @Test
    public void nullEndUserCtxt() {
        assertFalse(adsClickCollector.isInvokeAdsSvc(null, new ImmutablePair<>("amdata", Boolean.FALSE)));
    }

    @Test
    public void webUserAgent() {
        IEndUserContext endUserContext = mock(IEndUserContext.class);
        doReturn("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36").when(endUserContext).getUserAgent();
        assertFalse(adsClickCollector.isInvokeAdsSvc(endUserContext, new ImmutablePair<>("amdata", Boolean.FALSE)));
    }

    @Test
    public void ios() {
        IEndUserContext endUserContext = mock(IEndUserContext.class);
        doReturn("ebayUserAgent/eBayIOS;6.4.0;iOS;13.5.1;Apple;iPhone12_1;Vodafone.de;414x896;2.0")
                .when(endUserContext).getUserAgent();
        assertTrue(adsClickCollector.isInvokeAdsSvc(endUserContext, new ImmutablePair<>("amdata", Boolean.FALSE)));
    }

    @Test
    public void android() {
        IEndUserContext endUserContext = mock(IEndUserContext.class);
        doReturn("ebayUserAgent/eBayAndroid;6.4.0;Android;10;Google;sargo;T-Mobile;1080x2088;2.8")
                .when(endUserContext).getUserAgent();
        assertTrue(adsClickCollector.isInvokeAdsSvc(endUserContext, new ImmutablePair<>("amdata", Boolean.FALSE)));
    }

    @Test
    public void nullAmdata() {
        IEndUserContext endUserContext = mock(IEndUserContext.class);
        doReturn("ebayUserAgent/eBayAndroid;6.4.0;Android;10;Google;sargo;T-Mobile;1080x2088;2.8")
                .when(endUserContext).getUserAgent();
        assertTrue(adsClickCollector.isInvokeAdsSvc(endUserContext, new ImmutablePair<>(null, Boolean.TRUE)));
    }

    @Test
    public void emptyAmdata() {
        IEndUserContext endUserContext = mock(IEndUserContext.class);
        doReturn("ebayUserAgent/eBayAndroid;6.4.0;Android;10;Google;sargo;T-Mobile;1080x2088;2.8")
                .when(endUserContext).getUserAgent();
        assertTrue(adsClickCollector.isInvokeAdsSvc(endUserContext, new ImmutablePair<>("", Boolean.TRUE)));
    }

    @Test
    public void noAmdataAndNoPayloadess() {
        IEndUserContext endUserContext = mock(IEndUserContext.class);
        doReturn("ebayUserAgent/eBayAndroid;6.4.0;Android;10;Google;sargo;T-Mobile;1080x2088;2.8")
                .when(endUserContext).getUserAgent();
        Event event = new Event();
        event.setTargetUrl("https://www.ebay.com/itm/174202315485?chn=ps");

        assertFalse(adsClickCollector.isInvokeAdsSvc(endUserContext,
                adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event))));
    }

    @Test
    public void payloadLessEpn() {
        Event event = new Event();
        event.setTargetUrl("https://www.ebay.com/i/174202315485?chn=ps&norover=1&mkevt=1&mkrid=711-117182-37290-0&mkcid" +
                "=1&itemid=174202315485&targetid=917373795544&device=m&mktype=pla&googleloc=9026251&poi=&campid=" +
                "9343999128&mkgroupid=103102745668&rlsatarget=aud-622524041518:pla-917373795544&abcId=1139336&merch" +
                "antid=6296724&gclid=CjwKCAjw_-D3BRBIEiwAjVMy7B4Jv46jyG-zi3ZwE5NjtbUikf1dId1ikR6X35Pc8w4qDxXfaRIPvBoC" +
                "yBcQAvD_BwE");
        ImmutablePair<String, Boolean> adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertTrue(adsSignals.right);
        assertNull(adsSignals.left);
    }

    @Test
    public void payloadLessMissingMkevt() {
        Event event = new Event();
        event.setTargetUrl("https://www.ebay.com/i/174202315485?chn=ps&norover=1&mkrid=711-117182-37290-0&mkcid" +
                "=&itemid=174202315485&targetid=917373795544&device=m&mktype=pla&googleloc=9026251&poi=&campid=" +
                "33333&mkgroupid=103102745668&rlsatarget=aud-622524041518:pla-917373795544&abcId=1139336&merch" +
                "antid=6296724&gclid=CjwKCAjw_-D3BRBIEiwAjVMy7B4Jv46jyG-zi3ZwE5NjtbUikf1dId1ikR6X35Pc8w4qDxXfaRIPvBoC" +
                "yBcQAvD_BwE");
        ImmutablePair<String, Boolean> adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertFalse(adsSignals.right);
        assertNull(adsSignals.left);

        event.setTargetUrl("https://www.ebay.com/i/174202315485?chn=ps&norover=1&mkrid=711-117182-37290-0&mkcid" +
                "=2&itemid=174202315485&targetid=917373795544&device=m&mktype=pla&googleloc=9026251&poi=&campid=" +
                "3333&mkgroupid=103102745668&rlsatarget=aud-622524041518:pla-917373795544&abcId=1139336&merch" +
                "antid=6296724&gclid=CjwKCAjw_-D3BRBIEiwAjVMy7B4Jv46jyG-zi3ZwE5NjtbUikf1dId1ikR6X35Pc8w4qDxXfaRIPvBoC" +
                "yBcQAvD_BwE");
        adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertFalse(adsSignals.right);
        assertNull(adsSignals.left);

        event.setTargetUrl("https://www.ebay.com/i/174202315485?chn=ps&norover=1&mkrid=711-117182-37290-0" +
                "&itemid=174202315485&targetid=917373795544&device=m&mktype=pla&googleloc=9026251&poi=" +
                "333&mkgroupid=103102745668&rlsatarget=aud-622524041518:pla-917373795544&abcId=1139336&merch" +
                "antid=6296724&gclid=CjwKCAjw_-D3BRBIEiwAjVMy7B4Jv46jyG-zi3ZwE5NjtbUikf1dId1ikR6X35Pc8w4qDxXfaRIPvBoC" +
                "yBcQAvD_BwE");
        adsSignals = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertFalse(adsSignals.right);
        assertNull(adsSignals.left);
    }

    private MultivaluedMap<String, String> createHeader() {
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        List<String> list1 = new ArrayList<>();
        list1.add("clientId=urn%3Aebay-marketplace-consumerid%3A409a9203-ad93-4932-a101-485637812d0d,ip=10.224.57.159,userAgentAccept=text%2Fhtml%2Capplication%2Fxhtml%2Bxml%2Capplication%2Fxml%3Bq%3D0.9%2Cimage%2Fwebp%2Cimage%2Fapng%2C*%2F*%3Bq%3D0.8%2Capplication%2Fsigned-exchange%3Bv%3Db3,userAgentAcceptEncoding=gzip%2C+deflate,userAgent=Mozilla%2F5.0+(Macintosh%3B+Intel+Mac+OS+X+10_14_6)+AppleWebKit%2F537.36+(KHTML%2C+like+Gecko)+Chrome%2F78.0.3904.87+ Safari%2F537.36,uri=%2F,applicationURL=http%3A%2F%2Flvsadtrackhandlersample-3711473.lvs02.dev.ebayc3.com%3A8080%2F%3Famdata%3D%257B%2522enc%2522%253A%2522AQAFAAACIJTCObh01lRshq6SSqqfH3WQwGz0UI8axZzmevNEvXI54PLKnoki9RTrH7VxhIKdqX9GysFTjSpZZBS%252F7ijIy9J7ewapJiBi8EZr%252F6GjI%252BLJ0lnJ4H16uClzxxPY9qXRsHrp0BFJ8GrQlf3FtCgYLQ9F5iQq38ayx2ib6%252Fe73QxNqupgEJYLTkEQr08ea2%252BJvUvzkyNRZpiajjSAaur%252FqOk9%252FS879Mc%252BpCeV%252BaS%252F0zgj1KFAhuabi9pS%252FKMjnK3fm8DIil3pbGL96CvSWFqJp5PwYfSphP2lcyuobp2In2MwF8EF3Swp6T8bGQ0gPxJGMCbCZ3xs1h2O8vVTntN0hhr9wJfaoEC5WQzNnxxZYh6Tab9R%252FEZpJxBJjOkQomi6RG5hQJtLAkQM%252B88zaOkn0pxno7m70PWG6lQmjrQRn%252BshJmzev%252BwLan2x4%252B5mIMycD6fql3LTyaEzSVzFn8XUYDXQTlaKNSAl%252BjFnmGYFTM6VsX8PSd2mKSwnETzVsZZxuxS3eNlHxOvpUDpMZh54EaY9TRK%252FqgES3wYjrmqA57e6HkAEpi5IWb47q2LM3lxytCe0LGSYzmpM3lGMrfGd88Gy0PdGv2kJq%252FBwZofCwBhocJgjS7z66A%252Bm0dRb4cBV0s0V9Itj6Cq9GcY8ADhsARpf%252F6YrGnnBQYZYQcs%252BUeTe0zV7hw2qzo85LgqX%252BNgG7gfDc7gkp0Yi3YOZCPa3ogU%253D%2522%252C%2522cksum%2522%253A%2520%2522checksum%2520of%2520enc%2522%252C%2522ampid%2522%253A%2520%2522PL_CLK%2522%252C%2522clp%2522%253A%2520512325%252C%2522ekp%2522%253A%2520true%257D,expectSecureURL=false,physicalLocation=country%3DUS");
        headers.put("X-EBAY-C-ENDUSERCTX",list1);
        List<String> list2 = new ArrayList<>();
        list2.add("guid=8b99eec616c0a9ccdb932f64fcc116b3,cguid=8b99eed116c0a9ccdb932f64fcc116b1,tguid=8b99eec616c0a9ccdb932f64fcc116b3,pageid=2562720,cobrandId=2");
        headers.put("X-EBAY-C-TRACKING",list2);
        return headers;
    }

    @Test
    public void invokeAdSvc() {
        IEndUserContext endUserContext = mock(IEndUserContext.class);
        doReturn("ebayUserAgent/eBayAndroid;6.4.0;Android;10;Google;sargo;T-Mobile;1080x2088;2.8")
                .when(endUserContext).getUserAgent();
        Event event = new Event();
        event.setTargetUrl("http://www.ebay.com/itm?ab=1&uu=2&amdata=yui");
        event.setReferrer("http://www.google.com");
        AdsCollectionSvcClient adsCollectionSvcClient = mock(AdsCollectionSvcClient.class);
        adsClickCollector.adsCollectionSvcClient = adsCollectionSvcClient;
        doNothing().when(adsCollectionSvcClient).invokeService(any(AdsCollectionSvcRequest.class), anyString(), any(MultivaluedMap.class));
        adsClickCollector.processPromotedListingClick(endUserContext, event, createHeader());
        verify(adsCollectionSvcClient, times(1)).invokeService(any(AdsCollectionSvcRequest.class), anyString(), any(MultivaluedMap.class));
    }

    @Test
    public void buildRequest() {
        Event event = new Event();
        event.setTargetUrl("targetUrl");
        event.setReferrer("referrer");

        String amdata = "enc%3AencValue";
        AdsCollectionSvcRequest request = adsClickCollector.createAdsRequest(event, amdata);
        assertTrue(request.getAmdata().contains("enc%3AencValue%7Ctsp%3A"));
        assertTrue(request.getReferrer().equals("referrer"));
        assertTrue(request.getRequestUrl().equals("targetUrl"));
    }

    @Test
    public void buildRequestNoAmdataQueryParam() {
        Event event = new Event();
        event.setTargetUrl("https://www.ebay.com/i/174202315485?qparan=enc%3AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw");
        ImmutablePair<String, Boolean> adSignal = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));

        AdsCollectionSvcRequest request = adsClickCollector.createAdsRequest(event, adSignal.getLeft());
        assertTrue(request.getAmdata().contains("enc%3AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw"));
        assertTrue(request.getRequestUrl().equals("https://www.ebay.com/i/174202315485?qparan=enc%3AAQAJAAAAwHpAtFBN53uX%2FhF7olfs5c0c6VjAD%2BeWVThrk6BVedP9Pn7%2FkBfl6LKMkUUm8inVqdH4WNlxqotzwh8nSflDqy0YN91camaDnhq%2BF9AZQittV6DdoB2BFujDPkc3N0SvI%2F3IjS%2Ba0S03T4nCuWbzsfN8q36r91CopT%2FJ7PbyB8p7CBH7F6xIejd3OAvuiYTcEkc1GPd%2FNME7hVHE1SYoT0NHZQJSsS6S4TVH%2FH6TqBO2xqaDLcbEY%2FFsTbDDuaDHZw%3D%3D%7Ctkp%3ABlBMUIzmrr6tYw"));
    }

    @Test
    public void invalidMarketingUrl() {
        Event event = new Event();
        event.setTargetUrl("https://www.ebay.com/itm/325988063928?mkevt=1&plmtId=1110100102&mesgId=3041&mkpid=0&emsid=enull.m161560.l174179&mkcid=7&ch=osgood&euid&bu=43128145354&trkId=5ed6ebd9-c10b-43e4-a0a8-53e6600368a5&cnvId=a7fb74a1-c76a-4a5d-81a4-a4efd73d9c0c&osub=-1%7E1&crd=20240206142412&segname&recoId=325988063928&recoPos=5&autorefresh=true");
        ImmutablePair<String, Boolean> adSignal = adsClickCollector.getAdsSignals(adsClickCollector.getQueryParams(event));
        assertNull(adSignal.getLeft());
        assertTrue(adSignal.getRight());
    }
    @Test
    public void payloadLessEpnRequest() {
        Event event = new Event();
        event.setTargetUrl("targetUrl");
        event.setReferrer("referrer");

        AdsCollectionSvcRequest request = adsClickCollector.createAdsRequest(event, null);
        assertNull(request.getAmdata());
        assertTrue(request.getReferrer().equals("referrer"));
        assertTrue(request.getRequestUrl().equals("targetUrl"));
    }

    @Test
    public void amdataPresent() {
        assertFalse(adsClickCollector.amdataPresent(null));
        assertFalse(adsClickCollector.amdataPresent(""));
        assertFalse(adsClickCollector.amdataPresent("   "));
        assertTrue(adsClickCollector.amdataPresent("xxx"));
    }
}
