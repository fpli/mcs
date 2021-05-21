package chocolate.util;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.platform.raptor.raptordds.parsers.UserAgentParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = EventListenerApplication.class)
public class CollectionServiceUtilTest {
    @Test
    public void testIsDuplicateItmClick() {
        boolean isDuplicateItemClick = false;

        // non-itm page
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/i/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.co.uk/scp/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        // item page with title
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/asdfwerw/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/23362adfawerqwet/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/adfawerqwe321434/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        // bot click
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "Slackbot-LinkExpanding 1.0 (+https://api.slack.com/robots)"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "GingerClient"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", true, true, true);
        assertEquals(false, isDuplicateItemClick);

        // user clicks from special sites
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.nl/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://befr.ebay.be/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com.my/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        // user clicks from dweb
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, false, false);
        assertEquals(false, isDuplicateItemClick);

        // user clicks from tablet
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, false);
        assertEquals(false, isDuplicateItemClick);

        // user clicks from native app
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, false, false);
        assertEquals(false, isDuplicateItemClick);

        // user clicks from non-special sites and mobile phone web
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(true, isDuplicateItemClick);

        // other marketing status code
        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("200", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("404", "checkoutApi"
                , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, true);
        assertEquals(false, isDuplicateItemClick);

        // throw exception
        isDuplicateItemClick = false;
        try {
            isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", "checkoutApi"
                    , null, false, true, true);
        } catch (Exception e) {
            System.out.println(e);
        }
        assertEquals(false, isDuplicateItemClick);

        try {
            isDuplicateItemClick = CollectionServiceUtil.isDuplicateItmClick("301", null
                    , "https://www.ebay.com/itm/233622232591?mkevt=1", false, true, true);
        } catch (Exception e) {
            System.out.println(e);
        }
        assertEquals(false, isDuplicateItemClick);

    }

    @Test
    public void testGetAppIdFromUserAgent() {
        UserAgentParser agentParser = new UserAgentParser();
        String appId;
        UserAgentInfo agentInfo;

        // mobile phone web
        agentInfo = agentParser.parse("Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) " +
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1");
        appId = CollectionServiceUtil.getAppIdFromUserAgent(agentInfo);
        assertEquals("3564", appId);

        // mobile tablet web
        agentInfo = agentParser.parse("Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) " +
            "AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1");
        appId = CollectionServiceUtil.getAppIdFromUserAgent(agentInfo);
        assertEquals("1115", appId);

        // iphone
        agentInfo = agentParser.parse("eBayiPhone/6.9.6");
        appId = CollectionServiceUtil.getAppIdFromUserAgent(agentInfo);
        assertEquals("1462", appId);

        // ipad
        agentInfo = agentParser.parse("eBayiPad/6.9.6");
        appId = CollectionServiceUtil.getAppIdFromUserAgent(agentInfo);
        assertEquals("2878", appId);

        // android
        agentInfo = agentParser.parse("ebayUserAgent/eBayAndroid;6.9.6;Android;10;OnePlus;" +
            "OnePlus6T;YES OPTUS;1080x2260;2.6");
        appId = CollectionServiceUtil.getAppIdFromUserAgent(agentInfo);
        assertEquals("2571", appId);
    }

    @Test
    public void testIsEPNPromotedListingsClick() {
        boolean isEPNPromotedListingClick = false;
        String targetUrl = "https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet";

        // click from promoted listing iframe on ebay partner site
        isEPNPromotedListingClick = CollectionServiceUtil.isEPNPromotedListingsClick(ChannelIdEnum.EPN, getTargetUrlParameters(targetUrl), "https://www.ebay.co.uk/gum/v1/stick?q=carpet%20cleaner");
        assertEquals(true, isEPNPromotedListingClick);

        // non-epn channel
        isEPNPromotedListingClick = CollectionServiceUtil.isEPNPromotedListingsClick(ChannelIdEnum.PAID_SEARCH, getTargetUrlParameters(targetUrl), "https://www.ebay.com/gum/v1/stick?q=carpet%20cleaner");
        assertEquals(false, isEPNPromotedListingClick);

        // no mksrc parameter
        targetUrl = "https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet";
        isEPNPromotedListingClick = CollectionServiceUtil.isEPNPromotedListingsClick(ChannelIdEnum.EPN, getTargetUrlParameters(targetUrl), "https://www.ebay.com/gum/v1/stick?q=carpet%20cleaner");
        assertEquals(false, isEPNPromotedListingClick);

        // mksrc<>"PromotedListings"
        targetUrl = "https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=Promoted&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet";
        isEPNPromotedListingClick = CollectionServiceUtil.isEPNPromotedListingsClick(ChannelIdEnum.EPN, getTargetUrlParameters(targetUrl), "https://www.ebay.com/gum/v1/stick?q=carpet%20cleaner");
        assertEquals(false, isEPNPromotedListingClick);

        // no plrfr param
        targetUrl = "https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings";
        isEPNPromotedListingClick = CollectionServiceUtil.isEPNPromotedListingsClick(ChannelIdEnum.EPN, getTargetUrlParameters(targetUrl), "https://www.ebay.com/gum/v1/stick?q=carpet%20cleaner");
        assertEquals(false, isEPNPromotedListingClick);

        // plrfr=""
        targetUrl = "https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings&plrfr=";
        isEPNPromotedListingClick = CollectionServiceUtil.isEPNPromotedListingsClick(ChannelIdEnum.EPN, getTargetUrlParameters(targetUrl), "https://www.ebay.com/gum/v1/stick?q=carpet%20cleaner");
        assertEquals(false, isEPNPromotedListingClick);

        // original referer=""
        targetUrl = "https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet";
        isEPNPromotedListingClick = CollectionServiceUtil.isEPNPromotedListingsClick(ChannelIdEnum.EPN, getTargetUrlParameters(targetUrl), "");
        assertEquals(false, isEPNPromotedListingClick);

        // original referer is not ebay domain
        targetUrl = "https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet";
        isEPNPromotedListingClick = CollectionServiceUtil.isEPNPromotedListingsClick(ChannelIdEnum.EPN, getTargetUrlParameters(targetUrl), "https://www.gumtree.com/v1/stick?q=carpet");
        assertEquals(false, isEPNPromotedListingClick);

        // original referer is ebay domain but doesn't contain '%/gum/%'
        targetUrl = "https://www.ebay.com/itm/233622232591?mkevt=1&mkcid=1&mkrid=711-53200-19255-0&mksrc=PromotedListings&plrfr=https%3A%2F%2Fwww.gumtree.com%2Fv1%2Fstick%3Fq%3Dcarpet";
        isEPNPromotedListingClick = CollectionServiceUtil.isEPNPromotedListingsClick(ChannelIdEnum.EPN, getTargetUrlParameters(targetUrl), "https://www.ebay.com/itm/1234567");
        assertEquals(false, isEPNPromotedListingClick);
    }

    @Test
    public void testConstructViewItemChocolateURLForDeepLink() {
        // XC-3349, for native uri with Chocolate parameters, re-construct Chocolate url based on native uri and track (only support /itm page)
        String targetUrl = "ebay://link?nav=item.view&id=154347659933&mkevt=1&mkcid=1&mkrid=709-53481-19255-0&campid=5337369893&toolid=11800&customid=test&referrer=https%3A%2F%2Frover.ebay.com%2Frover%2F1%2F711-53200-19255-0%2F1";
        UriComponents deeplinkUriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();
        MultiValueMap<String, String> deeplinkParameters = deeplinkUriComponents.getQueryParams();

        String viewItemChocolateURL = CollectionServiceUtil.constructViewItemChocolateURLForDeepLink(deeplinkParameters);
        assertEquals("https://www.ebay.fr/itm/154347659933?mkevt=1&mkcid=1&mkrid=709-53481-19255-0&campid=5337369893&toolid=11800&customid=test&ff17=chocodeeplink", viewItemChocolateURL);

        assertEquals("", CollectionServiceUtil.constructViewItemChocolateURLForDeepLink(null));
        assertEquals("?ff17=referrerdeeplink", CollectionServiceUtil.constructReferrerChocolateURLForDeepLink(""));
    }

    @Test
    public void testIsPreinstallROI() {
        Map<String, String> roiPayloadMap = new HashMap<>();
        roiPayloadMap.put("mppid", "92");
        roiPayloadMap.put("rlutype", "1");
        roiPayloadMap.put("usecase", "prm");

        assertEquals(true, CollectionServiceUtil.isPreinstallROI(roiPayloadMap, "BIN-MobileApp"));
        assertEquals(true, CollectionServiceUtil.isPreinstallROI(roiPayloadMap, "BID-MobileApp"));
        assertEquals(true, CollectionServiceUtil.isPreinstallROI(roiPayloadMap, "Sell-MobileApp"));
        assertEquals(true, CollectionServiceUtil.isPreinstallROI(roiPayloadMap, "Reg-MobileApp"));
        assertEquals(true, CollectionServiceUtil.isPreinstallROI(roiPayloadMap, "BO-MobileApp"));
        assertEquals(true, CollectionServiceUtil.isPreinstallROI(roiPayloadMap, "RegSell-MobileApp"));
        assertEquals(false, CollectionServiceUtil.isPreinstallROI(roiPayloadMap, "BIN-FP"));

        roiPayloadMap.put("rlutype", "2");
        assertEquals(false, CollectionServiceUtil.isPreinstallROI(roiPayloadMap, "BIN-MobileApp"));

        roiPayloadMap.put("rlutype", "1");
        roiPayloadMap.put("usecase", "");
        assertEquals(false, CollectionServiceUtil.isPreinstallROI(roiPayloadMap, "BID-MobileApp"));

        roiPayloadMap.put("usecase", "prm");
        roiPayloadMap.put("mppid", "");
        assertEquals(false, CollectionServiceUtil.isPreinstallROI(roiPayloadMap, "Sell-MobileApp"));

        roiPayloadMap.clear();
        assertEquals(false, CollectionServiceUtil.isPreinstallROI(roiPayloadMap, "BIN-MobileApp"));
    }

    @Test
    public void testCreatePrmClickUrl() {
        IEndUserContext mockIEndUserContext = Mockito.mock(IEndUserContext.class);
        Mockito.when(mockIEndUserContext.getDeviceId()).thenReturn("023b4ffe1711e42a157a2480012d3864");

        Map<String, String> roiPayloadMap = new HashMap<>();
        roiPayloadMap.put("mppid", "92");
        assertEquals(CollectionServiceUtil.createPrmClickUrl(roiPayloadMap, mockIEndUserContext), "https://www.ebay.com?mkevt=1&mkcid=4&mkrid=14362-130847-18990-0&mppid=92&rlutype=1&site=0&udid=023b4ffe1711e42a157a2480012d3864");

        roiPayloadMap.put("siteId", "3");
        assertEquals(CollectionServiceUtil.createPrmClickUrl(roiPayloadMap, mockIEndUserContext), "https://www.ebay.co.uk?mkevt=1&mkcid=4&mkrid=14362-130847-18990-0&mppid=92&rlutype=1&site=3&udid=023b4ffe1711e42a157a2480012d3864");

        roiPayloadMap.put("siteId", "999");
        assertEquals(CollectionServiceUtil.createPrmClickUrl(roiPayloadMap, mockIEndUserContext), "https://www.ebay.com?mkevt=1&mkcid=4&mkrid=14362-130847-18990-0&mppid=92&rlutype=1&site=999&udid=023b4ffe1711e42a157a2480012d3864");

        assertEquals("", CollectionServiceUtil.createPrmClickUrl(roiPayloadMap, null));
    }

    @Test
    public void testSubstring() {
        assertNull(CollectionServiceUtil.substring(null, "", ""));
        assertNull(CollectionServiceUtil.substring("abcd", "efg", ""));
        assertNull(CollectionServiceUtil.substring("abcd", "d", ""));
        assertEquals("cd", CollectionServiceUtil.substring("abcd", "ab", ""));
    }


    public MultiValueMap<String, String> getTargetUrlParameters(String targetUrl) {
        UriComponents uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();
        MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
        return parameters;
    }
}
