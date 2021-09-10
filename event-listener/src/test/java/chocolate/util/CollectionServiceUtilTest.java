package chocolate.util;

import com.ebay.app.raptor.chocolate.EventListenerApplication;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.BehaviorMessageParser;
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

import javax.ws.rs.container.ContainerRequestContext;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = EventListenerApplication.class)
public class CollectionServiceUtilTest {
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

  @Test
  public void testFacebookPrefetchEnabled() {
    Map<String, String> requestHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    requestHeaders.put("X-Purpose", "preview");
    assertTrue(CollectionServiceUtil.isFacebookPrefetchEnabled(requestHeaders));
    requestHeaders.remove("X-Purpose");
    assertFalse(CollectionServiceUtil.isFacebookPrefetchEnabled(requestHeaders));
  }


  public MultiValueMap<String, String> getTargetUrlParameters(String targetUrl) {
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(targetUrl).build();
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();
    return parameters;
  }

  @Test
  public void testIsClickFromCheckoutAPI() {
    ChannelType type = ChannelType.EPN;
    IEndUserContext mockEndUserContext = Mockito.mock(IEndUserContext.class);
    when(mockEndUserContext.getUserAgent()).thenReturn("checkoutApi");
    assertTrue(CollectionServiceUtil.isClickFromCheckoutAPI(type, mockEndUserContext));

    when(mockEndUserContext.getUserAgent()).thenReturn(null);
    assertFalse(CollectionServiceUtil.isClickFromCheckoutAPI(type, mockEndUserContext));
  }

  @Test
  public void testIsROIFromCheckoutAPI() {
    Map<String, String> roiPayloadMap = new HashMap<>();
    roiPayloadMap.put("roisrc", "2");
    IEndUserContext mockEndUserContext = Mockito.mock(IEndUserContext.class);
    when(mockEndUserContext.getUserAgent()).thenReturn("checkoutApi");
    assertTrue(CollectionServiceUtil.isROIFromCheckoutAPI(roiPayloadMap, mockEndUserContext));

    roiPayloadMap.clear();
    roiPayloadMap.put("roisrc", "1");
    assertFalse(CollectionServiceUtil.isROIFromCheckoutAPI(roiPayloadMap, mockEndUserContext));

    roiPayloadMap.clear();
    roiPayloadMap.put("roisrc", "2");
    when(mockEndUserContext.getUserAgent()).thenReturn("");
    assertFalse(CollectionServiceUtil.isROIFromCheckoutAPI(roiPayloadMap, mockEndUserContext));
  }

  @Test
  public void testInPageWhitelist() {
    String targetUrl = "https://m.ebay.co.uk/?_mwBanner=1&mkevt=1&mkcid=7&ul_noapp=true";
    assertTrue(CollectionServiceUtil.inPageWhitelist(targetUrl));

    targetUrl = "https://www.ebay.co.uk/cnt/ReplyToMessages?M2MContact&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7&ul_noapp=true";
    assertTrue(CollectionServiceUtil.inPageWhitelist(targetUrl));

    targetUrl = "https://m.ebay.co.uk/seller?sid=amyonline2010&_mwBanner=1&mkevt=1&mkcid=7&ul_noapp=true";
    assertTrue(CollectionServiceUtil.inPageWhitelist(targetUrl));

    targetUrl = "https://pay.ebay.com/rxo?action=create&item=203553663494&transactionid=0&quantity=1&level=1&editAddress=0&newpurchaseok=0&refererpage=6&mkevt=1&mkcid=7&ul_noapp=true";
    assertTrue(CollectionServiceUtil.inPageWhitelist(targetUrl));

    targetUrl = "https://offer.ebay.com/ws/eBayISAPI.dll?ManageBestOffers&&boolp=1&mkevt=1&mkcid=7&ul_noapp=true&itemId=255067424578";
    assertTrue(CollectionServiceUtil.inPageWhitelist(targetUrl));

    targetUrl = "https://www.ebay.com/bo/seller/showOffers?ManageBestOffers&&boolp=1&mkevt=1&mkcid=7&ul_noapp=true&itemId=255067424578";
    assertTrue(CollectionServiceUtil.inPageWhitelist(targetUrl));

    targetUrl = "https://cart.ebay.co.uk/cart?mkevt=1&mkcid=7&ul_noapp=true";
    assertTrue(CollectionServiceUtil.inPageWhitelist(targetUrl));

    targetUrl = "https://m.ebay.de/?_mwBanner=1&ul_alt=store&ul_noapp=true";
    assertTrue(CollectionServiceUtil.inPageWhitelist(targetUrl));

    targetUrl = "https://www.ebay.com/itm/12345678";
    assertFalse(CollectionServiceUtil.inPageWhitelist(targetUrl));
  }

  @Test
  public void testIsLegacyRoverDeeplinkCase() {
    String targetUrl = "https://www.ebay.com/sch/i.html?_nkw=first+home+decor&mkevt=1&mkcid=1&mkrid=711-53200-19255-0&ff3=4&pub=5575580116&toolid=10001&campid=5338757545&customid=dec&ufes_redirect=true";
    String referer = "https://rover.ebay.com/rover/1/710-53481-19255-0/1?campid=5338757546&customid=deeplinktest&toolid=10001&mpre=https%3A%2F%2Fwww.ebay.co.uk%2Fitm%2F184967549514";
    assertFalse(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    targetUrl = "https://www.ebay.com/sch/i.html?_nkw=first+home+decor&mkevt=1&mkcid=1&mkrid=711-53200-19255-0&ff3=4&pub=5575580116&toolid=10001&campid=5338757545&customid=dec";
    referer = "https://rover.ebay.com/";
    assertTrue(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    targetUrl = "https://www.ebay.com/sch/i.html?_nkw=first+home+decor&mkevt=1&mkcid=1&mkrid=711-53200-19255-0&ff3=4&pub=5575580116&toolid=10001&campid=5338757545&customid=dec&ufes_redirect=1";
    assertTrue(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));
  }

  @Test
  public void testInRefererWhitelist() {
    String referer = "https://ebay.mtag.io/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, referer));
    assertFalse(CollectionServiceUtil.inRefererWhitelist(ChannelType.EPN, referer));

    referer = "https://ebay.pissedconsumer.com/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, referer));

    referer = "http://ebay.pissedconsumer.com/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, referer));

    referer = "https://secureir.ebaystatic.com/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, referer));

    referer = "http://secureir.ebaystatic.com/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, referer));

    referer = "http://ebay.mtag.io/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, referer));

    referer = "https://ebay.com/";
    assertFalse(CollectionServiceUtil.inRefererWhitelist(ChannelType.DISPLAY, referer));
  }
}
