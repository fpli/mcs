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

    targetUrl = "https://www.ebay.ca/pages/help/buy/return-item.html?norover=1&mkevt=1&mkrid=706-156598-900813-2&mkcid=2&keyword=latest&crlp=_&MT_ID=&geo_id=&rlsatarget=kwd-82120261275936:loc-32&adpos=&device=c&mktype=&loc=163392&poi=&abcId=&cmpgn=392111909&sitelnk=5&adgroupid=1313917436564721&network=o&matchtype=p&msclkid=129091f9b3e91c5a8289f3ed22c63a19";
    assertTrue(CollectionServiceUtil.inPageWhitelist(targetUrl));

    targetUrl = "https://www.ebay.com/itm/12345678";
    assertFalse(CollectionServiceUtil.inPageWhitelist(targetUrl));
  }

  @Test
  public void testIsLegacyRoverDeeplinkCase() {
    String targetUrl = "https://www.ebay.com/sch/i.html?_nkw=first+home+decor";
    String referer = "https://rover.ebay.com/rover/1/710-53481-19255-0/1?campid=5338757546&customid=deeplinktest&toolid=10001&mpre=https%3A%2F%2Fwww.ebay.co.uk%2Fitm%2F184967549514";
    assertFalse(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    targetUrl = "https://www.ebay.com/sch/i.html?_nkw=first+home+decor";
    referer = "https://rover.ebay.com/rover/1/710-53481-19255-0";
    assertTrue(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    referer = "https://rover.ebay.com/rover/1/710-53481-19255-0/2?campid=5338757546&customid=deeplinktest&toolid=10001&mpre=https%3A%2F%2Fwww.ebay.co.uk%2Fitm%2F184967549514";
    assertFalse(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    referer = "https://rover.ebay.com/rover/1/710-53481-19255-0/4?campid=5338757546&customid=deeplinktest&toolid=10001&mpre=https%3A%2F%2Fwww.ebay.co.uk%2Fitm%2F184967549514";
    assertFalse(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    referer = "https://rover.ebay.com/rover/1/710-53481-19255-0/16?campid=5338757546&customid=deeplinktest&toolid=10001&mpre=https%3A%2F%2Fwww.ebay.co.uk%2Fitm%2F184967549514";
    assertFalse(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    referer = "https://rover.ebay.com/rover/1/710-53481-19255-0/28?campid=5338757546&customid=deeplinktest&toolid=10001&mpre=https%3A%2F%2Fwww.ebay.co.uk%2Fitm%2F184967549514";
    assertFalse(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    referer = "https://rover.ebay.com/rover/1/710-53481-19255-0/7?campid=5338757546&customid=deeplinktest&toolid=10001&mpre=https%3A%2F%2Fwww.ebay.co.uk%2Fitm%2F184967549514";
    assertTrue(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    referer = "https://rover.ebay.com/rover/1/710-53481-19255-0/8?campid=5338757546&customid=deeplinktest&toolid=10001&mpre=https%3A%2F%2Fwww.ebay.co.uk%2Fitm%2F184967549514";
    assertTrue(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    referer = "https://rover.ebay.com/rover/1/710-53481-19255-0/26?campid=5338757546&customid=deeplinktest&toolid=10001&mpre=https%3A%2F%2Fwww.ebay.co.uk%2Fitm%2F184967549514";
    assertTrue(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    referer = "https://rover.ebay.com/rover/1/710-53481-19255-0/27?campid=5338757546&customid=deeplinktest&toolid=10001&mpre=https%3A%2F%2Fwww.ebay.co.uk%2Fitm%2F184967549514";
    assertTrue(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));

    referer = "https://www.ebay.com/sch/i.html";
    assertFalse(CollectionServiceUtil.isLegacyRoverDeeplinkCase(targetUrl, referer));
  }

  @Test
  public void testInRefererWhitelist() {
    String referer = "https://ebay.mtag.io/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "https://ebay.pissedconsumer.com/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "http://ebay.pissedconsumer.com/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "https://secureir.ebaystatic.com/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "http://secureir.ebaystatic.com/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "http://ebay.mtag.io/?abc=true";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "https://ebay.com/";
    assertFalse(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "https://ocsnext.ebay.co.uk/ocs/home?mkevt=1&mkpid=2&emsid=0&mkcid=8&bu=44210280665&segname=CH1234560_UK&crd=20220316030000&ch=osgood&sojTags=segname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2Cchnl%3Dmkcid";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "https://mesg.ebay.co.uk/";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "https://m.ebay.co.uk/messaging?FolderId=0&messageId=155046577635&curIdx=1";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "https://mesgmy.ebay.com/";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "https://pages.ebay.ca/help/buy/return-item.html?norover=1&mkevt=1&mkrid=706-156598-900813-2&mkcid=2&keyword=latest&crlp=_&MT_ID=&geo_id=&rlsatarget=kwd-82120261275936:loc-32&adpos=&device=c&mktype=&loc=163392&poi=&abcId=&cmpgn=392111909&sitelnk=5&adgroupid=1313917436564721&network=o&matchtype=p&msclkid=129091f9b3e91c5a8289f3ed22c63a19";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "http://ocsnext.ebay.co.uk/ocs/home?mkevt=1&mkpid=2&emsid=0&mkcid=8&bu=44210280665&segname=CH1234560_UK&crd=20220316030000&ch=osgood&sojTags=segname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2Cchnl%3Dmkcid";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "http://mesg.ebay.co.uk/";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "http://m.ebay.co.uk/messaging?FolderId=0&messageId=155046577635&curIdx=1";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "http://mesgmy.ebay.com/";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));

    referer = "http://pages.ebay.ca/help/buy/return-item.html?norover=1&mkevt=1&mkrid=706-156598-900813-2&mkcid=2&keyword=latest&crlp=_&MT_ID=&geo_id=&rlsatarget=kwd-82120261275936:loc-32&adpos=&device=c&mktype=&loc=163392&poi=&abcId=&cmpgn=392111909&sitelnk=5&adgroupid=1313917436564721&network=o&matchtype=p&msclkid=129091f9b3e91c5a8289f3ed22c63a19";
    assertTrue(CollectionServiceUtil.inRefererWhitelist(referer));
  }

  @Test
  public void testIsClickFromPlaceOfferAPI() {
    ChannelType type = ChannelType.EPN;
    IEndUserContext mockEndUserContext = Mockito.mock(IEndUserContext.class);
    when(mockEndUserContext.getUserAgent()).thenReturn("PlaceOfferAPI");
    assertTrue(CollectionServiceUtil.isClickFromPlaceOfferAPI(type, mockEndUserContext));

    assertFalse(CollectionServiceUtil.isClickFromPlaceOfferAPI(ChannelType.DISPLAY, mockEndUserContext));

    when(mockEndUserContext.getUserAgent()).thenReturn(null);
    assertFalse(CollectionServiceUtil.isClickFromPlaceOfferAPI(type, mockEndUserContext));
  }

  @Test
  public void testIsROIFromPlaceOfferAPI() {
    Map<String, String> roiPayloadMap = new HashMap<>();
    roiPayloadMap.put("roisrc", "6");
    IEndUserContext mockEndUserContext = Mockito.mock(IEndUserContext.class);
    when(mockEndUserContext.getUserAgent()).thenReturn("PlaceOfferAPI");
    assertTrue(CollectionServiceUtil.isROIFromPlaceOfferAPI(roiPayloadMap, mockEndUserContext));

    roiPayloadMap.clear();
    assertFalse(CollectionServiceUtil.isROIFromPlaceOfferAPI(roiPayloadMap, mockEndUserContext));

    roiPayloadMap.put("roisrc", "1");
    assertFalse(CollectionServiceUtil.isROIFromPlaceOfferAPI(roiPayloadMap, mockEndUserContext));

    roiPayloadMap.clear();
    roiPayloadMap.put("roisrc", "6");
    when(mockEndUserContext.getUserAgent()).thenReturn("");
    assertFalse(CollectionServiceUtil.isROIFromPlaceOfferAPI(roiPayloadMap, mockEndUserContext));
  }

  @Test
  public void testIsUlkDuplicateClick() {
    UserAgentParser agentParser = new UserAgentParser();
    UserAgentInfo iphoneAgentInfo = agentParser.parse("eBayiPhone/6.9.6");
    UserAgentInfo androidAgentInfo = agentParser.parse("ebayUserAgent/eBayAndroid;6.9.6;Android;10;OnePlus;" +
            "OnePlus6T;YES OPTUS;1080x2260;2.6");
    UserAgentInfo ipadAgentInfo = agentParser.parse("eBayiPad/6.9.6");
    UserAgentInfo iphoneMwebAgentInfo = agentParser.parse("Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) " +
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1");
    UserAgentInfo ipadMwebAgentInfo = agentParser.parse("Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) " +
            "AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1");
    UserAgentInfo androidMwebAgentInfo = agentParser.parse("Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) " +
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Mobile Safari/537.36");
    UserAgentInfo dwebAgentInfo = agentParser.parse("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36");

    String iphoneDeeplinkUrl = "ebay://link?nav=user.compose&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7&euid=2b5f1a613fee48aba94db71577623ff6&bu=45261687245&segname=11923&crd=20210901231250&osub=-1%7E1&ch=osgood&exe=euid&ext=39554&3=&sojTags=null%2Cbu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub&trkId=123456&trid=1234567&mkpid=0&emsid=eabc.mle&adcamp_landingpage=abc&placement-type=abcd&keyword=bcd&gclid=123";
    String ipadDeeplinkUrl = "padebay://link?nav=home&mkevt=1&mkcid=7";
    String ulkReferer = "https://www.ebay.co.uk/ulk/messages/reply?M2MContact&item=164208764236&requested=chevy.ray&qid=2412462805015&redirect=0&self=bbpexpress&mkevt=1&mkcid=7";
    String chocolateUrl = "https://www.ebay.com/itm/154613805298?mkevt=1&mkpid=0&emsid=e11000.m44.l9734&mkcid=7&ch=osgood&euid=82786bfb68184f0d8499977526dabee4&bu=45167398409&osub=-1%7E1&crd=20210923074143&segname=11000&sojTags=ch%3Dch%2Cbu%3Dbu%2Cosub%3Dosub%2Ccrd%3Dcrd%2Csegname%3Dsegname%2Cchnl%3Dmkcid";
    String referer = "android-app://com.google.android.gm/";

    assertTrue(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.MRKT_EMAIL, ulkReferer, iphoneDeeplinkUrl, iphoneAgentInfo));
    assertTrue(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SITE_EMAIL, ulkReferer, iphoneDeeplinkUrl, iphoneAgentInfo));
    assertTrue(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SITE_EMAIL, ulkReferer, ipadDeeplinkUrl, ipadAgentInfo));
    assertTrue(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.MRKT_EMAIL, ulkReferer, ipadDeeplinkUrl, ipadAgentInfo));
    assertTrue(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.DISPLAY, ulkReferer, iphoneDeeplinkUrl, iphoneAgentInfo));
    assertTrue(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.EPN, ulkReferer, iphoneDeeplinkUrl, iphoneAgentInfo));
    assertTrue(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SOCIAL_MEDIA, ulkReferer, ipadDeeplinkUrl, ipadAgentInfo));
    assertFalse(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SITE_EMAIL, referer, iphoneDeeplinkUrl, iphoneAgentInfo));
    assertFalse(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SITE_EMAIL, "", ipadDeeplinkUrl, ipadAgentInfo));
    assertFalse(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SITE_EMAIL, ulkReferer, chocolateUrl, iphoneAgentInfo));
    assertFalse(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SITE_EMAIL, ulkReferer, chocolateUrl, ipadAgentInfo));
    assertFalse(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SITE_EMAIL, ulkReferer, iphoneDeeplinkUrl, androidAgentInfo));
    assertFalse(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SITE_EMAIL, ulkReferer, iphoneDeeplinkUrl, iphoneMwebAgentInfo));
    assertFalse(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SITE_EMAIL, ulkReferer, iphoneDeeplinkUrl, ipadMwebAgentInfo));
    assertFalse(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SITE_EMAIL, ulkReferer, ipadDeeplinkUrl, androidMwebAgentInfo));
    assertFalse(CollectionServiceUtil.isUlkDuplicateClick(ChannelType.SITE_EMAIL, ulkReferer, ipadDeeplinkUrl, dwebAgentInfo));
  }

  @Test
  public void testInAdobePageWhitelist() {
    UserAgentParser agentParser = new UserAgentParser();
    UserAgentInfo iphoneAgentInfo = agentParser.parse("eBayiPhone/6.9.6");
    UserAgentInfo androidAgentInfo = agentParser.parse("ebayUserAgent/eBayAndroid;6.9.6;Android;10;OnePlus;" +
            "OnePlus6T;YES OPTUS;1080x2260;2.6");
    UserAgentInfo ipadAgentInfo = agentParser.parse("eBayiPad/6.9.6");
    UserAgentInfo iphoneMwebAgentInfo = agentParser.parse("Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) " +
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1");
    UserAgentInfo ipadMwebAgentInfo = agentParser.parse("Mozilla/5.0 (iPad; CPU OS 11_0 like Mac OS X) " +
            "AppleWebKit/604.1.34 (KHTML, like Gecko) Version/11.0 Mobile/15A5341f Safari/604.1");
    UserAgentInfo androidMwebAgentInfo = agentParser.parse("Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) " +
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Mobile Safari/537.36");
    UserAgentInfo dwebAgentInfo = agentParser.parse("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36");


    String targetUrl = "https://m.ebay.com/itm/234014894667?_mwBanner=1&mkevt=1&mkcid=8&mkpid=14&ufes_redirect=test&ul_noapp=true";
    String referer = "https://www.ebay.com/";
    assertTrue(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, iphoneMwebAgentInfo));
    assertTrue(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, ipadMwebAgentInfo));
    assertTrue(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, androidMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, iphoneAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, androidAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, ipadAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, dwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.SITE_EMAIL, referer, targetUrl, iphoneMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.DISPLAY, referer, targetUrl, iphoneMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.PAID_SEARCH, referer, targetUrl, iphoneMwebAgentInfo));

    targetUrl = "https://m.ebay.de/?_mwBanner=1&mkevt=1&mkcid=8&bu=123456789&mkpid=14&ul_noapp=true";
    referer = "https://www.ebay.com/";
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, iphoneMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, ipadMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, androidMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, iphoneAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, androidAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, ipadAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, dwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.SITE_EMAIL, referer, targetUrl, iphoneMwebAgentInfo));

    targetUrl = "https://m.ebay.com/itm/234014894667?_mwBanner=1&mkevt=1&mkcid=8&mkpid=1&ufes_redirect=test&ul_noapp=true";
    referer = "https://www.ebay.com/";
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, iphoneMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, ipadMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, androidMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, iphoneAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, androidAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, ipadAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, dwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.SITE_EMAIL, referer, targetUrl, iphoneMwebAgentInfo));

    targetUrl = "https://m.ebay.com/itm/234014894667?_mwBanner=1&mkevt=1&mkcid=8&mkpid=14&ufes_redirect=test&ul_noapp=true";
    referer = "https://www.ebay.com";
    assertTrue(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, iphoneMwebAgentInfo));
    assertTrue(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, ipadMwebAgentInfo));
    assertTrue(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, androidMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, iphoneAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, androidAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, ipadAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, dwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.SITE_EMAIL, referer, targetUrl, iphoneMwebAgentInfo));

    targetUrl = "https://m.ebay.com/itm/234014894667?_mwBanner=1&mkevt=1&mkcid=8&mkpid=14&ufes_redirect=test&ul_noapp=true";
    referer = "https://www.ebay.com/itm/272818202748?mkevt=1&mkpid=2&emsid=0&mkcid=8&bu=44923272962";
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, iphoneMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, ipadMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, androidMwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, iphoneAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, androidAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, ipadAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, dwebAgentInfo));
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.SITE_EMAIL, referer, targetUrl, iphoneMwebAgentInfo));

    targetUrl = "https://m.ebay.com/itm/234014894667";
    referer = "https://www.ebay.com/";
    assertFalse(CollectionServiceUtil.inAdobePageWhitelist(ChannelType.MRKT_EMAIL, referer, targetUrl, iphoneMwebAgentInfo));
  }
}
