package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import org.apache.flink.metrics.Meter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GdprConsentHandlerTest {
  private Map<String, Meter> gdprMetrics;

  @Before
  public void setUp() throws Exception {
    gdprMetrics = new HashMap<>();
    gdprMetrics.put("numGdprRecordsInRate", Mockito.mock(Meter.class));
    gdprMetrics.put("numGdprUrlDecodeErrorRate", Mockito.mock(Meter.class));
    gdprMetrics.put("numGdprConsentDecodeErrorRate", Mockito.mock(Meter.class));
    gdprMetrics.put("numGdprAllowContextualRate", Mockito.mock(Meter.class));
    gdprMetrics.put("numGdprAllowPersonalizedRate", Mockito.mock(Meter.class));
  }

  @Test
  public void testUrlDecodeException() {
    GdprConsentDomain gdprConsentDomain = GdprConsentHandler.handleGdprConsent("@ //:google.com", gdprMetrics);
    Mockito.verify(gdprMetrics.get("numGdprUrlDecodeErrorRate")).markEvent();
    assertTrue(gdprConsentDomain.isAllowedStoredPersonalizedData());
    assertTrue(gdprConsentDomain.isAllowedStoredContextualData());
    assertFalse(gdprConsentDomain.isTcfCompliantMode());
  }

  @Test
  public void testCanStorePersonalizedAndContextual() {
    String targetUrl = "http://www.ebayadservices.com/marketingtracking/v1/ar?mkcid=4&mpt=2136988828&gdpr=1&gdpr_consent=CPDcoOXPDcoOXAcABBENBSCsAP_AAAAAACiQHTNf_X_fb3_j-_59_9t0eY1f9_7_v-0zjheds-8Nyf_X_L8X_2M7vF36pq4KuR4Eu3LBIQdlHOHcTUmw6okVrTPsbk2Mr7NKJ7PEinMbe2dYGH9_n9XT-ZKY79_s___7v3eAAAIAAAAAAAAAAAAAACgAAIG_V3AAAAkEgkgAIAAXABQAFQAMgAcAA8ACAAEQAMIAaABqADyAIYAigBMACfAFUAVgAsABcADeAHMAPQAhABDQCIAIkAR0AlgCXAE0AKUAW4AwwBkADLgGoAaoA2QB3gD2AHxAPsA_QCAQEXARgAjQBHACUgFBAKWAU8Aq4BcwC_AGKANYAbQA3ABvADiAHoAPkAhsBDoCLwEiAJiATKAmwBOwChwFIgLFAWgAtgBcgC7wF5gMCCAG4AHAAeAB8AFoAfwBFACRAF8AM0AbQA5wB1AEDAIOAT8AoYBhADqgIfAR6AkIBKwCbQFhALoAXUAu0BeQaA6AFYALgAhgBkADLAGoANkAdgA_ACAAEFAIwAUsAp4BV4C0ALSAawA3gB1QD5AIbAQ6Ai8BIgCbAE7AKRAXIAwIMAFANkAdQBIQC6AF9CAAgANQDeAJCAXQIgNABWAC4AIYAZAAywBqADZAHYAPwAgABGAClgFPAKuAawA6oB8gENgIdAReAkQBNgCdgFIgLkAYEKgLgAUACGAEwALgAjgBlgDUAHYAPwAjABHAClgFXgLQAtIBvAEggJiATYApsBbAC5AF5gMCFABgBtADwAIKAdUBHoC-hkBUACgAQwAmABcAEcAMsAagA7IB9gH4ARgAjgBSwCrgFbAN4AmIBNgC0QFsALzAYEMAEAA1AG0APAAsQBsgDqgI9AXkOAggAIgAcAB4AFwAPgAtAByAD8ALoAZAA0AB_AEUAJEAWYAvgBlgDNAG0AOcAdQA7AB3AEAAIGAQWAg4CEAERAJtAT4BPwClgFQALaAXqAwADAgGEANYAa8A3gBxwDpAHVAPIAfIBCACHwEegJCgSsBK4CYgEygJtAUKApABSYCmAFTAKqAVsArsBZQC1AFxQLoAuoBfQDAh0FEABcAFAAVAAyABwAEAAIgAXQAwADGAGgAagA8AB9AEMARQAmABPgCqAKwAWAAuABfADEAGYAN4AcwA9ACEAENAIgAiQBHQCWAJgATQApQBYgC3gGEAYYAyABlADRAGoANkAb4A7wB7QD7AP0Af8BFgEYAI5ASkBKgCggFPAKuAWKAtAC0gFzALqAXkAvwBigDaAG4AOJAdMB1AD0AIbAQ6AiIBF4CQQEiAJUATYAnYBQ4CmgFWALFAWgAtgBcAC5AF2gLvAXmQARgAIAB-AGgAP4AkQBfADLAG0AOcAdgA8ACCgE-AKWAWIAwABhADZAG8AOqAdsBD4CPQEhAJXATEAm0BQoCkAFJgK2AXQAvIBfQDAiEDEABYAFAAMgAiABcADEAIYATAAqgBcAC-AGIAMwAbwA9ACOAFiAMIAZQA1ABvgDvgH2AfgA_wCMAEcAJSAUEAoYBTwCrwFoAWkAuYBfgDFAG0AOoAegBIICRAEqAJsAU0AsUBaIC2AFwALkAXaSgXgAIAAWABQADIAHAARQAwADEAHgARAAmABVAC4AF8AMQAZgA2gCEAENAIgAiQBHAClAFuAMIAZQA1QBsgDvAH4ARgAjgBTwCrwFoAWkAuoBigDcAHEAOoAfIBDoCLwEiAJsAWKAtgBdoC8yQBkABwAFwAQgA5ADIAJEAXIAvgBlgDUAG0AO4AgABCQCfAFQANeAbwA6oB9gErAJtAUmAsoBfRSB4AAuACgAKgAZAA4ACAAEUAMAAxgBoAGoAPIAhgCKAEwAJ4AUgAqgBYAC4AF8AMQAZgA5gCEAENAIgAiQBSgCxAFuAMIAZQA0QBqgDZAHfAPsA_QCLAEYAI4ASkAoIBQwCrgFbALmAXkA2gBuAD0AIdAReAkQBNgCdgFDgKaAVsAsUBbAC4AFyALtAXmUAaAAXAA-ACEAFoAOQAfgBWADIAG0ARwAkQBcgC-AGWANQAa4A2gBzgDqAHcAPAAgABCQCKgEiAJtAT4BPwClgFiALqAYAAwgBigDXgG8AOqAdsA8gB_wEegJiATKAm0BSACmAFTAK7AWgAugBeQC-gGBAA.f_gAAAAAAAAA&siteid=0&icep_siteid=0&ipn=admain2&adtype=3&size=970x250&pgroup=536045&mpvc=https%3A%2F%2Fadclick.g.doubleclick.net%2Fpcs%2Fclick%253Fxai%253DAKAOjsvUMTi7Pz9lELd52SOjgBIbPZlaKk6nO36162JeYsKeUDNh5A8avWVzXiHn9OvnD63UR0QV7qNCcGxB1ct7rsjsXp6Dkp2TGYxZeUnQRgStFfQoJLCjm_bTW7mE8R-95hyoIwXdw9hT5vSQhj0l3G6jQMBBIT9QijZ3Ji-EhH_9q5LWVy-62EZxEwfnztg2wY92U8izwV0Gh-phvQsI0SAOv4MUIRLwtPzsR-pibBskE5Vc1uYN_7VehWIpVwWCdrgRPZDsR1_5ekpHwMIPIRqTyzO5IjMBBEbDniwzNG-rhsoveQwezUh1%2526sai%253DAMfl-YT0_wSyGTcj9ZcVwk3QvUcYFlr-iq5j2th7f7p9hbSjXZlvwVjvtnGOyD5uewUtyPjqoN3Xxy2KHNoEwzkrWC-E8Yode9ikUWzq_nwgM07hE4KhHpQ4W45QlZOXaC8Xk6Fj6w%2526sig%253DCg0ArKJSzLdA8dQhlqNGEAE%2526urlfix%253D1%2526adurl%253D&mkevt=6&mkrvrid=3310380694482720&ff17=0&ff15=false&ff14=user&ff13=1&ff11=origin+%3DDap6.0&ff10=129221&ff1=zipcode%253Dnull%257Ccolor%253Dnull%257Cage%253D0%257Cgender%253DM%257Csite%253D0%257Ctime_out%253D10%257Chigh_score%253D0%257Cplacement%253Dnull%257Cheadline%253Dnull%257Coti%253D102&ff20=26&mksrid=536045&mkrid=711-161419-513441-9";
    GdprConsentDomain gdprConsentDomain = GdprConsentHandler.handleGdprConsent(targetUrl, gdprMetrics);
    Mockito.verify(gdprMetrics.get("numGdprRecordsInRate")).markEvent();
    Mockito.verify(gdprMetrics.get("numGdprAllowContextualRate")).markEvent();
    Mockito.verify(gdprMetrics.get("numGdprAllowPersonalizedRate")).markEvent();
    assertTrue(gdprConsentDomain.isAllowedStoredContextualData());
    assertTrue(gdprConsentDomain.isAllowedStoredPersonalizedData());
  }

  @Test
  public void testPurpose123456() {
    String targetUrl = "http://www.ebayadservices.com/marketingtracking/v1/ar?mkcid=4&mpt=1990581397&gdpr=1&gdpr_consent=CPA7qfyPA7qfyAVACADEBLCsAP_AAH_AAAYgHItd_X5Xb2FDeX59fttkeYEf1tbvK-QjCgCAM-AByVOQYLwG2mIytESgpAgCERAAoBJBIQFEDEEEREAQ4IEBAAHoAwgErIAKICLEiBEJAgIYCAsLF4AAAACQgVQZulCAml-QQ57rTUQokICQRwkIwEggAIIHIoVmWpXDUFCeXhtfOlEKIEIVpKvAOAjCgCAImAAiVMQYKwCGkASsEQAoAgCEQAgoBYBAQBABEkEREAR4IFBAADoAAAAqAAIACLEgBEBAAAQAAgLB4AAAACQAQQRAECAkBEQAZZjSUAokACQBwAIwEAAAIIAA.YAAAAAAAAAAA&siteid=77&adtype=0&size=1x1&ipn=admain2&placement=543560&mkevt=6&mkrvrid=3310381352614240&ff17=0&ff15=false&ff14=unknown&ff13=1&ff11=origin+%3DDap6.0&ff10=543560&ff1=zipcode%253Dnull%257Ccolor%253Dnull%257Cage%253D0%257Cgender%253Dnull%257Csite%253D77%257Ctime_out%253D10%257Chigh_score%253D0%257Cplacement%253Dnull%257Cheadline%253Dnull%257Coti%253D0&ff20=0&mksrid=707-161177-019421-0&mkrid=707-161177-019421-0";
    GdprConsentDomain gdprConsentDomain = GdprConsentHandler.handleGdprConsent(targetUrl, gdprMetrics);
    Mockito.verify(gdprMetrics.get("numGdprRecordsInRate")).markEvent();
    Mockito.verify(gdprMetrics.get("numGdprAllowContextualRate"), Mockito.never()).markEvent();
    Mockito.verify(gdprMetrics.get("numGdprAllowPersonalizedRate"), Mockito.never()).markEvent();
    assertFalse(gdprConsentDomain.isAllowedStoredContextualData());
    assertFalse(gdprConsentDomain.isAllowedStoredPersonalizedData());
  }

  @Test
  public void testGdprConsentDecodeException() {
    String targetUrl = "http://www.ebayadservices.com/marketingtracking/v1/ar?mkcid=4&mpt=1990581397&gdpr=1&gdpr_consent=1234&siteid=77&adtype=0&size=1x1&ipn=admain2&placement=543560&mkevt=6&mkrvrid=3310381352614240&ff17=0&ff15=false&ff14=unknown&ff13=1&ff11=origin+%3DDap6.0&ff10=543560&ff1=zipcode%253Dnull%257Ccolor%253Dnull%257Cage%253D0%257Cgender%253Dnull%257Csite%253D77%257Ctime_out%253D10%257Chigh_score%253D0%257Cplacement%253Dnull%257Cheadline%253Dnull%257Coti%253D0&ff20=0&mksrid=707-161177-019421-0&mkrid=707-161177-019421-0";
    GdprConsentDomain gdprConsentDomain = GdprConsentHandler.handleGdprConsent(targetUrl, gdprMetrics);
    Mockito.verify(gdprMetrics.get("numGdprRecordsInRate")).markEvent();
    Mockito.verify(gdprMetrics.get("numGdprConsentDecodeErrorRate")).markEvent();
    assertFalse(gdprConsentDomain.isAllowedStoredPersonalizedData());
    assertFalse(gdprConsentDomain.isAllowedStoredContextualData());
    assertTrue(gdprConsentDomain.isTcfCompliantMode());
  }
}