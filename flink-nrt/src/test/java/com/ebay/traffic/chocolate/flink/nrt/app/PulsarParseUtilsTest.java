package com.ebay.traffic.chocolate.flink.nrt.app;

import com.ebay.traffic.chocolate.flink.nrt.util.PulsarParseUtils;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class PulsarParseUtilsTest {

  @Test
  public void parseChannelType() {
    assertNull(PulsarParseUtils.getChannelIdFromUrlQueryString(""));
    String urlquerystring = "/roveropen/0/e12060/7?osub=-1%7E1rd%3Dcrd%2Cosub%3Dosub&ch=osgood&segname=12060" +
            "&bu=43886848848&euid=942d35b23ee140b69989083c45abb869";
    assertEquals("7", PulsarParseUtils.getChannelIdFromUrlQueryString(urlquerystring));


    urlquerystring = "/rover/1/724-53478-19255-0/1?rvrrefts=142044b41780a77d1a81e84fff85096c&cguid=142044ad1780a77d1a878f96ef5ea153&toolid=10029&dashenCnt=0&item=373439153220&ul_ref=https%253A%252F%252Frover.ebay.com%252Frover%252F1%252F724-53478-19255-0%252F1%253Ftoolid%253D10029%2526campid%253D5338525748%2526catId%253D220%2526type%253D2%2526ext%253D373439153220%2526item%253D373439153220%2526mpre%253Dhttps%253A%252F%252Fwww.ebay.it%252Fi%252F373439153220%2526srcrot%253D724-53478-19255-0%2526rvr_id%253D2841229668845%2526rvr_ts%253D142044b41780a77d1a81e84fff85096b&catId=220&campid=5338525748&type=2&mpre=https%3A%2F%2Fwww.ebay.it%2Fi%2F373439153220&dashenId=6774830084319678464&ext=373439153220";
    assertEquals("1", PulsarParseUtils.getChannelIdFromUrlQueryString(urlquerystring));
  }

  @Test
  public void getSojTagsFromUrl() {
    Map<String, String> map;

    map = PulsarParseUtils.getSojTagsFromUrlQueryString("/rover/0/e11050.m44.l1139/7?" +
            "rvrrefts=0886eb271760ad4fea202772ffe1e445&cguid=b7f4a84c1750aaecfb237947caea9754&osub=-1%7E1");
    assertTrue(map.isEmpty());

    map = PulsarParseUtils.getSojTagsFromUrlQueryString("/rover/0/e11050.m44.l1139/7?" +
            "rvrrefts=0886eb271760ad4fea202772ffe1e445&sojTags=");
    assertTrue(map.isEmpty());

    map = PulsarParseUtils.getSojTagsFromUrlQueryString("/rover/0/e11050.m44.l1139/7?" +
            "rvrrefts=0886eb271760ad4fea202772ffe1e445&cguid=b7f4a84c1750aaecfb237947caea9754&osub=-1%7E1" +
            "&crd=20201126123420&sojTags=bu%3Dbu%2Cch%3Dch%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Dloc%2Cosub%3Dosub" +
            "&ch=osgood&mpre=https%3A%2F%2Fcontact.ebay.fr%2Fws%2FeBayISAPI.dll%3FM2MContact%26item%3D383750816168%26qid" +
            "%3D2923852554019%26requested%3Dsihemch%26redirect%3D0&segname=11050&bu=43296320304" +
            "&euid=cd550bf56ed541cd89ed09d9f4cdafda");
    assertEquals("43296320304", map.get("bu"));
    assertEquals("osgood", map.get("ch"));
    assertEquals("11050", map.get("segname"));
    assertEquals("20201126123420", map.get("crd"));
    assertNull(map.get("url"));
    assertEquals("-1%7E1", map.get("osub"));
  }
}