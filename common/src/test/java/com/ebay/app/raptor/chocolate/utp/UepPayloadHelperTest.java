/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.utp;

import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class UepPayloadHelperTest {
  private static long userId = 1234567L;

  @Test
  public void getUtpPayload() {
    UepPayloadHelper helper = UepPayloadHelper.getInstance();
    String openUrl = "https://www.ebayadservices.com/marketingtracking/v1/impression?mkevt=4&mkpid=2&emsid=0&mkcid=8&bu=44076761443&osub=0&segname=seedlist&crd=20200611000000&ch=osgood&trkId=12345&sojTags=emid%3Dbu%2Cosub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2CtrkId%3DtrkId%2Cchnl%3Dmkcid";
    Map<String, String> result = helper.getUepPayload(openUrl, null, ActionTypeEnum.OPEN, ChannelTypeEnum.MRKT_EMAIL);
    System.out.println(result);
    assertEquals("1293411342", result.get("userid"));

    // empty bu
    openUrl = "https://www.ebayadservices.com/marketingtracking/v1/impression?mkevt=4&mkpid=2&emsid=0&mkcid=8&bu=&osub=0&segname=seedlist&crd=20200611000000&ch=osgood&trkId=12345&sojTags=emid%3Dbu%2Cosub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2CtrkId%3DtrkId%2Cchnl%3Dmkcid";
    result = helper.getUepPayload(openUrl, null, ActionTypeEnum.OPEN, ChannelTypeEnum.MRKT_EMAIL);
    System.out.println(result);
    assertEquals("0", result.get("userid"));

    // no bu
    openUrl = "https://www.ebayadservices.com/marketingtracking/v1/impression?mkevt=4&mkpid=2&emsid=0&mkcid=8&osub=0&segname=seedlist&crd=20200611000000&ch=osgood&trkId=12345&sojTags=emid%3Dbu%2Cosub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2CtrkId%3DtrkId%2Cchnl%3Dmkcid";
    result = helper.getUepPayload(openUrl, null, ActionTypeEnum.OPEN, ChannelTypeEnum.MRKT_MESSAGE_CENTER);
    System.out.println(result);
    assertEquals("0", result.get("userid"));

    // click
    String clickUrl = "https://rover.ebay.com/rover/2/0/8?rvrrefts=a85975b71760acb12083bc98ffffcebe&cguid=aea0e35b1730aad6c723f41bf6b54593&osub=218233737b58356e419de66d995371af%7ETE79007_T_ALL&cmob=34534543%7E02423%7E24234&fdbk=1&crd=20201227160000&ul_ref=https%253A%252F%252Frover.ebay.com%252Frover%252F2%252F0%252F8%253Fbu%253D44119608751%2526segname%253DTE79007_T_ALL%2526crd%253D20201227160000%2526mpre%253Dhttps%25253A%25252F%25252Fwww.ebay.com.au%25252Fi%25252F174533061645%25253Fcampaign-id%25253D90001%252526run-date%25253D20201227160000%252526templateId%25253D1cb7b781-f129-4982-b289-8bd0204d9101%252526templateVersion%25253D526%252526co%25253D14502%252526placement-type%25253Dmfe.piyi1P%252526user-id%25253D44119608751%252526instance%25253D1609110000%252526site-id%25253D15%252526trackingCode%25253DTE79007_T_ALL%252526placement-type-name%25253Dmfe.piyi1P%252526mfe-Id%25253D101269%2526ch%253Dosgood%2526url%253D%252524%25257BlocationUrl%25257D%25253Fcampaign-id%25253D%252524%25257BcampaignId%25257D%252526run-date%25253D%252524%25257BRUN_DT%25257D%252526templateId%25253D%252524%25257BtemplateId%25257D%252526templateVersion%25253D%252524%25257BtemplateVersion%25257D%252526co%25253D%252524%25257BlinkOrder%25257D%252526placement-type%25253D%252524%25257BplacementType%25257D%252526user-id%25253D%252524%25257BENCRYPTED_USER_ID%25257D%252526instance%25253D%252524%25257BINSTANCE_ID%25257D%252526site-id%25253D%252524%25257BSITE_ID%25257D%252526trackingCode%25253D%252524%25257BTrackingCode%25257D%252526placement-type-name%25253D%252524%25257BplacementType%25257D%252524%25257BotherParameters%25257D%2526osub%253D218233737b58356e419de66d995371af%25257ETE79007_T_ALL%2526sojTags%253Demid%25253Dbu%25252Cut%25253Dut%25252Csegname%25253Dsegname%25252Ccrd%25253Dcrd%25252Curl%25253Durl%25252Cch%25253Dch%25252Cosub%25253Dosub%2526srcrot%253D0%2526rvr_id%253D0%2526rvr_ts%253Da85975b71760acb12083bc98ffffcebd&sojTags=emid%3Dbu%2Cut%3Dut%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Durl%2Cch%3Dch%2Cosub%3Dosub&ch=osgood&mpre=https%3A%2F%2Fwww.ebay.com.au%2Fi%2F174533061645%3Fcampaign-id%3D90001%26run-date%3D20201227160000%26templateId%3D1cb7b781-f129-4982-b289-8bd0204d9101%26templateVersion%3D526%26co%3D14502%26placement-type%3Dmfe.piyi1P%26user-id%3D44119608751%26instance%3D1609110000%26site-id%3D15%26trackingCode%3DTE79007_T_ALL%26placement-type-name%3Dmfe.piyi1P%26mfe-Id%3D101269&segname=TE79007_T_ALL&bu=44119608751";
    result = helper.getUepPayload(clickUrl, null, ActionTypeEnum.CLICK, ChannelTypeEnum.MRKT_MESSAGE_CENTER);
    System.out.println(result);
    assertEquals("1266821314", result.get("userid"));
    assertEquals("34534543%7E02423%7E24234", result.get("canvas.mob.trk.id"));

    // Pulsar user id
    clickUrl = "https://rover.ebay.com/rover/2/0/8?rvrrefts=a85975b71760acb12083bc98ffffcebe&cguid=aea0e35b1730aad6c723f41bf6b54593&osub=218233737b58356e419de66d995371af%7ETE79007_T_ALL&fdbk=1&crd=20201227160000&ul_ref=https%253A%252F%252Frover.ebay.com%252Frover%252F2%252F0%252F8%253Fbu%253D44119608751%2526segname%253DTE79007_T_ALL%2526crd%253D20201227160000%2526mpre%253Dhttps%25253A%25252F%25252Fwww.ebay.com.au%25252Fi%25252F174533061645%25253Fcampaign-id%25253D90001%252526run-date%25253D20201227160000%252526templateId%25253D1cb7b781-f129-4982-b289-8bd0204d9101%252526templateVersion%25253D526%252526co%25253D14502%252526placement-type%25253Dmfe.piyi1P%252526user-id%25253D44119608751%252526instance%25253D1609110000%252526site-id%25253D15%252526trackingCode%25253DTE79007_T_ALL%252526placement-type-name%25253Dmfe.piyi1P%252526mfe-Id%25253D101269%2526ch%253Dosgood%2526url%253D%252524%25257BlocationUrl%25257D%25253Fcampaign-id%25253D%252524%25257BcampaignId%25257D%252526run-date%25253D%252524%25257BRUN_DT%25257D%252526templateId%25253D%252524%25257BtemplateId%25257D%252526templateVersion%25253D%252524%25257BtemplateVersion%25257D%252526co%25253D%252524%25257BlinkOrder%25257D%252526placement-type%25253D%252524%25257BplacementType%25257D%252526user-id%25253D%252524%25257BENCRYPTED_USER_ID%25257D%252526instance%25253D%252524%25257BINSTANCE_ID%25257D%252526site-id%25253D%252524%25257BSITE_ID%25257D%252526trackingCode%25253D%252524%25257BTrackingCode%25257D%252526placement-type-name%25253D%252524%25257BplacementType%25257D%252524%25257BotherParameters%25257D%2526osub%253D218233737b58356e419de66d995371af%25257ETE79007_T_ALL%2526sojTags%253Demid%25253Dbu%25252Cut%25253Dut%25252Csegname%25253Dsegname%25252Ccrd%25253Dcrd%25252Curl%25253Durl%25252Cch%25253Dch%25252Cosub%25253Dosub%2526srcrot%253D0%2526rvr_id%253D0%2526rvr_ts%253Da85975b71760acb12083bc98ffffcebd&sojTags=emid%3Dbu%2Cut%3Dut%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Durl%2Cch%3Dch%2Cosub%3Dosub&ch=osgood&mpre=https%3A%2F%2Fwww.ebay.com.au%2Fi%2F174533061645%3Fcampaign-id%3D90001%26run-date%3D20201227160000%26templateId%3D1cb7b781-f129-4982-b289-8bd0204d9101%26templateVersion%3D526%26co%3D14502%26placement-type%3Dmfe.piyi1P%26user-id%3D44119608751%26instance%3D1609110000%26site-id%3D15%26trackingCode%3DTE79007_T_ALL%26placement-type-name%3Dmfe.piyi1P%26mfe-Id%3D101269&segname=TE79007_T_ALL&bu=44119608751";
    result = helper.getUepPayload(clickUrl, userId, ActionTypeEnum.CLICK, ChannelTypeEnum.MRKT_MESSAGE_CENTER);
    System.out.println(result);
    assertEquals(String.valueOf(userId), result.get("userid"));
    assertEquals("", result.get("canvas.mob.trk.id"));

    // uep click
    clickUrl = "https://www.ebay.com/i/292626925815?mkevt=1&mkpid=2&emsid=e90001.m43.l1123&mkcid=8&bu=45357454939&osub=984d1ae5f4be407f85b72c20d309f59c%257ETE80101_T_AGM&segname=TE80101_T_AGM&crd=20210218090000&fdbk=1&ch=osgood&trkId=0AB8E3DC-E3FD0D54F97-0177B1D23EB7-00000000002917F9&cnvId=700001&mesgId=3023&plmtId=700001&recoId=292626925815&recoPos=1&sojTags=osub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2CtrkId%3DtrkId%2CmesgId%3DmesgId%2CplmtId%3DplmtId%2CrecoId%3DrecoId%2CrecoPos%3DrecoPos%2Cchnl%3Dmkcid";
    result = helper.getUepPayload(clickUrl, userId, ActionTypeEnum.CLICK, ChannelTypeEnum.MRKT_MESSAGE_CENTER);
    System.out.println(result);
    assertTrue(result.get("annotation.mesg.list").contains("mesg.fdbk"));
    assertEquals("700001", result.get("annotation.cnv.id"));
  }

  @Test
  public void testLowercaseParams() {
    UepPayloadHelper helper = UepPayloadHelper.getInstance();

    // original params
    String clickUrl = "https://www.ebay.com/i/262844489390?mkevt=1&mkpid=2&emsid=0&mkcid=8&bu=45056203152&osub=6d3f46d4dac2fe3af48b0a2235752ade%257ETE80101_T_AGM&segname=TE80101_T_AGM&crd=20210315090000&ch=osgood&trkId=0AB8FD6A-247ADC67D30-0177B1BC2479-000000000588F6FA&mesgId=3001&plmtId=700001&plmtPos=1&recoId=262844489390&recoPos=1&sojTags=osub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2CtrkId%3DtrkId%2CmesgId%3DmesgId%2CplmtId%3DplmtId%2CrecoId%3DrecoId%2CrecoPos%3DrecoPos%2Cchnl%3Dmkcid";
    Map<String, String> result = helper.getUepPayload(clickUrl, userId, ActionTypeEnum.CLICK, ChannelTypeEnum.MRKT_EMAIL);
    assertTrue(result.get("annotation.mesg.list").contains("\"mesg.id\":\"3001\""));
    assertTrue(result.get("annotation.mesg.list").contains("\"plmt.id\":\"700001\""));
    assertTrue(result.get("annotation.mesg.list").contains("\"plmt.pos\":\"1\""));
    assertTrue(result.get("annotation.mesg.list").contains("\"reco.id\":\"262844489390\""));
    assertTrue(result.get("annotation.mesg.list").contains("\"reco.pos\":\"1\""));

    // lowercase params
    clickUrl = "https://www.ebay.com/i/262844489390?mkevt=1&mkpid=2&emsid=0&mkcid=8&bu=45056203152&osub=6d3f46d4dac2fe3af48b0a2235752ade%257ETE80101_T_AGM&segname=TE80101_T_AGM&crd=20210315090000&ch=osgood&trkid=0AB8FD6A-247ADC67D30-0177B1BC2479-000000000588F6FA&mesgid=3001&plmtid=700001&plmtpos=1&recoid=262844489390&recopos=1&sojTags=osub%3Dosub%2Cseg%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2CtrkId%3Dtrid%2CmesgId%3Dmid%2CplmtId%3Dpid%2CrecoId%3Drid%2CrecoPos%3Drpo%2Cchnl%3Dmkcid";
    result = helper.getUepPayload(clickUrl, userId, ActionTypeEnum.CLICK, ChannelTypeEnum.MRKT_EMAIL);
    assertTrue(result.get("annotation.mesg.list").contains("\"mesg.id\":\"3001\""));
    assertTrue(result.get("annotation.mesg.list").contains("\"plmt.id\":\"700001\""));
    assertTrue(result.get("annotation.mesg.list").contains("\"plmt.pos\":\"1\""));
    assertTrue(result.get("annotation.mesg.list").contains("\"reco.id\":\"262844489390\""));
    assertTrue(result.get("annotation.mesg.list").contains("\"reco.pos\":\"1\""));
  }

  @Test
  public void testORSFields() {
    UepPayloadHelper helper = UepPayloadHelper.getInstance();

    // PA
    String clickUrl = "https://www.ebay.co.uk/itm/184447812891?mkevt=1&mkpid=2&emsid=e112358.m43.l1120&mkcid=8&bu=44979336032&osub=-1%7E1&segname=16TE179832_T_PREPURCHASE_CT1&crd=20210912090000&ch=osgood&sojTags=osub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2Cchnl%3Dmkcid";
    Map<String, String> result = helper.getUepPayload(clickUrl, userId, ActionTypeEnum.CLICK, ChannelTypeEnum.MRKT_EMAIL);
    assertEquals("PA", result.get("annotation.message.name"));
    assertEquals("2021-09-12 09:00:00", result.get("annotation.canvas.uniq.id"));
    assertEquals("2021-09-12 09:00:00", result.get("rundate"));

    // ESPRESSO
    clickUrl = "https://www.ebay.co.uk/itm/284441400639?campaign-id=90001&run-date=20210911070500&templateId=200fb594-80a8-4101-afb1-a7ad982fde56&templateVersion=135&co=15103&placement-type=mfe.rt2&user-id=45270717340&instance=1631369100&site-id=3&trackingCode=TE75002_T_ALL&placement-type-name=mfe.rt2&mfe-Id=101270&mkevt=1&mkpid=2&emsid=0&mkcid=8&bu=45270717340&osub=250dec3d3f6a06262a10ceb0301c3341%257ETE75002_T_ALL&segname=TE75002_T_ALL&crd=20210911070500&ch=osgood&sojTags=osub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2Cchnl%3Dmkcid";
    result = helper.getUepPayload(clickUrl, userId, ActionTypeEnum.CLICK, ChannelTypeEnum.MRKT_EMAIL);
    assertEquals("ESPRESSO", result.get("annotation.message.name"));
    assertEquals("2021-09-11 07:05:00", result.get("annotation.canvas.uniq.id"));
    assertEquals("2021-09-11 07:05:00", result.get("rundate"));

    // AXO
    clickUrl = "https://www.ebay.co.uk/itm/174934236344?mkevt=1&mkpid=0&emsid=e11403.m43.l1465&mkcid=7&ch=osgood&euid=8510415afc9d420290a940897a12b550&bu=44908191432&osub=-1%7E1&crd=20210912164330&segname=11403&sojTags=ch%3Dch%2Cbu%3Dbu%2Cosub%3Dosub%2Ccrd%3Dcrd%2Csegname%3Dsegname%2Cchnl%3Dmkcid";
    result = helper.getUepPayload(clickUrl, userId, ActionTypeEnum.CLICK, ChannelTypeEnum.SITE_EMAIL);
    assertEquals("AXO", result.get("annotation.message.name"));
    assertEquals("8510415afc9d420290a940897a12b550", result.get("annotation.canvas.uniq.id"));
    assertEquals("2021-09-12 16:43:30", result.get("rundate"));

    // SAVEDSEARCH
    clickUrl = "https://www.ebay.de/itm/185028122260?mkevt=1&mkpid=0&emsid=e11021.m43.l1120&mkcid=7&ch=osgood&euid=9170b20d52e449c08245016ad772693f&bu=44218042742&ut=RU&exe=99066&ext=233698&osub=-1%7E1&crd=20210902055245&segname=11021&sojTags=ch%3Dch%2Cbu%3Dbu%2Cut%3Dut%2Cnqt%3Dnqt%2Cnqc%3Dnqc%2Cmdbreftime%3Dmdbreftime%2Ces%3Des%2Cec%3Dec%2Cexe%3Dexe%2Cext%3Dext%2Cexe%3Dexe%2Cext%3Dext%2Cosub%3Dosub%2Ccrd%3Dcrd%2Csegname%3Dsegname%2Cchnl%3Dmkcid";
    result = helper.getUepPayload(clickUrl, userId, ActionTypeEnum.CLICK, ChannelTypeEnum.SITE_EMAIL);
    assertEquals("SAVEDSEARCH", result.get("annotation.message.name"));
    assertEquals("9170b20d52e449c08245016ad772693f", result.get("annotation.canvas.uniq.id"));
    assertEquals("2021-09-02 05:52:45", result.get("rundate"));

    // SellerInitiatedOffer
    clickUrl = "https://www.ebay.com/itm/284444582808?mkevt=1&mkpid=0&emsid=e11304.m43.l44813&mkcid=7&ch=osgood&euid=818921348af341b1adafaf25c3036795&bu=44239650422&osub=-1%7E1&crd=20210912111355&segname=11304&sojTags=ch%3Dch%2Cbu%3Dbu%2Cosub%3Dosub%2Ccrd%3Dcrd%2Csegname%3Dsegname%2Cchnl%3Dmkcid";
    result = helper.getUepPayload(clickUrl, userId, ActionTypeEnum.CLICK, ChannelTypeEnum.SITE_EMAIL);
    assertEquals("SellerInitiatedOffer", result.get("annotation.message.name"));
    assertEquals("818921348af341b1adafaf25c3036795", result.get("annotation.canvas.uniq.id"));
    assertEquals("2021-09-12 11:13:55", result.get("rundate"));

    // BuyerOrderShipmentDelay
    String openUrl;
    openUrl = "http://adservice.vip.qa.ebay.com/marketingtracking/v1/impression?mkevt=4&siteId=77&mkpid=0&emsid=e11021&mkcid=7&ch=osgood&euid=7a6dd93dde7049ab924184238fbebdf2&bu=66666666666&ut=RU&exe=0&ext=0&osub=-1%7E1&crd=20221116082538&segname=11967";
    result = helper.getUepPayload(openUrl, userId, ActionTypeEnum.OPEN, ChannelTypeEnum.SITE_EMAIL);
    assertEquals("BuyerOrderShipmentDelay", result.get("annotation.message.name"));

    // SellerOrderShipmentDelay
    openUrl = "http://adservice.vip.qa.ebay.com/marketingtracking/v1/impression?mkevt=4&siteId=77&mkpid=0&emsid=e11021&mkcid=7&ch=osgood&euid=7a6dd93dde7049ab924184238fbebdf2&bu=77777777777&ut=RU&exe=0&ext=0&osub=-1%7E1&crd=20221116082538&segname=12995";
    result = helper.getUepPayload(openUrl, userId, ActionTypeEnum.OPEN, ChannelTypeEnum.SITE_EMAIL);
    assertEquals("SellerOrderShipmentDelay", result.get("annotation.message.name"));
  }
}
