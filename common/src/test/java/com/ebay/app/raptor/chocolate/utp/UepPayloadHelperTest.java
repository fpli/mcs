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

  @Test
  public void getUtpPayload() {
    UepPayloadHelper helper = new UepPayloadHelper();
    String openUrl = "https://www.ebayadservices.com/marketingtracking/v1/impression?mkevt=4&mkpid=2&emsid=0&mkcid=8&bu=44076761443&osub=0&segname=seedlist&crd=20200611000000&ch=osgood&trkId=12345&sojTags=emid%3Dbu%2Cosub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2CtrkId%3DtrkId%2Cchnl%3Dmkcid";
    Map<String, String> result = helper.getUepPayload(openUrl, ActionTypeEnum.OPEN, ChannelTypeEnum.MRKT_EMAIL);
    System.out.println(result);
    assertEquals("1293411342", result.get("userid"));

    // empty bu
    openUrl = "https://www.ebayadservices.com/marketingtracking/v1/impression?mkevt=4&mkpid=2&emsid=0&mkcid=8&bu=&osub=0&segname=seedlist&crd=20200611000000&ch=osgood&trkId=12345&sojTags=emid%3Dbu%2Cosub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2CtrkId%3DtrkId%2Cchnl%3Dmkcid";
    result = helper.getUepPayload(openUrl, ActionTypeEnum.OPEN, ChannelTypeEnum.MRKT_EMAIL);
    System.out.println(result);
    assertEquals("0", result.get("userid"));

    // no bu
    openUrl = "https://www.ebayadservices.com/marketingtracking/v1/impression?mkevt=4&mkpid=2&emsid=0&mkcid=8&osub=0&segname=seedlist&crd=20200611000000&ch=osgood&trkId=12345&sojTags=emid%3Dbu%2Cosub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2CtrkId%3DtrkId%2Cchnl%3Dmkcid";
    result = helper.getUepPayload(openUrl, ActionTypeEnum.OPEN, ChannelTypeEnum.MRKT_MESSAGE_CENTER);
    System.out.println(result);
    assertEquals("0", result.get("userid"));

    // click
    String clickUrl = "https://rover.ebay.com/rover/2/0/8?rvrrefts=a85975b71760acb12083bc98ffffcebe&cguid=aea0e35b1730aad6c723f41bf6b54593&osub=218233737b58356e419de66d995371af%7ETE79007_T_ALL&fdbk=1&crd=20201227160000&ul_ref=https%253A%252F%252Frover.ebay.com%252Frover%252F2%252F0%252F8%253Fbu%253D44119608751%2526segname%253DTE79007_T_ALL%2526crd%253D20201227160000%2526mpre%253Dhttps%25253A%25252F%25252Fwww.ebay.com.au%25252Fi%25252F174533061645%25253Fcampaign-id%25253D90001%252526run-date%25253D20201227160000%252526templateId%25253D1cb7b781-f129-4982-b289-8bd0204d9101%252526templateVersion%25253D526%252526co%25253D14502%252526placement-type%25253Dmfe.piyi1P%252526user-id%25253D44119608751%252526instance%25253D1609110000%252526site-id%25253D15%252526trackingCode%25253DTE79007_T_ALL%252526placement-type-name%25253Dmfe.piyi1P%252526mfe-Id%25253D101269%2526ch%253Dosgood%2526url%253D%252524%25257BlocationUrl%25257D%25253Fcampaign-id%25253D%252524%25257BcampaignId%25257D%252526run-date%25253D%252524%25257BRUN_DT%25257D%252526templateId%25253D%252524%25257BtemplateId%25257D%252526templateVersion%25253D%252524%25257BtemplateVersion%25257D%252526co%25253D%252524%25257BlinkOrder%25257D%252526placement-type%25253D%252524%25257BplacementType%25257D%252526user-id%25253D%252524%25257BENCRYPTED_USER_ID%25257D%252526instance%25253D%252524%25257BINSTANCE_ID%25257D%252526site-id%25253D%252524%25257BSITE_ID%25257D%252526trackingCode%25253D%252524%25257BTrackingCode%25257D%252526placement-type-name%25253D%252524%25257BplacementType%25257D%252524%25257BotherParameters%25257D%2526osub%253D218233737b58356e419de66d995371af%25257ETE79007_T_ALL%2526sojTags%253Demid%25253Dbu%25252Cut%25253Dut%25252Csegname%25253Dsegname%25252Ccrd%25253Dcrd%25252Curl%25253Durl%25252Cch%25253Dch%25252Cosub%25253Dosub%2526srcrot%253D0%2526rvr_id%253D0%2526rvr_ts%253Da85975b71760acb12083bc98ffffcebd&sojTags=emid%3Dbu%2Cut%3Dut%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Curl%3Durl%2Cch%3Dch%2Cosub%3Dosub&ch=osgood&mpre=https%3A%2F%2Fwww.ebay.com.au%2Fi%2F174533061645%3Fcampaign-id%3D90001%26run-date%3D20201227160000%26templateId%3D1cb7b781-f129-4982-b289-8bd0204d9101%26templateVersion%3D526%26co%3D14502%26placement-type%3Dmfe.piyi1P%26user-id%3D44119608751%26instance%3D1609110000%26site-id%3D15%26trackingCode%3DTE79007_T_ALL%26placement-type-name%3Dmfe.piyi1P%26mfe-Id%3D101269&segname=TE79007_T_ALL&bu=44119608751";
    result = helper.getUepPayload(clickUrl, ActionTypeEnum.CLICK, ChannelTypeEnum.MRKT_MESSAGE_CENTER);
    System.out.println(result);
    assertEquals("1266821314", result.get("userid"));

    // uep click
    clickUrl = "https://www.ebay.com/i/292626925815?mkevt=1&mkpid=2&emsid=e90001.m43.l1123&mkcid=8&bu=45357454939&osub=984d1ae5f4be407f85b72c20d309f59c%257ETE80101_T_AGM&segname=TE80101_T_AGM&crd=20210218090000&fdbk=1&ch=osgood&trkId=0AB8E3DC-E3FD0D54F97-0177B1D23EB7-00000000002917F9&mesgId=3023&plmtId=700001&recoId=292626925815&recoPos=1&sojTags=osub%3Dosub%2Csegname%3Dsegname%2Ccrd%3Dcrd%2Cch%3Dch%2CtrkId%3DtrkId%2CmesgId%3DmesgId%2CplmtId%3DplmtId%2CrecoId%3DrecoId%2CrecoPos%3DrecoPos%2Cchnl%3Dmkcid";
    result = helper.getUepPayload(clickUrl, ActionTypeEnum.CLICK, ChannelTypeEnum.MRKT_MESSAGE_CENTER);
    System.out.println(result);
    assertTrue(result.get("annotation.mesg.list").contains("mesg.fdbk"));
  }
}