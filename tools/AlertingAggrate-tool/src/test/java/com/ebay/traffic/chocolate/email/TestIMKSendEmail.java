package com.ebay.traffic.chocolate.email;

import com.ebay.traffic.chocolate.pojo.IMKHourlyClickCount;
import com.ebay.traffic.chocolate.util.IMKDataSort;
import com.ebay.traffic.chocolate.util.TestCSVUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by shuangxu on 10/27/19.
 */
public class TestIMKSendEmail {
    @Test
    public void testIMKSendEmail() throws IOException{
        String testDir = TestCSVUtil.class.getResource("/").getPath() + "imkEmail/";
        System.out.println(testDir);
        String channelList = "ROI,PaidSearch,NaturalSearch,Display,SocialMedia";
        Map<String, List<IMKHourlyClickCount>> result = IMKDataSort.getHourlyClickCount(testDir, channelList.split(","));

        String emailHostName = "atom.corp.ebay.com";
        String emailAddressList = "shuangxu@ebay.com";
        IMKSendEmail.getInstance().init(emailHostName, emailAddressList);
        IMKSendEmail.getInstance().send(result, "IMK Hourly click report");
    }
}
