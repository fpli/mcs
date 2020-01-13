package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.IMKHourlyClickCount;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by shuangxu on 10/25/19.
 */
public class TestIMKDataSort {

    @Test
    public void testgetHourlyClickCountList() throws IOException{
        String testDir = TestCSVUtil.class.getResource("/").getPath() + "imkEmail/channel_name=SocialMedia/hourlyClick.csv";
        System.out.println(testDir);
        List<IMKHourlyClickCount> result = IMKDataSort.getHourlyClickCountList(testDir);
        assert(result.size() == 3);
        for(IMKHourlyClickCount item: result){
            System.out.println("channel: " + item.getChannel_id() +
                " event_dt: " + item.getEvent_dt() +
                " click_hour: " + item.getClick_hour() +
                " click_count: " + item.getClick_count());
        }
    }

    @Test
    public void testgetHourlyClickCount() throws IOException{
        String testDir = TestCSVUtil.class.getResource("/").getPath() + "imkEmail/";
        System.out.println(testDir);
        String channelList = "ROI,PaidSearch,NaturalSearch,Display,SocialMedia";
        Map<String, List<IMKHourlyClickCount>> result = IMKDataSort.getHourlyClickCount(testDir, channelList.split(","));
        assert(result.keySet().contains("ROI"));
        assert(result.keySet().contains("SocialMedia"));
        assert(result.size() == 2);

        for(String channel: result.keySet()){
            List<IMKHourlyClickCount> hourlyCountList = result.get(channel);
            for(IMKHourlyClickCount item: hourlyCountList){
                System.out.println("channel: " + item.getChannel_id() +
                        " event_dt: " + item.getEvent_dt() +
                        " click_hour: " + item.getClick_hour() +
                        " click_count: " + item.getClick_count());
            }
        }
    }
}
