package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.traffic.chocolate.flink.nrt.app.UtpMonitorApp;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DuplicateParameterMonitorTest {

    @Test
    public void testNoDuplicate(){
        String url="http://adservice2.vip.qa.lvs.ebay.com/marketingtracking/v1/ar?siteId=0&mkcid=2600242";
        assert UtpMonitorApp.getDuplicateValue(url,"mkcid").equals("DEFAULT");
    }

    @Test
    public void testNoTargetParameter(){
        String url="http://adservice2.vip.qa.lvs.ebay.com/marketingtracking/v1/ar?siteId=0&mkcid=2600242";
        assert UtpMonitorApp.getDuplicateValue(url,"123").equals("NULL");
    }

    @Test
    public void testAllEmptyParameter(){
        String url="http://adservice2.vip.qa.lvs.ebay.com/marketingtracking/v1/ar?siteId=0&mkcid=&mkcid=";
        assert UtpMonitorApp.getDuplicateValue(url,"mkcid").equals("EMPTY");
    }

    @Test
    public void testPartEmptyParameter(){
        String url="http://adservice2.vip.qa.lvs.ebay.com/marketingtracking/v1/ar?siteId=0&mkcid=123&mkcid=456&mkcid=";
        assert UtpMonitorApp.getDuplicateValue(url,"mkcid").equals("123+456+EMPTY");
    }

    @Test
    public void testSameDuplicateParameter(){
        String url="http://adservice2.vip.qa.lvs.ebay.com/marketingtracking/v1/ar?siteId=0&mkcid=123&mkcid=123";
        assert UtpMonitorApp.getDuplicateValue(url,"mkcid").equals("DEFAULT");
    }

    @Test
    public void testPayload() {
        Map<String, String> payload = new HashMap<>();
        payload.put("!uxe","100949");
        payload.put("!uxt","237608");
    
        assert  UtpMonitorApp.getUxe(payload).equals("100949");
        assert  UtpMonitorApp.getUxt(payload).equals("237608");
    }

}