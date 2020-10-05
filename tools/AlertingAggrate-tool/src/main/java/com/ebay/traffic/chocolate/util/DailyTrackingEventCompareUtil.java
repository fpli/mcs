package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.DailyTrackingEventCompare;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class DailyTrackingEventCompareUtil {
    private static final Logger logger = LoggerFactory.getLogger(DailyTrackingEventCompareUtil.class);

    public static ArrayList<DailyTrackingEventCompare> getTrackingEvent(){
        String path = "/home/_choco_admin/trackingEvent";
        ArrayList<DailyTrackingEventCompare> list = new ArrayList<>();
        HashMap<String,String> herculesMap = getTrackingEventMap(path,"herculeslvs");
        HashMap<String,String> apolloMap = getTrackingEventMap(path,"apollorno");
        Set<String> sortDateSet = new TreeSet<>(Comparator.reverseOrder());
        sortDateSet.addAll(herculesMap.keySet());
        sortDateSet.addAll(apolloMap.keySet());
        for(String date: sortDateSet){
            DailyTrackingEventCompare dtec = new DailyTrackingEventCompare();
            dtec.setTableName("tracking_event");
            dtec.setDate(date);
            dtec.setApolloCount(apolloMap.get(date));
            dtec.setHerculesCount(herculesMap.get(date));
            dtec.setStatus(getStatus(dtec.getHerculesCount(),dtec.getApolloCount()));
            list.add(dtec);
        }
        return list;
    }

    private static String getStatus(String herculesCount,String apolloCount) {
        if(herculesCount == null || apolloCount == null){
            return "Warning";
        }
        if (herculesCount.equalsIgnoreCase(apolloCount)) {
            return "Ok";
        } else {
            return "Warning";
        }
    }

    private static HashMap<String, String> getTrackingEventMap(String path, String clusterName) {
        String dirName = path  + "/" + clusterName;
        return FileReadUtil.getTrackingEventMap(dirName);
    }

    public static ArrayList<DailyTrackingEventCompare> getDailyTrackingEventCompares(){
        return getTrackingEvent();
    }
}
