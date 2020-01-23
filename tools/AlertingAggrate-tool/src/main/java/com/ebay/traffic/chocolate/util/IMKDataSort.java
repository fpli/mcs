package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.IMKHourlyClickCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.ArrayList;


/**
 * Created by shuangxu on 10/24/19.
 */
public class IMKDataSort {

    private static final Logger logger = LoggerFactory.getLogger(IMKDataSort.class);

    public static Map<String, List<IMKHourlyClickCount>> getHourlyClickCount(String inputPath, String[] channelList){
        Map<String, List<IMKHourlyClickCount>> hourlyClickCountMap = new HashMap<String, List<IMKHourlyClickCount>>();
        if(channelList.length > 0){
            for(String channel: channelList){
                String filePath = inputPath + "channel_name=" + channel + "/hourlyClick.csv";
                List<IMKHourlyClickCount> clickList = getHourlyClickCountList(filePath);
                if(clickList != null && clickList.size() > 0)
                    hourlyClickCountMap.put(channel, clickList);
            }

            Comparator<IMKHourlyClickCount> by_count_dt = Comparator.comparing(IMKHourlyClickCount::getEvent_dt).reversed();
            Comparator<IMKHourlyClickCount> by_click_hour = Comparator.comparing(IMKHourlyClickCount::getClick_hour).reversed();
            Comparator<IMKHourlyClickCount> unionComparator = by_count_dt.thenComparing(by_click_hour);

            for(String channel: hourlyClickCountMap.keySet()){
                List<IMKHourlyClickCount> result = hourlyClickCountMap.get(channel).stream().sorted(unionComparator).collect(Collectors.toList());
                if (result.size() <= 10) {
                    hourlyClickCountMap.put(channel, result);
                } else {
                    List<IMKHourlyClickCount> res = new ArrayList<>();
                    int count = 0;
                    for (IMKHourlyClickCount hcc : result) {
                        count++;
                        if (count > 10) {
                            break;
                        } else
                            res.add(hcc);
                    }
                    hourlyClickCountMap.put(channel, res);
                }
            }
        }
        return hourlyClickCountMap;
    }

    public static List<IMKHourlyClickCount> getHourlyClickCountList(String hourlyClickCountFile){
        List<IMKHourlyClickCount> hourlyClickCountList = new ArrayList();
        try{
            hourlyClickCountList = CSVUtil.mapToBean(hourlyClickCountFile, IMKHourlyClickCount.class, ',');
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            return hourlyClickCountList;
        }
    }
}
