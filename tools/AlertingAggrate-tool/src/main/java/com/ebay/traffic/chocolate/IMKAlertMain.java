package com.ebay.traffic.chocolate;

import com.ebay.traffic.chocolate.email.IMKSendEmail;
import com.ebay.traffic.chocolate.pojo.IMKHourlyClickCount;
import com.ebay.traffic.chocolate.util.IMKDataSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by shuangxu on 10/24/19.
 */
public class IMKAlertMain {

    private static final Logger logger = LoggerFactory.getLogger(IMKAlertMain.class);

    public static void main(String[] args) throws IOException{
        String emailHostName = args[0];
        String filePath = args[1];
        String toEmail = args[2];
        String[] channelList = args[3].split(",");
        String title = args[4];


        IMKSendEmail.getInstance().init(emailHostName, toEmail);

        Map<String, List<IMKHourlyClickCount>> hourlyClickCount = IMKDataSort.getHourlyClickCount(filePath, channelList);

        IMKSendEmail.getInstance().send(hourlyClickCount, title);
    }
}
