package com.ebay.traffic.chocolate;

import com.ebay.traffic.chocolate.email.SendEmail;
import com.ebay.traffic.chocolate.util.TimeUtil;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhijin
 * <p>
 * BatchDone Alert entrence.
 */
public class BatchDoneJob {

    private static final Logger logger = LoggerFactory.getLogger(BatchDoneJob.class);

    public static void config() {
        String logDir = "../conf/log4j.properties";

        PropertyConfigurator.configure(logDir);
    }

    public static void main(String[] args) {
        config();

        String date = args[0];
        String emailHostName = args[1];
        String toEmail = args[2];
        String runPeriod = args[3];
        String time = TimeUtil.getHour(System.currentTimeMillis());

        logger.info("current time is: " + time);
        logger.info("current date is: " + date);
        logger.info("toEmail is: " + toEmail);
        logger.info("runPeriod is: " + runPeriod);

        logger.info("init SendEmail start--");
        SendEmail.getInstance().init(emailHostName, toEmail, date, time, runPeriod);
        logger.info("init SendEmail end--");

        SendEmail.getInstance().send();
    }

}
