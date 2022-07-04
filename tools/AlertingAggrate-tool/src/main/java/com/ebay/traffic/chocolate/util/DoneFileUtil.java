package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.DoneFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

public class DoneFileUtil {

    private static final Logger logger = LoggerFactory.getLogger(DoneFileUtil.class);

    public static DoneFile getDoneFileDetail(String tableName, String clusterName, String path) {
        DoneFile doneFile = getDalayDelayInfo(tableName, path);
        doneFile.setClusterName(clusterName);
        return doneFile;
    }

    public static ArrayList<String> getParams(String pattern, String path) {
        List<String> list = getFileList(pattern, path);
        Collections.sort(list, Collections.reverseOrder());

        ArrayList<String> retList = new ArrayList<>();
        int delay = 0;

        if (list == null || list.size() == 0) {
            int h = LocalDateTime.now().getHour();
            delay = -1;
        }

        String donefile = "";
        String max_time = "";
        if (list.size() >= 1) {
            donefile = list.get(0);
            String[] arr = donefile.split("\\.");
            if (arr.length == 3) {
                donefile = arr[2];
            }
            logger.info("log: donefile ----> " + donefile);
            System.out.println("console: donefile ----> " + donefile);
            if (donefile.length() > 10) {
                max_time = donefile.substring(0, 10);
                delay = getDelay(max_time);
            }
            logger.info("log: max_time ----> " + max_time);
            System.out.println("console: max_time ----> " + max_time);
        }

        retList.add(new Integer(delay).toString());
        retList.add(list.get(0));

        return retList;
    }

    private static List<String> getFileList(String pattern, String cluster) {
        if ("apollo".equalsIgnoreCase(cluster)) {
            return DoneFileReadUtil.getDoneFileList(Constants.APOLLO_DONE_FILES, pattern);
        } else if ("hercules".equalsIgnoreCase(cluster)) {
            return DoneFileReadUtil.getDoneFileList(Constants.HERCULES_DONE_FILES, pattern);
        } else {
            return null;
        }
    }

    public static int getDelay(String max_time) {
        int year = Integer.parseInt(max_time.substring(0, 4));
        int month = Integer.parseInt(max_time.substring(4, 6));
        int day = Integer.parseInt(max_time.substring(6, 8));
        int hour = Integer.parseInt(max_time.substring(8, 10));

        Calendar c = Calendar.getInstance();
        c.set(year, month - 1, day, hour, 0);
        long delay = 0;
        try {
            long t = c.getTimeInMillis();
            long current = System.currentTimeMillis();
            delay = (current - t) / 3600000L;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return (int) delay;
    }

    public static DoneFile getDalayDelayInfo(String pattern, String path) {
        ArrayList<String> list = getParams(pattern, path);

        int delay_hour = Integer.parseInt(list.get(0));
        String donefile = list.get(1);

        DoneFile doneFile = new DoneFile();
        int delay = 0;
        String status = "Ok";

        if (delay_hour >= 3) {
            delay = delay_hour - 2;
        }

        int waring_delay_max_value = 12;
        if (path.contains("hercules")) {
            waring_delay_max_value = 5;
        }

        if (delay > 0 && delay <= waring_delay_max_value) {
            status = "Warning";
        } else if (delay > waring_delay_max_value) {
            status = "Critical";
        }

        doneFile.setDataSource(pattern);
        doneFile.setStatus(status);
        doneFile.setDelay(delay);
        doneFile.setCurrentDoneFile(donefile);

        return doneFile;
    }

    public static ArrayList<DoneFile> getDoneFileInfos() {
        ArrayList<DoneFile> list = new ArrayList<>();
        list.add(getDoneFileDetail("imk_rvr_trckng_event_hourly","apollo-rno", "apollo"));
        list.add(getDoneFileDetail("ams_click_hourly","apollo-rno", "apollo"));
        list.add(getDoneFileDetail("ams_imprsn_hourly","apollo-rno", "apollo"));
        list.add(getDoneFileDetail("utp_event_hourly","apollo-rno", "apollo"));
        list.add(getDoneFileDetail("imk_rvr_trckng_event_hourly","hercules", "hercules"));
        list.add(getDoneFileDetail("ams_click_hourly","hercules", "hercules"));
        list.add(getDoneFileDetail("ams_imprsn_hourly","hercules", "hercules"));
        list.add(getDoneFileDetail("utp_event_hourly","hercules", "hercules"));
        return list;
    }

}
