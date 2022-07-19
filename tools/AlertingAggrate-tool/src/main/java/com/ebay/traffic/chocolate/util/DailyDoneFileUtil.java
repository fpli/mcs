package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.DoneFile;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

public class DailyDoneFileUtil {

    private static final Logger logger = LoggerFactory.getLogger(DailyDoneFileUtil.class);

    public static DoneFile getDoneFileDetail(String tableName, String clusterName, String path) {
        DoneFile doneFile = new DoneFile();
        doneFile.setDataSource(tableName);
        doneFile.setCurrentDoneFile(getDoneFile(tableName,path));
        doneFile.setClusterName(clusterName);
        setDoneFileStatus(doneFile);
        return doneFile;
    }

    public static String getDoneFile(String pattern, String path) {
        List<String> list = getFileList(pattern, path);
        if(CollectionUtils.isEmpty(list)){
            return null;
        }
        Collections.sort(list, Collections.reverseOrder());
        return list.get(0);
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



    public static void setDoneFileStatus(DoneFile doneFile) {
        String status = "Ok";
        String currentDf = doneFile.getCurrentDoneFile();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-3);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        String date = format.format(calendar.getTime());
        if(StringUtils.isBlank(currentDf) || !currentDf.contains(date)){
            status = "Critical";
        }
        doneFile.setStatus(status);
    }

    public static ArrayList<DoneFile> getDoneFileInfos() {
        ArrayList<DoneFile> list = new ArrayList<>();
        list.add(getDoneFileDetail("appdl_installs_daily","apollo-rno", "apollo"));
        list.add(getDoneFileDetail("appdl_reinstalls_daily","apollo-rno", "apollo"));
        list.add(getDoneFileDetail("appdl_installs_daily","hercules", "hercules"));
        list.add(getDoneFileDetail("appdl_reinstalls_daily","hercules", "hercules"));
        return list;
    }

}
