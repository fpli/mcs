package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.BatchDoneConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

public class DoneFileReadUtil {
    private static final Logger logger = LoggerFactory.getLogger(DoneFileReadUtil.class);

    public static ArrayList<String> getDoneFileList(String path, String pattern) {
        try {

//            String path1="src/test/resources/1102HDFSFiles2/apollo_done_files.txt";
            ArrayList<String> list = readFiles(path);
            ArrayList<String> list1 = new ArrayList<>();
            for (String fileName : list) {
                String[] strs = fileName.split("/");
                String str = "";
                if (strs.length == 8) {
                    str = strs[7];
                }

                if (str.contains(pattern)) {
                    list1.add(str);
                    System.out.println(str);
                    logger.info(str);
                }
            }

            logger.info("getFileList end: " + pattern);

            return list1;
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

        return null;
    }

    public static ArrayList<String> getFileList(String path) {
        ArrayList<String> list = null;
        try {
            list = readFiles(path);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return list;
    }

    public static ArrayList<String> readFiles(String path) throws Exception {
        File file = new File(path);
        BufferedReader br = new BufferedReader(new FileReader(file));

        ArrayList<String> list = new ArrayList<>();
        String str = "";
        while ((str = br.readLine()) != null) {
            list.add(str);
        }

        return list;
    }

    public static HashMap<String, String> getAllJobMap(String path, ArrayList<BatchDoneConfig> configList) {
        try {
            HashMap<String, String> allJobMap = readFilesToMap(path, configList);
            logger.info("getAllJobMap end: ");

            return allJobMap;
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

        return null;
    }

    public static HashMap<String, String> readFilesToMap(String path, ArrayList<BatchDoneConfig> configList) throws Exception {
        File file = new File(path);
        BufferedReader br = new BufferedReader(new FileReader(file));
        HashMap<String, String> allJobMap = new HashMap<>();

        String allStr = "";
        String str = "";
        while ((allStr = br.readLine()) != null) {
            String[] strs = allStr.split("/");
            // get last
            str = strs[strs.length - 1];

            if (allStr.contains(Constants.APOLLO_RNO)) {
                allJobMap.put(Constants.APOLLO_RNO + Constants.COLON + str, str);
            } else if (allStr.contains(Constants.HERCULES)) {
                allJobMap.put(Constants.HERCULES + Constants.COLON + str, str);
            }
        }

        return allJobMap;
    }
}
