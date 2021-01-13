package com.ebay.traffic.chocolate.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

public class DoneFileReadUtil {
    private static final Logger logger = LoggerFactory.getLogger(DoneFileReadUtil.class);

    public static ArrayList<String> getDoneFileList(String path, String pattern) {
        try {

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
}
