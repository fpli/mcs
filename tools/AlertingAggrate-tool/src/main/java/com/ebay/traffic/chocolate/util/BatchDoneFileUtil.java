package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.BatchDoneConfig;
import com.ebay.traffic.chocolate.pojo.BatchDoneFile;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

public class BatchDoneFileUtil {

    private static final Logger logger = LoggerFactory.getLogger(BatchDoneFileUtil.class);

    public static LinkedHashMap<String, ArrayList<BatchDoneFile>> getDoneFileInfos() {
        //00 getBatchDoneConfig-List<Project>
        ArrayList<BatchDoneConfig> configList = getBatchDoneConfigList();

        //01 getDoneFile-information====Map<“Cluster:donefile”,“donefile”>
//        HashMap<String, String> allJobMap = DoneFileReadUtil.getAllJobMap("src/main/resources/allBatchDone.txt", configList);
        HashMap<String, String> allJobMap = DoneFileReadUtil.getAllJobMap(Constants.ALL_DONE_FILES, configList);

        //02 <projectName,ArrayList<BatchDoneFile>>
        LinkedHashMap<String, ArrayList<BatchDoneFile>> htmlMap = getHtmlpMap(configList, allJobMap);

        return htmlMap;
    }

    /**
     * getHtmpMap
     *
     * @param configList
     * @param allJobMap
     * @return
     */
    private static LinkedHashMap<String, ArrayList<BatchDoneFile>> getHtmlpMap(ArrayList<BatchDoneConfig> configList, HashMap<String, String> allJobMap) {
        ArrayList<BatchDoneFile> batchDoneFilesList = new ArrayList<>();
        LinkedHashMap<String, ArrayList<BatchDoneFile>> htmlMap = new LinkedHashMap<>();

        for (BatchDoneConfig bdc : configList) {
            String htmlProjectName = bdc.getProjectName();
            String doneName = bdc.getDoneName();
            String doneTemplate = bdc.getDoneTemplate();
            String dateFormat = bdc.getDateFormat();
            String doneTime = bdc.getDoneTime();
            String jobLink = bdc.getJobLink();
            String support = bdc.getSupport();
            String cluster = bdc.getCluster();
            String calType = bdc.getCalType();
            String threshold = bdc.getThreshold();

            // 03 judge delay according to the recent time
            Calendar ch = Calendar.getInstance();
            Calendar cd = Calendar.getInstance();
            Calendar cc = Calendar.getInstance();
            SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
            String key;
            String value = Constants.NONE;

            int delay = 0;
            String status = Constants.OK;


            BatchDoneFile bdf = new BatchDoneFile();
            if (calType.contains(Constants.H_2)) {
                // Done file is last hour,so -1
                // 48h
                int j;
                for (j = 0; j < 48; j++) {
                    ch.set(Calendar.HOUR_OF_DAY, ch.get(Calendar.HOUR_OF_DAY) - 1);
                    String currentTime = sdf.format(ch.getTime());
                    key = cluster + ":" + doneTemplate.replace("$", currentTime);
                    value = allJobMap.get(key);
                    if (value != null) {
                        break;
                    }
                }
                if (value == null) {
                    // Done file is last hour is running,so -1
                    delay = cc.get(Calendar.HOUR_OF_DAY) + 24 - 1;
                    status = Constants.CRITICAL;
                    value = Constants.NONE;
                } else {
                    // Done file is last hour is running,so -1
                    delay = j - 1;
                    if (delay == 0 || delay == -1) {
                        status = Constants.OK;
                        delay = 0;
                    } else if (delay > 0 && delay <= Integer.parseInt(threshold)) {
                        status = Constants.WARNING;
                    } else if (delay > Integer.parseInt(threshold)) {
                        status = Constants.CRITICAL;
                    }
                }

            } else if (calType.contains(Constants.T_1)) {
                // Done file is yesterday,so -1
                // 2day
                int j;
                for (j = 0; j < 2; j++) {
                    cd.set(Calendar.DATE, cd.get(Calendar.DATE) - 1);
                    String currentTime = sdf.format(cd.getTime());
                    key = cluster + ":" + doneTemplate.replace("$", currentTime);
                    value = allJobMap.get(key);
                    if (value != null) {
                        break;
                    }
                }
                int targetHour = Integer.parseInt(doneTime);
                int currentHour = cd.get(Calendar.HOUR_OF_DAY);
                if (value == null) {
                    if (currentHour - targetHour <= 0) {
                        delay = 0;
                        status = Constants.OK;
                    } else {
                        delay = 24 - targetHour + currentHour;
                        status = Constants.CRITICAL;
                    }

                    value = Constants.NONE;
                } else {
                    if (j == 0) {
                        delay = 0;
                        status = Constants.OK;
                    } else if (j > 0) {
                        delay = currentHour - targetHour;
                        if (delay > 0 && delay <= Integer.parseInt(threshold)) {
                            status = Constants.WARNING;
                        } else if (delay > Integer.parseInt(threshold)) {
                            status = Constants.CRITICAL;
                        } else {
                            delay = 0;
                            status = Constants.OK;
                        }
                    }

                }
            }
            if (!htmlMap.containsKey(htmlProjectName)) {
                batchDoneFilesList = new ArrayList<>();
            }
            bdf.setCurrentDoneFile(value);
            bdf.setDoneName(doneName);
            bdf.setStatus(status);
            bdf.setDelay(delay);
            bdf.setCluster(cluster);
            bdf.setSupport(support);
            bdf.setJobLink(jobLink);

            batchDoneFilesList.add(bdf);
            htmlMap.put(htmlProjectName, batchDoneFilesList);
        }
        return htmlMap;
    }

    /**
     * getBatchDoneConfigList
     *
     * @return ArrayList<BatchDoneConfig>
     */
    private static ArrayList<BatchDoneConfig> getBatchDoneConfigList() {
        SAXReader reader = new SAXReader();
        Document document = null;
        try {
//            document = reader.read(new File("src/test/resources/batch-done.xml"));
            document = reader.read(new File(Constants.BATCH_DONE));
        } catch (DocumentException e) {
        }
        Element projects = document.getRootElement();
        Iterator it = projects.elementIterator();

        ArrayList<BatchDoneConfig> configList = new ArrayList<>();


        String projectName;
        while (it.hasNext()) {
            Element project = (Element) it.next();
            projectName = project.attribute("project_name").getValue();
            List<Element> ls = project.elements("df");
            for (Element elem : ls) {
                BatchDoneConfig bdc = new BatchDoneConfig();
                bdc.setProjectName(projectName);
                bdc.setDoneName(elem.attribute("done_name").getValue());
                bdc.setDoneTemplate(elem.attribute("done_template").getValue());
                bdc.setDateFormat(elem.attribute("date_format").getValue());
                bdc.setDoneTime(elem.attribute("done_time").getValue());
                bdc.setJobLink(elem.attribute("job_link").getValue());
                bdc.setSupport(elem.attribute("support").getValue());
                bdc.setCluster(elem.attribute("cluster").getValue());
                bdc.setCalType(elem.attribute("cal_type").getValue());
                bdc.setThreshold(elem.attribute("threshold").getValue());
                configList.add(bdc);
            }
        }
        return configList;
    }


}
