package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.parse.EPNReportUtil;
import com.ebay.traffic.chocolate.report.AirflowReport;
import com.ebay.traffic.chocolate.report.FlowerReport;
import com.ebay.traffic.chocolate.report.Uc4Report;
import com.ebay.traffic.chocolate.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HourlyEmailHtml {

    private static final Logger logger = LoggerFactory.getLogger(HourlyEmailHtml.class);

    public static String getESAlertHtml(String runPeriod) {
        try {
            return Table.parseESAlertProjects(ESAlertUtil.getESAlertInfos(runPeriod), null);
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "getESAlertHtml";
        }
    }

    public static String getDoneFileHtml() {
        try {
            return "Done File Monitor\n" + DoneFileTable.parseDoneFileProject(DoneFileUtil.getDoneFileInfos());
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "getDoneFileHtml";
        }
    }
    public static String getBatchDoneFileHtml() {
        try {
            return "<style>a{text-decoration: none;}</style>"+BatchDoneFileTable.parseBatchDoneFileProject(BatchDoneFileUtil.getDoneFileInfos());
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "getBatchDoneFileHtml";
        }
    }


    public static String getRotationAlertHtml() {
        try {
            return "Rotation Data Monitor\n" + RotationAlertTable.parseRotationAlertProject(RotationAlertUtil.getRotationAlertInfos());
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "getRotationAlertHtml";
        }
    }

    public static String getEPNHourlyReportHtml() {
        try {
            return EPNReportUtil.getHourlyReport();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "getEPNHourlyReportHtml";
        }
    }

    public static String getAzkabanReportHtml() {
        try {
            return AzkabanUtil.getAzkabanReportHtml();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "getAzkabanReportHtml";
        }
    }

    public static String getIMKHourlyCountHtml() {
        try {
            return IMKHourlyCountUtil.getIMKHourlyCountHtml();
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "getIMKHourlyCountHtml";
        }
    }

    public static String getHourlyEPNClusterFileVerifyHtml() {
        try {
            return "EPN Hdfs File Number Monitor \n" + HourlyEPNClusterFileVerifyTable.parseHourlyEPNClusterFileVerifyProject(HourlyEPNClusterFileVerifyUtil.getHourlyEPNClusterFileVerifyInfos());
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "getHourlyEPNClusterFileVerifyHtml";
        }
    }

    public static String getAirflowReportHtml(String cluster) {
        try {
            return AirflowTable.parse(AirflowReport.getAirflowList(cluster));
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "getAirflowReportHtml";
        }
    }

    public static String getWorkerReportHtml(String cluster) {
        try {
            return "Airflow worker Monitor \n" + FlowerTable.parse(FlowerReport.getAirflowWorkerList(cluster));
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "getAirflowReportHtml";
        }
    }

    public static String getSherlockAlertHtml(String runPeriod) {
        try {
            return SherlockMetricTable.parseSherlockAlertProjects(SherlockAlertUtil.getSherlockAlertInfos(runPeriod));
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            return "getSherlockAlertHtml";
        }
    }

    public static String getUc4ReportHtml() {
        try {
            return Uc4Table.parse(Uc4Report.getUc4JobPlanList());
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "getUc4ReportHtml";
        }
    }
}
