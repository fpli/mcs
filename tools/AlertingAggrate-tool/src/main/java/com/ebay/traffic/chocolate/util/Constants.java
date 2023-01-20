package com.ebay.traffic.chocolate.util;

public class Constants {
    public static String TOOLS_BASE_DIR = "/datashare/mkttracking/tools/AlertingAggrate-tool-imk-v3/";
    public static String PREFIX_TMP_DIR = TOOLS_BASE_DIR + "temp/";
    public static String APOLLO_DONE_FILES = PREFIX_TMP_DIR + "apollo_files/apollo_done_files.txt";
    public static String HERCULES_DONE_FILES = PREFIX_TMP_DIR + "hercules_files/hercules_done_files.txt";
    public static String DAILY_CLICK_TREND_FILE = PREFIX_TMP_DIR + "daily_click_trend/dailyClickTrend.csv";
    public static String DAILY_DOMAIN_TREND_FILE = PREFIX_TMP_DIR + "daily_domain_trend/dailyDomainTrend.csv";
    public static String HOURLY_CLICK_COUNT_FILE = PREFIX_TMP_DIR + "hourly_click_count/hourlyClickCount.csv";
    public static String HOURLY_MONITOR_EPN = PREFIX_TMP_DIR + "hourly_monitor_epn/";
    public static String IMK_HOURLY_COUNT = PREFIX_TMP_DIR + "/imk_hourly_count/";
    public static String TD_DIR = PREFIX_TMP_DIR + "td/";
    public static String PREFIX_CONF_DIR = TOOLS_BASE_DIR + "conf/";
    public static String AZKABAN_HOURLY_XML = PREFIX_CONF_DIR + "azkaban-hourly.xml";
    public static String METRIC_DAILY_XML = PREFIX_CONF_DIR + "metric-daily.xml";
    public static String METRIC_HOURLY_XML = PREFIX_CONF_DIR + "metric-hourly.xml";
    public static String AIRFLOW_HOURLY_XML = PREFIX_CONF_DIR + "airflow-hourly.xml";

    public static String TRACKING_EVENT_DIR = TOOLS_BASE_DIR + "/data_check/trackingEvent";
    public static String ROTATION_DIR = "/home/_choco_admin/rotation/";
    public static String Uc4_DIR = PREFIX_CONF_DIR + "uc4-hourly.xml";
    public static String LOG4J_DIR = TOOLS_BASE_DIR + "log/1.txt";
    public static String BATCH_DONE = PREFIX_CONF_DIR + "batch-done.xml";
    public static String ALL_DONE_FILES = PREFIX_TMP_DIR + "all_done_files/" + "all_done_files.txt";

    // Airflow 27
    public static String AIRFLOW_GET_DAG_API_URL_27 = "http://airflowprod-web.mrkttech-tracking-ns.svc.27.tess.io:8080/api/v1/dags";
    public static String FLOWER_DASHBOARD_URL_27 = "http://flower.mrkttech-tracking-ns.svc.27.tess.io:5555/dashboard?json=1";

    // Airflow 79
    public static String AIRFLOW_GET_DAG_API_URL_79 = "http://airflowprod-web.marketing-tracking-airflow-prod.svc.79.tess.io:8080/api/v1/dags";
    public static String FLOWER_DASHBOARD_URL_79 = "http://flower.marketing-tracking-airflow-prod.svc.79.tess.io:5555/dashboard?json=1";

    // Airflow 94
    public static String AIRFLOW_GET_DAG_API_URL_94 = "http://airflowprod-web.marketing-tracking-airflow-prod.svc.94.tess.io:8080/api/v1/dags";
    public static String FLOWER_DASHBOARD_URL_94 = "http://flower.marketing-tracking-airflow-prod.svc.94.tess.io:5555/dashboard?json=1";

    // Airflow 127
    public static String AIRFLOW_GET_DAG_API_URL_127 = "http://airflowprod-web.marketing-tracking-airflow-prod.svc.127.tess.io:8080/api/v1/dags";
    public static String FLOWER_DASHBOARD_URL_127 = "http://flower.marketing-tracking-airflow-prod.svc.127.tess.io:5555/dashboard?json=1";

    // sherlock
    public static String SHERLOCK_METRIC_END_POINT = "https://metrics-egress.sherlock.io";
    public static String SHERLOCK_METRIC_DAILY_XML = PREFIX_CONF_DIR + "sherlock-metric-daily.xml";
    public static String SHERLOCK_METRIC_HOURLY_XML = PREFIX_CONF_DIR + "sherlock-metric-hourly.xml";
    public static String OS_IDENTITY_END_POINT = "https://os-identity.vip.ebayc3.com/v2.0/tokens";
    public static String AUTH_USER_NAME = "_PaaS_Provisioning";
    public static String AUTH_PASSWD = "Cloud2018#E2Eebay@123";
    public static String AUTH_PASSWD_BASE64 = "Q2xvdWQyMDE4I0UyRWViYXlAMTIz";

    // BatchDone
    public static String APOLLO_RNO = "apollo-rno";
    public static String HERCULES = "hercules";
    public static String COLON = ":";
    public static String NONE = "None";
    public static String OK = "Ok";
    public static String H_2 = "H-2";
    public static String T_1 = "T-1";
    public static String CRITICAL = "Critical";
    public static String WARNING = "Warning";
    public static String SUPPORT_DOC = "Oncall doc";


}
