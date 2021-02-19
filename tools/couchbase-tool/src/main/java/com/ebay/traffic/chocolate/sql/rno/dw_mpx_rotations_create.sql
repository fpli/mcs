set mapreduce.job.queuename=hdlq-commrce-default;
set hive.optimize.index.filter=false;
set mapreduce.job.split.metainfo.maxsize=-1;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.vectorized.execution.reduce.groupby.enabled = true;

set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
SET hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;

SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;
SET hive.auto.convert.join=false;


create external table choco_data.dw_mpx_rotations (
    `rotation_id` decimal(18,0),
    `placement_id` decimal(9,0),
    `campaign_id` decimal(18,0),
    `vendor_id` int,
    `portl_bkt_id` int,
    `portl_sub_bkt_id` int,
    `portl_prtnr_grp_id` smallint,
    `rotation_string` string,
    `rotation_name` string,
    `rotation_cost` decimal(18,2),
    `rotation_count` decimal(9,0),
    `rotation_count_type` string,
    `rotation_date_start` string,
    `rotation_date_end` string,
    `portl_dshbrd_flag_id` smallint,
    `portl_invc_flag_id` smallint,
    `portl_src_cre_dt` string,
    `rotation_ct_url_name` string,
    `rotation_sts_name` string,
    `rotation_desc_txt` string,
    `mpx_pixel_dmnsn_size_txt` string,
    `mpx_orgnzng_txt` string,
    `trfc_order_stndrd_txt` string,
    `trfc_order_java_scrpt_txt` string,
    `trfc_order_link_txt` string,
    `trfc_order_trcr_txt` string,
    `rot_pt_id` decimal(18,0),
    `mpx_chnl_id` smallint,
    `portl_app_id` int,
    `geo_client_id` int,
    `rotation_brand_ind` int,
    `cre_date` string,
    `cre_user` string,
    `upd_date` timestamp,
    `upd_user` string
)
STORED AS PARQUET
LOCATION 'viewfs://apollo-rno/apps/b_marketing_tracking/rotation/dw_mpx_rotations_parquet';



create table choco_data.dw_mpx_rotations_temp (
  ROTATION_ID DECIMAL(18,0),
  PLACEMENT_ID DECIMAL(9,0),
  CAMPAIGN_ID DECIMAL(18,0),
  VENDOR_ID INT,
  PORTL_BKT_ID INT,
  PORTL_SUB_BKT_ID INT,
  PORTL_PRTNR_GRP_ID SMALLINT,
  ROTATION_STRING VARCHAR(200),
  ROTATION_NAME VARCHAR(2000),
  ROTATION_COST DECIMAL(18,2),
  ROTATION_COUNT DECIMAL(9,0),
  ROTATION_COUNT_TYPE CHAR(1),
  ROTATION_DATE_START STRING, -- DATE FORMAT 'YYYY-MM-DD'
  ROTATION_DATE_END STRING, --  DATE FORMAT 'YYYY-MM-DD'
  PORTL_DSHBRD_FLAG_ID SMALLINT,
  PORTL_INVC_FLAG_ID SMALLINT,
  PORTL_SRC_CRE_DT STRING, -- DATE FORMAT 'YYYYMMDD'
  ROTATION_CT_URL_NAME VARCHAR(4000),
  ROTATION_STS_NAME VARCHAR(10),
  ROTATION_DESC_TXT VARCHAR(4000),
  MPX_PIXEL_DMNSN_SIZE_TXT VARCHAR(32),
  MPX_ORGNZNG_TXT VARCHAR(255),
  TRFC_ORDER_STNDRD_TXT VARCHAR(4000),
  TRFC_ORDER_JAVA_SCRPT_TXT VARCHAR(4000),
  TRFC_ORDER_LINK_TXT VARCHAR(4000),
  TRFC_ORDER_TRCR_TXT VARCHAR(4000),
  ROT_PT_ID DECIMAL(18,0),
  MPX_CHNL_ID SMALLINT,
  PORTL_APP_ID INT,
  GEO_CLIENT_ID INT,
  ROTATION_BRAND_IND INT,
  CRE_DATE STRING,
  CRE_USER VARCHAR(30),
  UPD_DATE TIMESTAMP,
  UPD_USER VARCHAR(30)
)STORED AS TEXTFILE;


create table choco_data.V_GBH_FROM_NAME (
  ROTATION_ID DECIMAL(18,0),
  GBH_TMP varchar(50),
  GBH_TMP1 varchar(50),
  NEW_GEO_CLIENT_ID int
)STORED AS TEXTFILE;



create table choco_data.dw_mpx_rotations_temp_one (
  ROTATION_ID DECIMAL(18,0),
  PLACEMENT_ID DECIMAL(9,0),
  CAMPAIGN_ID DECIMAL(18,0),
  VENDOR_ID INT,
  PORTL_BKT_ID INT,
  PORTL_SUB_BKT_ID INT,
  PORTL_PRTNR_GRP_ID SMALLINT,
  ROTATION_STRING VARCHAR(200),
  ROTATION_NAME VARCHAR(2000),
  ROTATION_COST DECIMAL(18,2),
  ROTATION_COUNT DECIMAL(9,0),
  ROTATION_COUNT_TYPE CHAR(1),
  ROTATION_DATE_START STRING, -- DATE FORMAT 'YYYY-MM-DD'
  ROTATION_DATE_END STRING, --  DATE FORMAT 'YYYY-MM-DD'
  PORTL_DSHBRD_FLAG_ID SMALLINT,
  PORTL_INVC_FLAG_ID SMALLINT,
  PORTL_SRC_CRE_DT STRING, -- DATE FORMAT 'YYYYMMDD'
  ROTATION_CT_URL_NAME VARCHAR(4000),
  ROTATION_STS_NAME VARCHAR(10),
  ROTATION_DESC_TXT VARCHAR(4000),
  MPX_PIXEL_DMNSN_SIZE_TXT VARCHAR(32),
  MPX_ORGNZNG_TXT VARCHAR(255),
  TRFC_ORDER_STNDRD_TXT VARCHAR(4000),
  TRFC_ORDER_JAVA_SCRPT_TXT VARCHAR(4000),
  TRFC_ORDER_LINK_TXT VARCHAR(4000),
  TRFC_ORDER_TRCR_TXT VARCHAR(4000),
  ROT_PT_ID DECIMAL(18,0),
  MPX_CHNL_ID SMALLINT,
  PORTL_APP_ID INT,
  GEO_CLIENT_ID INT,
  ROTATION_BRAND_IND INT,
  CRE_DATE STRING,
  CRE_USER VARCHAR(30),
  UPD_DATE TIMESTAMP,
  UPD_USER VARCHAR(30)
)STORED AS TEXTFILE;


create table choco_data.dw_mpx_rotations_temp_two (
  ROTATION_ID DECIMAL(18,0),
  PLACEMENT_ID DECIMAL(9,0),
  CAMPAIGN_ID DECIMAL(18,0),
  VENDOR_ID INT,
  PORTL_BKT_ID INT,
  PORTL_SUB_BKT_ID INT,
  PORTL_PRTNR_GRP_ID SMALLINT,
  ROTATION_STRING VARCHAR(200),
  ROTATION_NAME VARCHAR(2000),
  ROTATION_COST DECIMAL(18,2),
  ROTATION_COUNT DECIMAL(9,0),
  ROTATION_COUNT_TYPE CHAR(1),
  ROTATION_DATE_START STRING, -- DATE FORMAT 'YYYY-MM-DD'
  ROTATION_DATE_END STRING, --  DATE FORMAT 'YYYY-MM-DD'
  PORTL_DSHBRD_FLAG_ID SMALLINT,
  PORTL_INVC_FLAG_ID SMALLINT,
  PORTL_SRC_CRE_DT STRING, -- DATE FORMAT 'YYYYMMDD'
  ROTATION_CT_URL_NAME VARCHAR(4000),
  ROTATION_STS_NAME VARCHAR(10),
  ROTATION_DESC_TXT VARCHAR(4000),
  MPX_PIXEL_DMNSN_SIZE_TXT VARCHAR(32),
  MPX_ORGNZNG_TXT VARCHAR(255),
  TRFC_ORDER_STNDRD_TXT VARCHAR(4000),
  TRFC_ORDER_JAVA_SCRPT_TXT VARCHAR(4000),
  TRFC_ORDER_LINK_TXT VARCHAR(4000),
  TRFC_ORDER_TRCR_TXT VARCHAR(4000),
  ROT_PT_ID DECIMAL(18,0),
  MPX_CHNL_ID SMALLINT,
  PORTL_APP_ID INT,
  GEO_CLIENT_ID INT,
  ROTATION_BRAND_IND INT,
  CRE_DATE STRING,
  CRE_USER VARCHAR(30),
  UPD_DATE TIMESTAMP,
  UPD_USER VARCHAR(30)
)STORED AS TEXTFILE;


create external table choco_data.dw_mpx_rotations_ups (
  ROTATION_ID DECIMAL(18,0),
  ROTATION_STRING VARCHAR(200),
  ROTATION_NAME VARCHAR(2000),
  MPX_PIXEL_DMNSN_SIZE_TXT VARCHAR(32),
  MPX_CHNL_ID SMALLINT,
  ROTATION_CT_URL_NAME VARCHAR(4000),
  ROTATION_STS_NAME VARCHAR(10),
  ROTATION_COST DECIMAL(18,2),
  ROTATION_COUNT DECIMAL(9,0),
  ROTATION_COUNT_TYPE CHAR(1),
  ROTATION_DATE_START STRING, -- DATE FORMAT 'YYYY-MM-DD'
  ROTATION_DATE_END STRING, --  DATE FORMAT 'YYYY-MM-DD'
  ROTATION_DESC_TXT VARCHAR(4000),
  MPX_ORGNZNG_TXT VARCHAR(255),
  TRFC_ORDER_STNDRD_TXT VARCHAR(4000),
  TRFC_ORDER_JAVA_SCRPT_TXT VARCHAR(4000),
  TRFC_ORDER_LINK_TXT VARCHAR(4000),
  TRFC_ORDER_TRCR_TXT VARCHAR(4000),
  VENDOR_ID INT,
  VENDOR_NAME VARCHAR(200),
  VENDOR_URL VARCHAR(4000),
  VENDOR_TYPE VARCHAR(200),
  CLIENT_ID INT,
  CAMPAIGN_ID DECIMAL(18,0),
  CLIENT_NAME VARCHAR(200),
  CAMPAIGN_NAME VARCHAR(1000),
  PLACEMENT_ID DECIMAL(9,0),
  PERF_TRACK_1 VARCHAR(100),
  PERF_TRACK_2 VARCHAR(100),
  PERF_TRACK_3 VARCHAR(100),
  PERF_TRACK_4 VARCHAR(100),
  PERF_TRACK_5 VARCHAR(100),
  PERF_TRACK_6 VARCHAR(100),
  PERF_TRACK_7 VARCHAR(100),
  PERF_TRACK_8 VARCHAR(100),
  PERF_TRACK_9 VARCHAR(100),
  PERF_TRACK_10 VARCHAR(100),
  CRE_DATE STRING,
  CRE_USER VARCHAR(30),
  UPD_USER VARCHAR(30)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
NULL DEFINED AS ""
STORED AS TEXTFILE
LOCATION 'viewfs://apollo-rno/user/b_marketing_tracking/rotation/rno_daily/new_update/rotations';