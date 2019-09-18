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


msck repair table choco_data.dw_mpx_rotations_ups;


insert overwrite table choco_data.dw_mpx_rotations_temp
select o.rotation_id,
       o.placement_id,
       o.campaign_id,
       o.vendor_id,
       o.PORTL_BKT_ID,
       o.PORTL_SUB_BKT_ID,
       o.PORTL_PRTNR_GRP_ID,
       o.rotation_string,
       o.rotation_name,
       o.rotation_cost,
       o.rotation_count,
       o.rotation_count_type,
       o.rotation_date_start,
       o.rotation_date_end,
       o.PORTL_DSHBRD_FLAG_ID,
       o.PORTL_INVC_FLAG_ID,
       o.PORTL_SRC_CRE_DT STRING,
       o.ROTATION_CT_URL_NAME,
       o.ROTATION_STS_NAME,
       o.ROTATION_DESC_TXT,
       o.MPX_PIXEL_DMNSN_SIZE_TXT,
       o.MPX_ORGNZNG_TXT,
       o.TRFC_ORDER_STNDRD_TXT,
       o.TRFC_ORDER_JAVA_SCRPT_TXT,
       o.TRFC_ORDER_LINK_TXT,
       o.TRFC_ORDER_TRCR_TXT,
       o.ROT_PT_ID,
       o.MPX_CHNL_ID,
       o.PORTL_APP_ID,
       o.GEO_CLIENT_ID,
       o.ROTATION_BRAND_IND,
       o.CRE_DATE,
       o.CRE_USER,
       o.UPD_DATE,
       o.UPD_USER
from choco_data.dw_mpx_rotations o
left outer join choco_data.dw_mpx_rotations_ups u
on o.rotation_id = u.rotation_id
where u.rotation_id is null;


insert into table choco_data.dw_mpx_rotations_temp
(
         rotation_id        ,
         rotation_string    ,
         rotation_name      ,
         rotation_cost      ,
         rotation_count     ,
         rotation_count_type,
         rotation_date_start,
         rotation_date_end  ,
         placement_id       ,
         campaign_id        ,
         vendor_id    ,
         rotation_ct_url_name,
         rotation_sts_name,
         rotation_desc_txt,
         mpx_pixel_dmnsn_size_txt,
         mpx_orgnzng_txt,
         trfc_order_stndrd_txt,
         trfc_order_java_scrpt_txt,
         trfc_order_link_txt,
         trfc_order_trcr_txt,
         rot_pt_id,
         mpx_chnl_id,
         rotation_brand_ind,
         cre_date,
         cre_user,
         upd_date,
         upd_user
)
select ori.rotation_id,
       nvl(ori.rotation_string, 'Undefined'),
       nvl(ori.rotation_name, 'Undefined'),
       nvl(ori.rotation_cost, 0),
       nvl(ori.rotation_count, 0),
       nvl(ori.rotation_count_type, 'U'),
       nvl(ori.rotation_date_start, '1900-01-01'),
       nvl(ori.rotation_date_end, '1900-01-01'),
       nvl(ori.placement_id, -999),
       nvl(ori.campaign_id, -999),
       nvl(ori.vendor_id, -999),
       ori.ROTATION_CT_URL_NAME,
       ori.ROTATION_STS_NAME,
       ori.ROTATION_DESC_TXT,
       ori.MPX_PIXEL_DMNSN_SIZE_TXT,
       ori.MPX_ORGNZNG_TXT,
       ori.TRFC_ORDER_STNDRD_TXT,
       ori.TRFC_ORDER_JAVA_SCRPT_TXT,
       ori.TRFC_ORDER_LINK_TXT,
       ori.TRFC_ORDER_TRCR_TXT,
       nvl(lkp.ROT_PT_ID, -999),
       ori.MPX_CHNL_ID,
       ori.ROTATION_BRAND_IND,
       ori.cre_date,
       ori.cre_user,
       current_timestamp(),
       ori.upd_user
from (
select ro.rotation_id,
       ro.rotation_string,
       ro.rotation_name,
       ro.rotation_cost,
       ro.rotation_count,
       ro.rotation_count_type,
       ro.rotation_date_start,
       ro.rotation_date_end,
       ro.placement_id,
       ro.campaign_id,
       ro.vendor_id,
       ro.ROTATION_CT_URL_NAME,
       ro.ROTATION_STS_NAME,
       ro.ROTATION_DESC_TXT,
       ro.MPX_PIXEL_DMNSN_SIZE_TXT,
       ro.MPX_ORGNZNG_TXT,
       ro.TRFC_ORDER_STNDRD_TXT,
       ro.TRFC_ORDER_JAVA_SCRPT_TXT,
       ro.TRFC_ORDER_LINK_TXT,
       ro.TRFC_ORDER_TRCR_TXT,
       ro.MPX_CHNL_ID,
       rot.ROTATION_BRAND_IND,
       rot.cre_date,
       rot.cre_user,
       ro.upd_user,
       ro.perf_track_1,
       ro.perf_track_2,
       ro.perf_track_3,
       ro.perf_track_4,
       ro.perf_track_5,
       ro.perf_track_6,
       ro.perf_track_7,
       ro.perf_track_8,
       ro.perf_track_9,
       ro.perf_track_10
from choco_data.dw_mpx_rotations_ups ro
left outer join choco_data.dw_mpx_rotations rot
on ro.rotation_id = rot.rotation_id
where rot.rotation_id is not null
) ori
left outer join choco_data.dw_mpx_rotations_pt_lkp lkp
on ori.perf_track_1 = lkp.perf_track_1
   and ori.perf_track_2 = lkp.perf_track_2
   and ori.perf_track_3 = lkp.perf_track_3
   and ori.perf_track_4 = lkp.perf_track_4
   and ori.perf_track_5 = lkp.perf_track_5
   and ori.perf_track_6 = lkp.perf_track_6
   and ori.perf_track_7 = lkp.perf_track_7
   and ori.perf_track_8 = lkp.perf_track_8
   and ori.perf_track_9 = lkp.perf_track_9
   and ori.perf_track_10 = lkp.perf_track_10;


insert into table choco_data.dw_mpx_rotations_temp
(
         rotation_id        ,
         rotation_string    ,
         rotation_name      ,
         rotation_cost      ,
         rotation_count     ,
         rotation_count_type,
         rotation_date_start,
         rotation_date_end  ,
         placement_id       ,
         campaign_id        ,
         vendor_id    ,
         rotation_ct_url_name,
         rotation_sts_name,
         rotation_desc_txt,
         mpx_pixel_dmnsn_size_txt,
         mpx_orgnzng_txt,
         trfc_order_stndrd_txt,
         trfc_order_java_scrpt_txt,
         trfc_order_link_txt,
         trfc_order_trcr_txt,
         rot_pt_id,
         mpx_chnl_id,
         rotation_brand_ind,
         cre_date,
         cre_user,
         upd_date,
         upd_user
)
select ro.rotation_id,
       nvl(ro.rotation_string, 'Undefined') rotation_string,
       nvl(ro.rotation_name, 'Undefined') rotation_name,
       nvl(ro.rotation_cost, 0) rotation_cost,
       nvl(ro.rotation_count, 0) rotation_count,
       nvl(ro.rotation_count_type, 'U') rotation_count_type,
       nvl(ro.rotation_date_start, '1900-01-01') rotation_date_start,
       nvl(ro.rotation_date_end, '1900-01-01') rotation_date_end,
       nvl(ro.placement_id, -999) placement_id,
       nvl(ro.campaign_id, -999) campaign_id,
       nvl(ro.vendor_id, -999) vendor_id,
       ro.ROTATION_CT_URL_NAME,
       ro.ROTATION_STS_NAME,
       ro.ROTATION_DESC_TXT,
       ro.MPX_PIXEL_DMNSN_SIZE_TXT,
       ro.MPX_ORGNZNG_TXT,
       ro.TRFC_ORDER_STNDRD_TXT,
       ro.TRFC_ORDER_JAVA_SCRPT_TXT,
       ro.TRFC_ORDER_LINK_TXT,
       ro.TRFC_ORDER_TRCR_TXT,
       nvl(lkp.ROT_PT_ID, -999),
       ro.MPX_CHNL_ID,
       0,
       ro.CRE_DATE,
       ro.CRE_USER,
       current_timestamp(),
       ro.UPD_USER
from choco_data.dw_mpx_rotations_ups ro
left outer join choco_data.dw_mpx_rotations_pt_lkp lkp
on  ro.perf_track_1 = lkp.perf_track_1
and ro.perf_track_2 = lkp.perf_track_2
and ro.perf_track_3 = lkp.perf_track_3
and ro.perf_track_4 = lkp.perf_track_4
and ro.perf_track_5 = lkp.perf_track_5
and ro.perf_track_6 = lkp.perf_track_6
and ro.perf_track_7 = lkp.perf_track_7
and ro.perf_track_8 = lkp.perf_track_8
and ro.perf_track_9 = lkp.perf_track_9
and ro.perf_track_10 = lkp.perf_track_10
left outer join choco_data.dw_mpx_rotations roi
on ro.rotation_id = roi.rotation_id
where roi.rotation_id is null;



insert overwrite table choco_data.V_GBH_FROM_NAME
select rotation_id,
       GBH_TMP,
       GBH_TMP1,
       NEW_GEO_CLIENT_ID
from (
select rotation_id,
       rotation_name,
       GBH_TMP,
       GBH_TMP1,
       case when cast(GBH_TMP1 as int) is null then null else cast(GBH_TMP1 as int) end as NEW_GEO_CLIENT_ID
from (
select rotation_id,
       rotation_name,
       GBH_TMP,
       case when instr(GBH_TMP,'_') > 0 then substring(GBH_TMP, 1, instr(GBH_TMP,'_') - 1 ) else GBH_TMP end as GBH_TMP1
from (
select rotation_id,
       rotation_name,
       case when instr(rotation_name, '&GBH_ID=') > 0 then substring(rotation_name, instr(rotation_name, '&GBH_ID=')+8, 10) else null end as GBH_TMP
from choco_data.dw_mpx_rotations_temp
) gt
) gtone
) ngci
where instr(rotation_name, '&GBH_ID=') > 0 and NEW_GEO_CLIENT_ID is not null;



insert overwrite table choco_data.dw_mpx_rotations_temp_one
select t.rotation_id,
       t.placement_id,
       t.campaign_id,
       t.vendor_id,
       t.PORTL_BKT_ID,
       t.PORTL_SUB_BKT_ID,
       t.PORTL_PRTNR_GRP_ID,
       t.rotation_string,
       trim(t.rotation_name) rotation_name,
       t.rotation_cost,
       t.rotation_count,
       trim(t.rotation_count_type) rotation_count_type,
       t.rotation_date_start,
       t.rotation_date_end,
       t.PORTL_DSHBRD_FLAG_ID,
       t.PORTL_INVC_FLAG_ID,
       t.PORTL_SRC_CRE_DT STRING,
       trim(t.ROTATION_CT_URL_NAME) ROTATION_CT_URL_NAME,
       trim(t.ROTATION_STS_NAME) ROTATION_STS_NAME,
       trim(t.ROTATION_DESC_TXT) ROTATION_DESC_TXT,
       t.MPX_PIXEL_DMNSN_SIZE_TXT,
       t.MPX_ORGNZNG_TXT,
       t.TRFC_ORDER_STNDRD_TXT,
       t.TRFC_ORDER_JAVA_SCRPT_TXT,
       t.TRFC_ORDER_LINK_TXT,
       t.TRFC_ORDER_TRCR_TXT,
       t.ROT_PT_ID,
       t.MPX_CHNL_ID,
       t.PORTL_APP_ID,
       nvl(v.NEW_GEO_CLIENT_ID, t.GEO_CLIENT_ID) GEO_CLIENT_ID,
       t.ROTATION_BRAND_IND,
       t.CRE_DATE,
       t.CRE_USER,
       t.UPD_DATE,
       t.UPD_USER
from choco_data.dw_mpx_rotations_temp t
left outer join choco_data.V_GBH_FROM_NAME v
on t.rotation_id = v.rotation_id;


insert overwrite table choco_data.dw_mpx_rotations_temp_two
select t.rotation_id,
       t.placement_id,
       t.campaign_id,
       t.vendor_id,
       t.PORTL_BKT_ID,
       t.PORTL_SUB_BKT_ID,
       t.PORTL_PRTNR_GRP_ID,
       t.rotation_string,
       t.rotation_name,
       t.rotation_cost,
       t.rotation_count,
       t.rotation_count_type,
       t.rotation_date_start,
       t.rotation_date_end,
       t.PORTL_DSHBRD_FLAG_ID,
       t.PORTL_INVC_FLAG_ID,
       t.PORTL_SRC_CRE_DT STRING,
       t.ROTATION_CT_URL_NAME,
       t.ROTATION_STS_NAME,
       t.ROTATION_DESC_TXT,
       t.MPX_PIXEL_DMNSN_SIZE_TXT,
       t.MPX_ORGNZNG_TXT,
       t.TRFC_ORDER_STNDRD_TXT,
       t.TRFC_ORDER_JAVA_SCRPT_TXT,
       t.TRFC_ORDER_LINK_TXT,
       t.TRFC_ORDER_TRCR_TXT,
       t.ROT_PT_ID,
       t.MPX_CHNL_ID,
       t.PORTL_APP_ID,
       t.GEO_CLIENT_ID,
       1,
       t.CRE_DATE,
       t.CRE_USER,
       current_timestamp() upd_date,
       t.UPD_USER
from choco_data.dw_mpx_rotations_temp_one t
where instr(ROTATION_DESC_TXT, 'GBC') > 0 and ROTATION_BRAND_IND = 0;


insert into table choco_data.dw_mpx_rotations_temp_two
select t.rotation_id,
       t.placement_id,
       t.campaign_id,
       t.vendor_id,
       t.PORTL_BKT_ID,
       t.PORTL_SUB_BKT_ID,
       t.PORTL_PRTNR_GRP_ID,
       t.rotation_string,
       t.rotation_name,
       t.rotation_cost,
       t.rotation_count,
       t.rotation_count_type,
       t.rotation_date_start,
       t.rotation_date_end,
       t.PORTL_DSHBRD_FLAG_ID,
       t.PORTL_INVC_FLAG_ID,
       t.PORTL_SRC_CRE_DT STRING,
       t.ROTATION_CT_URL_NAME,
       t.ROTATION_STS_NAME,
       t.ROTATION_DESC_TXT,
       t.MPX_PIXEL_DMNSN_SIZE_TXT,
       t.MPX_ORGNZNG_TXT,
       t.TRFC_ORDER_STNDRD_TXT,
       t.TRFC_ORDER_JAVA_SCRPT_TXT,
       t.TRFC_ORDER_LINK_TXT,
       t.TRFC_ORDER_TRCR_TXT,
       t.ROT_PT_ID,
       t.MPX_CHNL_ID,
       t.PORTL_APP_ID,
       t.GEO_CLIENT_ID,
       t.ROTATION_BRAND_IND,
       t.CRE_DATE,
       t.CRE_USER,
       t.UPD_DATE,
       t.UPD_USER
from choco_data.dw_mpx_rotations_temp_one t
where ROTATION_DESC_TXT not like '%GBC%' or ROTATION_BRAND_IND <>0;



insert overwrite table choco_data.dw_mpx_rotations
select *
from choco_data.dw_mpx_rotations_temp_two;