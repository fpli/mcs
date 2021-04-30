insert into table choco_data.ams_imprsn_diff_tmp_test
select new_imprsn.imprsn_cntnr_id,new_imprsn.imprsn_dt
FROM (select * from choco_data.ams_imprsn_new_test where imprsn_dt='#{imprsn_dt}' and imprsn_cntnr_id is not  null) new_imprsn
    FULL outer join (select * from choco_data.ams_imprsn_old_test where imprsn_dt='#{imprsn_dt}' and imprsn_cntnr_id is not  null) old_imprsn
on  new_imprsn.imprsn_cntnr_id <=>     old_imprsn.imprsn_cntnr_id and
    new_imprsn.imprsn_cntnr_id,old_imprsn.imprsn_cntnr_id,
    new_imprsn.file_schm_vrsn_num,old_imprsn.file_schm_vrsn_num,
    new_imprsn.file_id     ,old_imprsn.file_id     ,
    new_imprsn.batch_id    ,old_imprsn.batch_id    ,
    new_imprsn.chnl_id     ,old_imprsn.chnl_id     ,
    new_imprsn.crltn_guid_txt,old_imprsn.crltn_guid_txt,
    new_imprsn.guid_txt    ,old_imprsn.guid_txt    ,
    new_imprsn.user_id     ,old_imprsn.user_id     ,
    new_imprsn.clnt_rmt_ip ,old_imprsn.clnt_rmt_ip ,
    new_imprsn.brwsr_type_num,old_imprsn.brwsr_type_num,
    new_imprsn.brwsr_name  ,old_imprsn.brwsr_name  ,
    new_imprsn.rfr_url_name,old_imprsn.rfr_url_name,
    new_imprsn.prvs_rfr_dmn_name,old_imprsn.prvs_rfr_dmn_name,
    new_imprsn.user_query_txt,old_imprsn.user_query_txt,
    new_imprsn.plcmnt_data_txt,old_imprsn.plcmnt_data_txt,
    new_imprsn.pblshr_id   ,old_imprsn.pblshr_id   ,
    new_imprsn.ams_pblshr_cmpgn_id,old_imprsn.ams_pblshr_cmpgn_id,
    new_imprsn.ams_tool_id ,old_imprsn.ams_tool_id ,
    new_imprsn.cstm_id     ,old_imprsn.cstm_id     ,
    new_imprsn.flex_fld_vrsn_num,old_imprsn.flex_fld_vrsn_num,
    new_imprsn.flex_fld_1_txt,old_imprsn.flex_fld_1_txt,
    new_imprsn.flex_fld_2_txt,old_imprsn.flex_fld_2_txt,
    new_imprsn.flex_fld_3_txt,old_imprsn.flex_fld_3_txt,
    new_imprsn.flex_fld_4_txt,old_imprsn.flex_fld_4_txt,
    new_imprsn.flex_fld_5_txt,old_imprsn.flex_fld_5_txt,
    new_imprsn.flex_fld_6_txt,old_imprsn.flex_fld_6_txt,
    new_imprsn.flex_fld_7_txt,old_imprsn.flex_fld_7_txt,
    new_imprsn.flex_fld_8_txt,old_imprsn.flex_fld_8_txt,
    new_imprsn.flex_fld_9_txt,old_imprsn.flex_fld_9_txt,
    new_imprsn.flex_fld_10_txt,old_imprsn.flex_fld_10_txt,
    new_imprsn.flex_fld_11_txt,old_imprsn.flex_fld_11_txt,
    new_imprsn.flex_fld_12_txt,old_imprsn.flex_fld_12_txt,
    new_imprsn.flex_fld_13_txt,old_imprsn.flex_fld_13_txt,
    new_imprsn.flex_fld_14_txt,old_imprsn.flex_fld_14_txt,
    new_imprsn.flex_fld_15_txt,old_imprsn.flex_fld_15_txt,
    new_imprsn.flex_fld_16_txt,old_imprsn.flex_fld_16_txt,
    new_imprsn.flex_fld_17_txt,old_imprsn.flex_fld_17_txt,
    new_imprsn.flex_fld_18_txt,old_imprsn.flex_fld_18_txt,
    new_imprsn.flex_fld_19_txt,old_imprsn.flex_fld_19_txt,
    new_imprsn.flex_fld_20_txt,old_imprsn.flex_fld_20_txt,
    new_imprsn.ctx_txt     ,old_imprsn.ctx_txt     ,
    new_imprsn.ctx_cld_txt ,old_imprsn.ctx_cld_txt ,
    new_imprsn.ctx_rslt_txt,old_imprsn.ctx_rslt_txt,
    new_imprsn.rfr_dmn_name,old_imprsn.rfr_dmn_name,
    new_imprsn.imprsn_ts   ,old_imprsn.imprsn_ts   ,
    new_imprsn.ams_prgrm_id,old_imprsn.ams_prgrm_id,
    new_imprsn.advrtsr_id  ,old_imprsn.advrtsr_id  ,
    new_imprsn.ams_click_fltr_type_id,old_imprsn.ams_click_fltr_type_id,
    new_imprsn.filter_yn_ind,old_imprsn.filter_yn_ind,
    new_imprsn.rover_url_txt,old_imprsn.rover_url_txt,
    new_imprsn.mplx_timeout_flag,old_imprsn.mplx_timeout_flag,
    new_imprsn.cb_kw       ,old_imprsn.cb_kw       ,
    new_imprsn.cb_cat      ,old_imprsn.cb_cat      ,
    new_imprsn.cb_ex_kw    ,old_imprsn.cb_ex_kw    ,
    new_imprsn.cb_ex_cat   ,old_imprsn.cb_ex_cat   ,
    new_imprsn.fb_used     ,old_imprsn.fb_used     ,
    new_imprsn.ad_format   ,old_imprsn.ad_format   ,
    new_imprsn.ad_content_type,old_imprsn.ad_content_type,
    new_imprsn.load_time   ,old_imprsn.load_time   ,
    new_imprsn.app_id      ,old_imprsn.app_id      ,
    new_imprsn.app_package_name,old_imprsn.app_package_name,
    new_imprsn.app_name    ,old_imprsn.app_name    ,
    new_imprsn.app_version ,old_imprsn.app_version ,
    new_imprsn.device_name ,old_imprsn.device_name ,
    new_imprsn.os_name     ,old_imprsn.os_name     ,
    new_imprsn.os_version  ,old_imprsn.os_version  ,
    new_imprsn.udid        ,old_imprsn.udid        ,
    new_imprsn.sdk_name    ,old_imprsn.sdk_name    ,
    new_imprsn.sdk_version ,old_imprsn.sdk_version ,
    new_imprsn.ams_trans_rsn_cd,old_imprsn.ams_trans_rsn_cd,
    new_imprsn.trfc_src_cd ,old_imprsn.trfc_src_cd ,
    new_imprsn.rt_rule_flag1,old_imprsn.rt_rule_flag1,
    new_imprsn.rt_rule_flag2,old_imprsn.rt_rule_flag2,
    new_imprsn.rt_rule_flag3,old_imprsn.rt_rule_flag3,
    new_imprsn.rt_rule_flag4,old_imprsn.rt_rule_flag4,
    new_imprsn.rt_rule_flag5,old_imprsn.rt_rule_flag5,
    new_imprsn.rt_rule_flag6,old_imprsn.rt_rule_flag6,
    new_imprsn.rt_rule_flag7,old_imprsn.rt_rule_flag7,
    new_imprsn.rt_rule_flag8,old_imprsn.rt_rule_flag8,
    new_imprsn.rt_rule_flag9,old_imprsn.rt_rule_flag9,
    new_imprsn.rt_rule_flag10,old_imprsn.rt_rule_flag10,
    new_imprsn.rt_rule_flag11,old_imprsn.rt_rule_flag11,
    new_imprsn.rt_rule_flag12,old_imprsn.rt_rule_flag12,
    new_imprsn.rt_rule_flag13,old_imprsn.rt_rule_flag13,
    new_imprsn.rt_rule_flag14,old_imprsn.rt_rule_flag14,
    new_imprsn.rt_rule_flag15,old_imprsn.rt_rule_flag15,
    new_imprsn.rt_rule_flag16,old_imprsn.rt_rule_flag16,
    new_imprsn.rt_rule_flag17,old_imprsn.rt_rule_flag17,
    new_imprsn.rt_rule_flag18,old_imprsn.rt_rule_flag18,
    new_imprsn.rt_rule_flag19,old_imprsn.rt_rule_flag19,
    new_imprsn.rt_rule_flag20,old_imprsn.rt_rule_flag20,
    new_imprsn.rt_rule_flag21,old_imprsn.rt_rule_flag21,
    new_imprsn.rt_rule_flag22,old_imprsn.rt_rule_flag22,
    new_imprsn.rt_rule_flag23,old_imprsn.rt_rule_flag23,
    new_imprsn.rt_rule_flag24,old_imprsn.rt_rule_flag24,
    new_imprsn.nrt_rule_flag1,old_imprsn.nrt_rule_flag1,
    new_imprsn.nrt_rule_flag2,old_imprsn.nrt_rule_flag2,
    new_imprsn.nrt_rule_flag3,old_imprsn.nrt_rule_flag3,
    new_imprsn.nrt_rule_flag4,old_imprsn.nrt_rule_flag4,
    new_imprsn.nrt_rule_flag5,old_imprsn.nrt_rule_flag5,
    new_imprsn.nrt_rule_flag6,old_imprsn.nrt_rule_flag6,
    new_imprsn.nrt_rule_flag7,old_imprsn.nrt_rule_flag7,
    new_imprsn.nrt_rule_flag8,old_imprsn.nrt_rule_flag8,
    new_imprsn.nrt_rule_flag9,old_imprsn.nrt_rule_flag9,
    new_imprsn.nrt_rule_flag10,old_imprsn.nrt_rule_flag10,
    new_imprsn.nrt_rule_flag11,old_imprsn.nrt_rule_flag11,
    new_imprsn.nrt_rule_flag12,old_imprsn.nrt_rule_flag12,
    new_imprsn.nrt_rule_flag13,old_imprsn.nrt_rule_flag13,
    new_imprsn.nrt_rule_flag14,old_imprsn.nrt_rule_flag14,
    new_imprsn.nrt_rule_flag15,old_imprsn.nrt_rule_flag15,
    new_imprsn.nrt_rule_flag16,old_imprsn.nrt_rule_flag16,
    new_imprsn.nrt_rule_flag17,old_imprsn.nrt_rule_flag17,
    new_imprsn.nrt_rule_flag18,old_imprsn.nrt_rule_flag18,
    new_imprsn.nrt_rule_flag19,old_imprsn.nrt_rule_flag19,
    new_imprsn.nrt_rule_flag20,old_imprsn.nrt_rule_flag20,
    new_imprsn.nrt_rule_flag21,old_imprsn.nrt_rule_flag21,
    new_imprsn.nrt_rule_flag22,old_imprsn.nrt_rule_flag22,
    new_imprsn.nrt_rule_flag23,old_imprsn.nrt_rule_flag23,
    new_imprsn.nrt_rule_flag24,old_imprsn.nrt_rule_flag24,
    new_imprsn.nrt_rule_flag25,old_imprsn.nrt_rule_flag25,
    new_imprsn.nrt_rule_flag26,old_imprsn.nrt_rule_flag26,
    new_imprsn.nrt_rule_flag27,old_imprsn.nrt_rule_flag27,
    new_imprsn.nrt_rule_flag28,old_imprsn.nrt_rule_flag28,
    new_imprsn.nrt_rule_flag29,old_imprsn.nrt_rule_flag29,
    new_imprsn.nrt_rule_flag30,old_imprsn.nrt_rule_flag30,
    new_imprsn.nrt_rule_flag31,old_imprsn.nrt_rule_flag31,
    new_imprsn.nrt_rule_flag32,old_imprsn.nrt_rule_flag32,
    new_imprsn.nrt_rule_flag33,old_imprsn.nrt_rule_flag33,
    new_imprsn.nrt_rule_flag34,old_imprsn.nrt_rule_flag34,
    new_imprsn.nrt_rule_flag35,old_imprsn.nrt_rule_flag35,
    new_imprsn.nrt_rule_flag36,old_imprsn.nrt_rule_flag36,
    new_imprsn.nrt_rule_flag37,old_imprsn.nrt_rule_flag37,
    new_imprsn.nrt_rule_flag38,old_imprsn.nrt_rule_flag38,
    new_imprsn.nrt_rule_flag39,old_imprsn.nrt_rule_flag39,
    new_imprsn.nrt_rule_flag40,old_imprsn.nrt_rule_flag40,
    new_imprsn.nrt_rule_flag41,old_imprsn.nrt_rule_flag41,
    new_imprsn.nrt_rule_flag42,old_imprsn.nrt_rule_flag42,
    new_imprsn.nrt_rule_flag43,old_imprsn.nrt_rule_flag43,
    new_imprsn.nrt_rule_flag44,old_imprsn.nrt_rule_flag44,
    new_imprsn.nrt_rule_flag45,old_imprsn.nrt_rule_flag45,
    new_imprsn.nrt_rule_flag46,old_imprsn.nrt_rule_flag46,
    new_imprsn.nrt_rule_flag47,old_imprsn.nrt_rule_flag47,
    new_imprsn.nrt_rule_flag48,old_imprsn.nrt_rule_flag48,
    new_imprsn.nrt_rule_flag49,old_imprsn.nrt_rule_flag49,
    new_imprsn.nrt_rule_flag50,old_imprsn.nrt_rule_flag50,
    new_imprsn.nrt_rule_flag51,old_imprsn.nrt_rule_flag51,
    new_imprsn.nrt_rule_flag51,old_imprsn.nrt_rule_flag51,
    new_imprsn.nrt_rule_flag52,old_imprsn.nrt_rule_flag52,
    new_imprsn.nrt_rule_flag53,old_imprsn.nrt_rule_flag53,
    new_imprsn.nrt_rule_flag54,old_imprsn.nrt_rule_flag54,
    new_imprsn.nrt_rule_flag55,old_imprsn.nrt_rule_flag55,
    new_imprsn.nrt_rule_flag56,old_imprsn.nrt_rule_flag56,
    new_imprsn.nrt_rule_flag57,old_imprsn.nrt_rule_flag57,
    new_imprsn.nrt_rule_flag58,old_imprsn.nrt_rule_flag58,
    new_imprsn.nrt_rule_flag59,old_imprsn.nrt_rule_flag59,
    new_imprsn.nrt_rule_flag60,old_imprsn.nrt_rule_flag60,
    new_imprsn.nrt_rule_flag61,old_imprsn.nrt_rule_flag61,
    new_imprsn.nrt_rule_flag62,old_imprsn.nrt_rule_flag62,
    new_imprsn.nrt_rule_flag63,old_imprsn.nrt_rule_flag63,
    new_imprsn.nrt_rule_flag64,old_imprsn.nrt_rule_flag64,
    new_imprsn.nrt_rule_flag65,old_imprsn.nrt_rule_flag65,
    new_imprsn.nrt_rule_flag66,old_imprsn.nrt_rule_flag66,
    new_imprsn.nrt_rule_flag67,old_imprsn.nrt_rule_flag67,
    new_imprsn.nrt_rule_flag68,old_imprsn.nrt_rule_flag68,
    new_imprsn.nrt_rule_flag69,old_imprsn.nrt_rule_flag69,
    new_imprsn.nrt_rule_flag70,old_imprsn.nrt_rule_flag70,
    new_imprsn.nrt_rule_flag71,old_imprsn.nrt_rule_flag71,
    new_imprsn.nrt_rule_flag72,old_imprsn.nrt_rule_flag72,
    new_imprsn.imprsn_cntnr_id is not null and old_imprsn.imprsn_cntnr_id is not null)
UNION
select null as imprsn_cntnr_id,'#{imprsn_dt}' as imprsn_dt
where (select count(*) as count from choco_data.ams_imprsn_new_test where imprsn_dt='#{imprsn_dt}' and imprsn_cntnr_id is null) <>
      (select count(*) as count from choco_data.ams_imprsn_old_test where imprsn_dt='#{imprsn_dt}' and imprsn_cntnr_id is null)