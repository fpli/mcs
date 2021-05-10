CREATE EXTERNAL TABLE choco_data.epnnrt_imprsn_automation_diff(
    `old_imprsn_cntnr_id` decimal(18),
    `new_imprsn_cntnr_id` decimal(18),
    `old_file_schm_vrsn_num` smallint,
    `new_file_schm_vrsn_num` smallint,
    `old_file_id` decimal(18),
    `new_file_id` decimal(18),
    `old_batch_id` decimal(18),
    `new_batch_id` decimal(18),
    `old_chnl_id` int,
    `new_chnl_id` int,
    `old_crltn_guid_txt` string,
    `new_crltn_guid_txt` string,
    `old_guid_txt` string,
    `new_guid_txt` string,
    `old_user_id` decimal(18),
    `new_user_id` decimal(18),
    `old_clnt_rmt_ip` string,
    `new_clnt_rmt_ip` string,
    `old_brwsr_type_num` int,
    `new_brwsr_type_num` int,
    `old_brwsr_name` string,
    `new_brwsr_name` string,
    `old_rfr_url_name` string,
    `new_rfr_url_name` string,
    `old_prvs_rfr_dmn_name` string,
    `new_prvs_rfr_dmn_name` string,
    `old_user_query_txt` string,
    `new_user_query_txt` string,
    `old_plcmnt_data_txt` string,
    `new_plcmnt_data_txt` string,
    `old_pblshr_id` decimal(18),
    `new_pblshr_id` decimal(18),
    `old_ams_pblshr_cmpgn_id` decimal(18),
    `new_ams_pblshr_cmpgn_id` decimal(18),
    `old_ams_tool_id` decimal(18),
    `new_ams_tool_id` decimal(18),
    `old_cstm_id` string,
    `new_cstm_id` string,
    `old_flex_fld_vrsn_num` int,
    `new_flex_fld_vrsn_num` int,
    `old_flex_fld_1_txt` string,
    `new_flex_fld_1_txt` string,
    `old_flex_fld_2_txt` string,
    `new_flex_fld_2_txt` string,
    `old_flex_fld_3_txt` string,
    `new_flex_fld_3_txt` string,
    `old_flex_fld_4_txt` string,
    `new_flex_fld_4_txt` string,
    `old_flex_fld_5_txt` string,
    `new_flex_fld_5_txt` string,
    `old_flex_fld_6_txt` string,
    `new_flex_fld_6_txt` string,
    `old_flex_fld_7_txt` string,
    `new_flex_fld_7_txt` string,
    `old_flex_fld_8_txt` string,
    `new_flex_fld_8_txt` string,
    `old_flex_fld_9_txt` string,
    `new_flex_fld_9_txt` string,
    `old_flex_fld_10_txt` string,
    `new_flex_fld_10_txt` string,
    `old_flex_fld_11_txt` string,
    `new_flex_fld_11_txt` string,
    `old_flex_fld_12_txt` string,
    `new_flex_fld_12_txt` string,
    `old_flex_fld_13_txt` string,
    `new_flex_fld_13_txt` string,
    `old_flex_fld_14_txt` string,
    `new_flex_fld_14_txt` string,
    `old_flex_fld_15_txt` string,
    `new_flex_fld_15_txt` string,
    `old_flex_fld_16_txt` string,
    `new_flex_fld_16_txt` string,
    `old_flex_fld_17_txt` string,
    `new_flex_fld_17_txt` string,
    `old_flex_fld_18_txt` string,
    `new_flex_fld_18_txt` string,
    `old_flex_fld_19_txt` string,
    `new_flex_fld_19_txt` string,
    `old_flex_fld_20_txt` string,
    `new_flex_fld_20_txt` string,
    `old_ctx_txt` string,
    `new_ctx_txt` string,
    `old_ctx_cld_txt` string,
    `new_ctx_cld_txt` string,
    `old_ctx_rslt_txt` string,
    `new_ctx_rslt_txt` string,
    `old_rfr_dmn_name` string,
    `new_rfr_dmn_name` string,
    `old_imprsn_ts` timestamp,
    `new_imprsn_ts` timestamp,
    `old_ams_prgrm_id` tinyint,
    `new_ams_prgrm_id` tinyint,
    `old_advrtsr_id` tinyint,
    `new_advrtsr_id` tinyint,
    `old_ams_click_fltr_type_id` tinyint,
    `new_ams_click_fltr_type_id` tinyint,
    `old_filter_yn_ind` tinyint,
    `new_filter_yn_ind` tinyint,
    `old_rover_url_txt` string,
    `new_rover_url_txt` string,
    `old_mplx_timeout_flag` tinyint,
    `new_mplx_timeout_flag` tinyint,
    `old_cb_kw` string,
    `new_cb_kw` string,
    `old_cb_cat` string,
    `new_cb_cat` string,
    `old_cb_ex_kw` string,
    `new_cb_ex_kw` string,
    `old_cb_ex_cat` string,
    `new_cb_ex_cat` string,
    `old_fb_used` string,
    `new_fb_used` string,
    `old_ad_format` string,
    `new_ad_format` string,
    `old_ad_content_type` string,
    `new_ad_content_type` string,
    `old_load_time` decimal(18),
    `new_load_time` decimal(18),
    `old_app_id` string,
    `new_app_id` string,
    `old_app_package_name` string,
    `new_app_package_name` string,
    `old_app_name` string,
    `new_app_name` string,
    `old_app_version` string,
    `new_app_version` string,
    `old_device_name` string,
    `new_device_name` string,
    `old_os_name` string,
    `new_os_name` string,
    `old_os_version` string,
    `new_os_version` string,
    `old_udid` string,
    `new_udid` string,
    `old_sdk_name` string,
    `new_sdk_name` string,
    `old_sdk_version` string,
    `new_sdk_version` string,
    `old_ams_trans_rsn_cd` smallint,
    `new_ams_trans_rsn_cd` smallint,
    `old_trfc_src_cd` tinyint,
    `new_trfc_src_cd` tinyint,
    `old_rt_rule_flag1` tinyint,
    `new_rt_rule_flag1` tinyint,
    `old_rt_rule_flag2` tinyint,
    `new_rt_rule_flag2` tinyint,
    `old_rt_rule_flag3` tinyint,
    `new_rt_rule_flag3` tinyint,
    `old_rt_rule_flag4` tinyint,
    `new_rt_rule_flag4` tinyint,
    `old_rt_rule_flag5` tinyint,
    `new_rt_rule_flag5` tinyint,
    `old_rt_rule_flag6` tinyint,
    `new_rt_rule_flag6` tinyint,
    `old_rt_rule_flag7` tinyint,
    `new_rt_rule_flag7` tinyint,
    `old_rt_rule_flag8` tinyint,
    `new_rt_rule_flag8` tinyint,
    `old_rt_rule_flag9` tinyint,
    `new_rt_rule_flag9` tinyint,
    `old_rt_rule_flag10` tinyint,
    `new_rt_rule_flag10` tinyint,
    `old_rt_rule_flag11` tinyint,
    `new_rt_rule_flag11` tinyint,
    `old_rt_rule_flag12` tinyint,
    `new_rt_rule_flag12` tinyint,
    `old_rt_rule_flag13` tinyint,
    `new_rt_rule_flag13` tinyint,
    `old_rt_rule_flag14` tinyint,
    `new_rt_rule_flag14` tinyint,
    `old_rt_rule_flag15` tinyint,
    `new_rt_rule_flag15` tinyint,
    `old_rt_rule_flag16` tinyint,
    `new_rt_rule_flag16` tinyint,
    `old_rt_rule_flag17` tinyint,
    `new_rt_rule_flag17` tinyint,
    `old_rt_rule_flag18` tinyint,
    `new_rt_rule_flag18` tinyint,
    `old_rt_rule_flag19` tinyint,
    `new_rt_rule_flag19` tinyint,
    `old_rt_rule_flag20` tinyint,
    `new_rt_rule_flag20` tinyint,
    `old_rt_rule_flag21` tinyint,
    `new_rt_rule_flag21` tinyint,
    `old_rt_rule_flag22` tinyint,
    `new_rt_rule_flag22` tinyint,
    `old_rt_rule_flag23` tinyint,
    `new_rt_rule_flag23` tinyint,
    `old_rt_rule_flag24` tinyint,
    `new_rt_rule_flag24` tinyint,
    `old_nrt_rule_flag1` tinyint,
    `new_nrt_rule_flag1` tinyint,
    `old_nrt_rule_flag2` tinyint,
    `new_nrt_rule_flag2` tinyint,
    `old_nrt_rule_flag3` tinyint,
    `new_nrt_rule_flag3` tinyint,
    `old_nrt_rule_flag4` tinyint,
    `new_nrt_rule_flag4` tinyint,
    `old_nrt_rule_flag5` tinyint,
    `new_nrt_rule_flag5` tinyint,
    `old_nrt_rule_flag6` tinyint,
    `new_nrt_rule_flag6` tinyint,
    `old_nrt_rule_flag7` tinyint,
    `new_nrt_rule_flag7` tinyint,
    `old_nrt_rule_flag8` tinyint,
    `new_nrt_rule_flag8` tinyint,
    `old_nrt_rule_flag9` tinyint,
    `new_nrt_rule_flag9` tinyint,
    `old_nrt_rule_flag10` tinyint,
    `new_nrt_rule_flag10` tinyint,
    `old_nrt_rule_flag11` tinyint,
    `new_nrt_rule_flag11` tinyint,
    `old_nrt_rule_flag12` tinyint,
    `new_nrt_rule_flag12` tinyint,
    `old_nrt_rule_flag13` tinyint,
    `new_nrt_rule_flag13` tinyint,
    `old_nrt_rule_flag14` tinyint,
    `new_nrt_rule_flag14` tinyint,
    `old_nrt_rule_flag15` tinyint,
    `new_nrt_rule_flag15` tinyint,
    `old_nrt_rule_flag16` tinyint,
    `new_nrt_rule_flag16` tinyint,
    `old_nrt_rule_flag17` tinyint,
    `new_nrt_rule_flag17` tinyint,
    `old_nrt_rule_flag18` tinyint,
    `new_nrt_rule_flag18` tinyint,
    `old_nrt_rule_flag19` tinyint,
    `new_nrt_rule_flag19` tinyint,
    `old_nrt_rule_flag20` tinyint,
    `new_nrt_rule_flag20` tinyint,
    `old_nrt_rule_flag21` tinyint,
    `new_nrt_rule_flag21` tinyint,
    `old_nrt_rule_flag22` tinyint,
    `new_nrt_rule_flag22` tinyint,
    `old_nrt_rule_flag23` tinyint,
    `new_nrt_rule_flag23` tinyint,
    `old_nrt_rule_flag24` tinyint,
    `new_nrt_rule_flag24` tinyint,
    `old_nrt_rule_flag25` tinyint,
    `new_nrt_rule_flag25` tinyint,
    `old_nrt_rule_flag26` tinyint,
    `new_nrt_rule_flag26` tinyint,
    `old_nrt_rule_flag27` tinyint,
    `new_nrt_rule_flag27` tinyint,
    `old_nrt_rule_flag28` tinyint,
    `new_nrt_rule_flag28` tinyint,
    `old_nrt_rule_flag29` tinyint,
    `new_nrt_rule_flag29` tinyint,
    `old_nrt_rule_flag30` tinyint,
    `new_nrt_rule_flag30` tinyint,
    `old_nrt_rule_flag31` tinyint,
    `new_nrt_rule_flag31` tinyint,
    `old_nrt_rule_flag32` tinyint,
    `new_nrt_rule_flag32` tinyint,
    `old_nrt_rule_flag33` tinyint,
    `new_nrt_rule_flag33` tinyint,
    `old_nrt_rule_flag34` tinyint,
    `new_nrt_rule_flag34` tinyint,
    `old_nrt_rule_flag35` tinyint,
    `new_nrt_rule_flag35` tinyint,
    `old_nrt_rule_flag36` tinyint,
    `new_nrt_rule_flag36` tinyint,
    `old_nrt_rule_flag37` tinyint,
    `new_nrt_rule_flag37` tinyint,
    `old_nrt_rule_flag38` tinyint,
    `new_nrt_rule_flag38` tinyint,
    `old_nrt_rule_flag39` tinyint,
    `new_nrt_rule_flag39` tinyint,
    `old_nrt_rule_flag40` tinyint,
    `new_nrt_rule_flag40` tinyint,
    `old_nrt_rule_flag41` tinyint,
    `new_nrt_rule_flag41` tinyint,
    `old_nrt_rule_flag42` tinyint,
    `new_nrt_rule_flag42` tinyint,
    `old_nrt_rule_flag43` tinyint,
    `new_nrt_rule_flag43` tinyint,
    `old_nrt_rule_flag44` tinyint,
    `new_nrt_rule_flag44` tinyint,
    `old_nrt_rule_flag45` tinyint,
    `new_nrt_rule_flag45` tinyint,
    `old_nrt_rule_flag46` tinyint,
    `new_nrt_rule_flag46` tinyint,
    `old_nrt_rule_flag47` tinyint,
    `new_nrt_rule_flag47` tinyint,
    `old_nrt_rule_flag48` tinyint,
    `new_nrt_rule_flag48` tinyint,
    `old_nrt_rule_flag49` tinyint,
    `new_nrt_rule_flag49` tinyint,
    `old_nrt_rule_flag50` tinyint,
    `new_nrt_rule_flag50` tinyint,
    `old_nrt_rule_flag51` tinyint,
    `new_nrt_rule_flag51` tinyint,
    `old_nrt_rule_flag52` tinyint,
    `new_nrt_rule_flag52` tinyint,
    `old_nrt_rule_flag53` tinyint,
    `new_nrt_rule_flag53` tinyint,
    `old_nrt_rule_flag54` tinyint,
    `new_nrt_rule_flag54` tinyint,
    `old_nrt_rule_flag55` tinyint,
    `new_nrt_rule_flag55` tinyint,
    `old_nrt_rule_flag56` tinyint,
    `new_nrt_rule_flag56` tinyint,
    `old_nrt_rule_flag57` tinyint,
    `new_nrt_rule_flag57` tinyint,
    `old_nrt_rule_flag58` tinyint,
    `new_nrt_rule_flag58` tinyint,
    `old_nrt_rule_flag59` tinyint,
    `new_nrt_rule_flag59` tinyint,
    `old_nrt_rule_flag60` tinyint,
    `new_nrt_rule_flag60` tinyint,
    `old_nrt_rule_flag61` tinyint,
    `new_nrt_rule_flag61` tinyint,
    `old_nrt_rule_flag62` tinyint,
    `new_nrt_rule_flag62` tinyint,
    `old_nrt_rule_flag63` tinyint,
    `new_nrt_rule_flag63` tinyint,
    `old_nrt_rule_flag64` tinyint,
    `new_nrt_rule_flag64` tinyint,
    `old_nrt_rule_flag65` tinyint,
    `new_nrt_rule_flag65` tinyint,
    `old_nrt_rule_flag66` tinyint,
    `new_nrt_rule_flag66` tinyint,
    `old_nrt_rule_flag67` tinyint,
    `new_nrt_rule_flag67` tinyint,
    `old_nrt_rule_flag68` tinyint,
    `new_nrt_rule_flag68` tinyint,
    `old_nrt_rule_flag69` tinyint,
    `new_nrt_rule_flag69` tinyint,
    `old_nrt_rule_flag70` tinyint,
    `new_nrt_rule_flag70` tinyint,
    `old_nrt_rule_flag71` tinyint,
    `new_nrt_rule_flag71` tinyint,
    `old_nrt_rule_flag72` tinyint,
    `new_nrt_rule_flag72` tinyint
)
PARTITIONED BY (
  `imprsn_dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'viewfs://apollo-rno/apps/b_marketing_tracking/epnnrt-automation-diff/imp'
TBLPROPERTIES (
  'transient_lastDdlTime'='1601444855')