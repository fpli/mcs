CREATE EXTERNAL TABLE choco_data.ams_click_v2(
  `imprsn_cntnr_id` decimal(18),
  `file_schm_vrsn_num` smallint,
  `file_id` decimal(18),
  `batch_id` decimal(18),
  `click_id` decimal(18),
  `chnl_id` int,
  `crltn_guid_txt` string,
  `guid_txt` string,
  `user_id` decimal(18),
  `clnt_rmt_ip` string,
  `brwsr_type_num` int,
  `brwsr_name` string,
  `rfr_url_name` string,
  `encryptd_ind` tinyint,
  `plcmnt_data_txt` string,
  `pblshr_id` decimal(18),
  `ams_pblshr_cmpgn_id` decimal(18),
  `ams_tool_id` decimal(18),
  `cstm_id` string,
  `lndng_page_url_name` string,
  `user_query_txt` string,
  `flex_fld_vrsn_num` int,
  `flex_fld_1_txt` string,
  `flex_fld_2_txt` string,
  `flex_fld_3_txt` string,
  `flex_fld_4_txt` string,
  `imprsn_ts` timestamp,
  `click_ts` timestamp,
  `last_vwd_item_id` decimal(18),
  `last_vwd_item_ts` timestamp,
  `last_adn_click_id` decimal(18),
  `last_adn_click_ts` timestamp,
  `flex_fld_5_txt` string,
  `flex_fld_6_txt` string,
  `flex_fld_7_txt` string,
  `flex_fld_8_txt` string,
  `flex_fld_9_txt` string,
  `flex_fld_10_txt` string,
  `flex_fld_11_txt` string,
  `flex_fld_12_txt` string,
  `flex_fld_13_txt` string,
  `flex_fld_14_txt` string,
  `flex_fld_15_txt` string,
  `flex_fld_16_txt` string,
  `flex_fld_17_txt` string,
  `flex_fld_18_txt` string,
  `flex_fld_19_txt` string,
  `flex_fld_20_txt` string,
  `icep_flex_fld_vrsn_id` smallint,
  `icep_flex_fld_1_txt` string,
  `icep_flex_fld_2_txt` string,
  `icep_flex_fld_3_txt` string,
  `icep_flex_fld_4_txt` string,
  `icep_flex_fld_5_txt` string,
  `icep_flex_fld_6_txt` string,
  `ams_prgrm_id` tinyint,
  `advrtsr_id` tinyint,
  `ams_click_fltr_type_id` tinyint,
  `imprsn_loose_match_ind` tinyint,
  `fltr_yn_ind` tinyint,
  `ams_trans_rsn_cd` smallint,
  `ams_page_type_map_id` decimal(18),
  `rfrng_dmn_name` string,
  `tfs_rfrng_dmn_name` string,
  `geo_trgtd_rsn_cd` tinyint,
  `src_plcmnt_data_txt` string,
  `geo_trgtd_cntry_cd` string,
  `tool_lvl_optn_ind` tinyint,
  `acnt_lvl_optn_ind` tinyint,
  `geo_trgtd_ind` tinyint,
  `pblshr_acptd_prgrm_ind` tinyint,
  `incmng_click_url_vctr_id` decimal(18),
  `str_name_txt` string,
  `item_id` decimal(18),
  `ctgry_id` decimal(18),
  `keyword_txt` string,
  `prgrm_excptn_list_ind` tinyint,
  `roi_fltr_yn_ind` tinyint,
  `seller_name` string,
  `rover_url_txt` string,
  `mplx_timeout_flag` tinyint,
  `app_id` string,
  `app_package_name` string,
  `app_name` string,
  `app_version` string,
  `device_name` string,
  `os_name` string,
  `os_version` string,
  `udid` string,
  `sdk_name` string,
  `sdk_version` string,
  `trfc_src_cd` tinyint,
  `roi_rule_values` int,
  `rt_rule_flag1` tinyint,
  `rt_rule_flag2` tinyint,
  `rt_rule_flag3` tinyint,
  `rt_rule_flag4` tinyint,
  `rt_rule_flag5` tinyint,
  `rt_rule_flag6` tinyint,
  `rt_rule_flag7` tinyint,
  `rt_rule_flag8` tinyint,
  `rt_rule_flag9` tinyint,
  `rt_rule_flag10` tinyint,
  `rt_rule_flag11` tinyint,
  `rt_rule_flag12` tinyint,
  `rt_rule_flag13` tinyint,
  `rt_rule_flag14` tinyint,
  `rt_rule_flag15` tinyint,
  `rt_rule_flag16` tinyint,
  `rt_rule_flag17` tinyint,
  `rt_rule_flag18` tinyint,
  `rt_rule_flag19` tinyint,
  `rt_rule_flag20` tinyint,
  `rt_rule_flag21` tinyint,
  `rt_rule_flag22` tinyint,
  `rt_rule_flag23` tinyint,
  `rt_rule_flag24` tinyint,
  `nrt_rule_flag1` tinyint,
  `nrt_rule_flag2` tinyint,
  `nrt_rule_flag3` tinyint,
  `nrt_rule_flag4` tinyint,
  `nrt_rule_flag5` tinyint,
  `nrt_rule_flag6` tinyint,
  `nrt_rule_flag7` tinyint,
  `nrt_rule_flag8` tinyint,
  `nrt_rule_flag9` tinyint,
  `nrt_rule_flag10` tinyint,
  `nrt_rule_flag11` tinyint,
  `nrt_rule_flag12` tinyint,
  `nrt_rule_flag13` tinyint,
  `nrt_rule_flag14` tinyint,
  `nrt_rule_flag15` tinyint,
  `nrt_rule_flag16` tinyint,
  `nrt_rule_flag17` tinyint,
  `nrt_rule_flag18` tinyint,
  `nrt_rule_flag19` tinyint,
  `nrt_rule_flag20` tinyint,
  `nrt_rule_flag21` tinyint,
  `nrt_rule_flag22` tinyint,
  `nrt_rule_flag23` tinyint,
  `nrt_rule_flag24` tinyint,
  `nrt_rule_flag25` tinyint,
  `nrt_rule_flag26` tinyint,
  `nrt_rule_flag27` tinyint,
  `nrt_rule_flag28` tinyint,
  `nrt_rule_flag29` tinyint,
  `nrt_rule_flag30` tinyint,
  `nrt_rule_flag31` tinyint,
  `nrt_rule_flag32` tinyint,
  `nrt_rule_flag33` tinyint,
  `nrt_rule_flag34` tinyint,
  `nrt_rule_flag35` tinyint,
  `nrt_rule_flag36` tinyint,
  `nrt_rule_flag37` tinyint,
  `nrt_rule_flag38` tinyint,
  `nrt_rule_flag39` tinyint,
  `nrt_rule_flag40` tinyint,
  `nrt_rule_flag41` tinyint,
  `nrt_rule_flag42` tinyint,
  `nrt_rule_flag43` tinyint,
  `nrt_rule_flag44` tinyint,
  `nrt_rule_flag45` tinyint,
  `nrt_rule_flag46` tinyint,
  `nrt_rule_flag47` tinyint,
  `nrt_rule_flag48` tinyint,
  `nrt_rule_flag49` tinyint,
  `nrt_rule_flag50` tinyint,
  `nrt_rule_flag51` tinyint,
  `nrt_rule_flag52` tinyint,
  `nrt_rule_flag53` tinyint,
  `nrt_rule_flag54` tinyint,
  `nrt_rule_flag55` tinyint,
  `nrt_rule_flag56` tinyint,
  `nrt_rule_flag57` tinyint,
  `nrt_rule_flag58` tinyint,
  `nrt_rule_flag59` tinyint,
  `nrt_rule_flag60` tinyint,
  `nrt_rule_flag61` tinyint,
  `nrt_rule_flag62` tinyint,
  `nrt_rule_flag63` tinyint,
  `nrt_rule_flag64` tinyint,
  `nrt_rule_flag65` tinyint,
  `nrt_rule_flag66` tinyint,
  `nrt_rule_flag67` tinyint,
  `nrt_rule_flag68` tinyint,
  `nrt_rule_flag69` tinyint,
  `nrt_rule_flag70` tinyint,
  `nrt_rule_flag71` tinyint,
  `nrt_rule_flag72` tinyint,
  `nrt_rule_flag73` tinyint,
  `nrt_rule_flag74` tinyint,
  `nrt_rule_flag75` tinyint,
  `nrt_rule_flag76` tinyint,
  `nrt_rule_flag77` tinyint,
  `nrt_rule_flag78` tinyint,
  `nrt_rule_flag79` tinyint,
  `nrt_rule_flag80` tinyint)
PARTITIONED BY (
  `click_dt` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'viewfs://apollo-rno/apps/b_marketing_tracking/chocolate/epnnrt_v2/click'
TBLPROPERTIES (
  'transient_lastDdlTime'='1601444855')