insert into choco_data.epnnrt_click_automation_diff_tmp
select
    new.imprsn_cntnr_id           as new_imprsn_cntnr_id           , old.imprsn_cntnr_id           as old_imprsn_cntnr_id           ,
    new.file_schm_vrsn_num        as new_file_schm_vrsn_num        , old.file_schm_vrsn_num        as old_file_schm_vrsn_num        ,
    new.file_id                   as new_file_id                   , old.file_id                   as old_file_id                   ,
    new.batch_id                  as new_batch_id                  , old.batch_id                  as old_batch_id                  ,
    new.click_id                  as new_click_id                  , old.click_id                  as old_click_id                  ,
    new.chnl_id                   as new_chnl_id                   , old.chnl_id                   as old_chnl_id                   ,
    new.crltn_guid_txt            as new_crltn_guid_txt            , old.crltn_guid_txt            as old_crltn_guid_txt            ,
    new.guid_txt                  as new_guid_txt                  , old.guid_txt                  as old_guid_txt                  ,
    new.user_id                   as new_user_id                   , old.user_id                   as old_user_id                   ,
    new.clnt_rmt_ip               as new_clnt_rmt_ip               , old.clnt_rmt_ip               as old_clnt_rmt_ip               ,
    new.brwsr_type_num            as new_brwsr_type_num            , old.brwsr_type_num            as old_brwsr_type_num            ,
    new.brwsr_name                as new_brwsr_name                , old.brwsr_name                as old_brwsr_name                ,
    new.rfr_url_name              as new_rfr_url_name              , old.rfr_url_name              as old_rfr_url_name              ,
    new.encryptd_ind              as new_encryptd_ind              , old.encryptd_ind              as old_encryptd_ind              ,
    new.plcmnt_data_txt           as new_plcmnt_data_txt           , old.plcmnt_data_txt           as old_plcmnt_data_txt           ,
    new.pblshr_id                 as new_pblshr_id                 , old.pblshr_id                 as old_pblshr_id                 ,
    new.ams_pblshr_cmpgn_id       as new_ams_pblshr_cmpgn_id       , old.ams_pblshr_cmpgn_id       as old_ams_pblshr_cmpgn_id       ,
    new.ams_tool_id               as new_ams_tool_id               , old.ams_tool_id               as old_ams_tool_id               ,
    new.cstm_id                   as new_cstm_id                   , old.cstm_id                   as old_cstm_id                   ,
    new.lndng_page_url_name       as new_lndng_page_url_name       , old.lndng_page_url_name       as old_lndng_page_url_name       ,
    new.user_query_txt            as new_user_query_txt            , old.user_query_txt            as old_user_query_txt            ,
    new.flex_fld_vrsn_num         as new_flex_fld_vrsn_num         , old.flex_fld_vrsn_num         as old_flex_fld_vrsn_num         ,
    new.flex_fld_1_txt            as new_flex_fld_1_txt            , old.flex_fld_1_txt            as old_flex_fld_1_txt            ,
    new.flex_fld_2_txt            as new_flex_fld_2_txt            , old.flex_fld_2_txt            as old_flex_fld_2_txt            ,
    new.flex_fld_3_txt            as new_flex_fld_3_txt            , old.flex_fld_3_txt            as old_flex_fld_3_txt            ,
    new.flex_fld_4_txt            as new_flex_fld_4_txt            , old.flex_fld_4_txt            as old_flex_fld_4_txt            ,
    new.imprsn_ts                 as new_imprsn_ts                 , old.imprsn_ts                 as old_imprsn_ts                 ,
    new.click_ts                  as new_click_ts                  , old.click_ts                  as old_click_ts                  ,
    new.last_adn_click_id         as new_last_adn_click_id         , old.last_adn_click_id         as old_last_adn_click_id         ,
    new.last_adn_click_ts         as new_last_adn_click_ts         , old.last_adn_click_ts         as old_last_adn_click_ts         ,
    new.flex_fld_5_txt            as new_flex_fld_5_txt            , old.flex_fld_5_txt            as old_flex_fld_5_txt            ,
    new.flex_fld_6_txt            as new_flex_fld_6_txt            , old.flex_fld_6_txt            as old_flex_fld_6_txt            ,
    new.flex_fld_7_txt            as new_flex_fld_7_txt            , old.flex_fld_7_txt            as old_flex_fld_7_txt            ,
    new.flex_fld_8_txt            as new_flex_fld_8_txt            , old.flex_fld_8_txt            as old_flex_fld_8_txt            ,
    new.flex_fld_9_txt            as new_flex_fld_9_txt            , old.flex_fld_9_txt            as old_flex_fld_9_txt            ,
    new.flex_fld_10_txt           as new_flex_fld_10_txt           , old.flex_fld_10_txt           as old_flex_fld_10_txt           ,
    new.flex_fld_11_txt           as new_flex_fld_11_txt           , old.flex_fld_11_txt           as old_flex_fld_11_txt           ,
    new.flex_fld_12_txt           as new_flex_fld_12_txt           , old.flex_fld_12_txt           as old_flex_fld_12_txt           ,
    new.flex_fld_13_txt           as new_flex_fld_13_txt           , old.flex_fld_13_txt           as old_flex_fld_13_txt           ,
    new.flex_fld_14_txt           as new_flex_fld_14_txt           , old.flex_fld_14_txt           as old_flex_fld_14_txt           ,
    new.flex_fld_15_txt           as new_flex_fld_15_txt           , old.flex_fld_15_txt           as old_flex_fld_15_txt           ,
    new.flex_fld_16_txt           as new_flex_fld_16_txt           , old.flex_fld_16_txt           as old_flex_fld_16_txt           ,
    new.flex_fld_17_txt           as new_flex_fld_17_txt           , old.flex_fld_17_txt           as old_flex_fld_17_txt           ,
    new.flex_fld_18_txt           as new_flex_fld_18_txt           , old.flex_fld_18_txt           as old_flex_fld_18_txt           ,
    new.flex_fld_19_txt           as new_flex_fld_19_txt           , old.flex_fld_19_txt           as old_flex_fld_19_txt           ,
    new.flex_fld_20_txt           as new_flex_fld_20_txt           , old.flex_fld_20_txt           as old_flex_fld_20_txt           ,
    new.icep_flex_fld_vrsn_id     as new_icep_flex_fld_vrsn_id     , old.icep_flex_fld_vrsn_id     as old_icep_flex_fld_vrsn_id     ,
    new.icep_flex_fld_1_txt       as new_icep_flex_fld_1_txt       , old.icep_flex_fld_1_txt       as old_icep_flex_fld_1_txt       ,
    new.icep_flex_fld_2_txt       as new_icep_flex_fld_2_txt       , old.icep_flex_fld_2_txt       as old_icep_flex_fld_2_txt       ,
    new.icep_flex_fld_3_txt       as new_icep_flex_fld_3_txt       , old.icep_flex_fld_3_txt       as old_icep_flex_fld_3_txt       ,
    new.icep_flex_fld_4_txt       as new_icep_flex_fld_4_txt       , old.icep_flex_fld_4_txt       as old_icep_flex_fld_4_txt       ,
    new.icep_flex_fld_5_txt       as new_icep_flex_fld_5_txt       , old.icep_flex_fld_5_txt       as old_icep_flex_fld_5_txt       ,
    new.icep_flex_fld_6_txt       as new_icep_flex_fld_6_txt       , old.icep_flex_fld_6_txt       as old_icep_flex_fld_6_txt       ,
    new.ams_prgrm_id              as new_ams_prgrm_id              , old.ams_prgrm_id              as old_ams_prgrm_id              ,
    new.advrtsr_id                as new_advrtsr_id                , old.advrtsr_id                as old_advrtsr_id                ,
    new.ams_click_fltr_type_id    as new_ams_click_fltr_type_id    , old.ams_click_fltr_type_id    as old_ams_click_fltr_type_id    ,
    new.imprsn_loose_match_ind    as new_imprsn_loose_match_ind    , old.imprsn_loose_match_ind    as old_imprsn_loose_match_ind    ,
    new.fltr_yn_ind               as new_fltr_yn_ind               , old.fltr_yn_ind               as old_fltr_yn_ind               ,
    new.ams_trans_rsn_cd          as new_ams_trans_rsn_cd          , old.ams_trans_rsn_cd          as old_ams_trans_rsn_cd          ,
    new.ams_page_type_map_id      as new_ams_page_type_map_id      , old.ams_page_type_map_id      as old_ams_page_type_map_id      ,
    new.rfrng_dmn_name            as new_rfrng_dmn_name            , old.rfrng_dmn_name            as old_rfrng_dmn_name            ,
    new.tfs_rfrng_dmn_name        as new_tfs_rfrng_dmn_name        , old.tfs_rfrng_dmn_name        as old_tfs_rfrng_dmn_name        ,
    new.geo_trgtd_rsn_cd          as new_geo_trgtd_rsn_cd          , old.geo_trgtd_rsn_cd          as old_geo_trgtd_rsn_cd          ,
    new.src_plcmnt_data_txt       as new_src_plcmnt_data_txt       , old.src_plcmnt_data_txt       as old_src_plcmnt_data_txt       ,
    new.geo_trgtd_cntry_cd        as new_geo_trgtd_cntry_cd        , old.geo_trgtd_cntry_cd        as old_geo_trgtd_cntry_cd        ,
    new.tool_lvl_optn_ind         as new_tool_lvl_optn_ind         , old.tool_lvl_optn_ind         as old_tool_lvl_optn_ind         ,
    new.acnt_lvl_optn_ind         as new_acnt_lvl_optn_ind         , old.acnt_lvl_optn_ind         as old_acnt_lvl_optn_ind         ,
    new.geo_trgtd_ind             as new_geo_trgtd_ind             , old.geo_trgtd_ind             as old_geo_trgtd_ind             ,
    new.pblshr_acptd_prgrm_ind    as new_pblshr_acptd_prgrm_ind    , old.pblshr_acptd_prgrm_ind    as old_pblshr_acptd_prgrm_ind    ,
    new.incmng_click_url_vctr_id  as new_incmng_click_url_vctr_id  , old.incmng_click_url_vctr_id  as old_incmng_click_url_vctr_id  ,
    new.str_name_txt              as new_str_name_txt              , old.str_name_txt              as old_str_name_txt              ,
    new.item_id                   as new_item_id                   , old.item_id                   as old_item_id                   ,
    new.ctgry_id                  as new_ctgry_id                  , old.ctgry_id                  as old_ctgry_id                  ,
    new.keyword_txt               as new_keyword_txt               , old.keyword_txt               as old_keyword_txt               ,
    new.prgrm_excptn_list_ind     as new_prgrm_excptn_list_ind     , old.prgrm_excptn_list_ind     as old_prgrm_excptn_list_ind     ,
    new.roi_fltr_yn_ind           as new_roi_fltr_yn_ind           , old.roi_fltr_yn_ind           as old_roi_fltr_yn_ind           ,
    new.seller_name               as new_seller_name               , old.seller_name               as old_seller_name               ,
    new.rover_url_txt             as new_rover_url_txt             , old.rover_url_txt             as old_rover_url_txt             ,
    new.mplx_timeout_flag         as new_mplx_timeout_flag         , old.mplx_timeout_flag         as old_mplx_timeout_flag         ,
    new.app_id                    as new_app_id                    , old.app_id                    as old_app_id                    ,
    new.app_package_name          as new_app_package_name          , old.app_package_name          as old_app_package_name          ,
    new.app_name                  as new_app_name                  , old.app_name                  as old_app_name                  ,
    new.app_version               as new_app_version               , old.app_version               as old_app_version               ,
    new.device_name               as new_device_name               , old.device_name               as old_device_name               ,
    new.os_name                   as new_os_name                   , old.os_name                   as old_os_name                   ,
    new.os_version                as new_os_version                , old.os_version                as old_os_version                ,
    new.udid                      as new_udid                      , old.udid                      as old_udid                      ,
    new.sdk_name                  as new_sdk_name                  , old.sdk_name                  as old_sdk_name                  ,
    new.sdk_version               as new_sdk_version               , old.sdk_version               as old_sdk_version               ,
    new.trfc_src_cd               as new_trfc_src_cd               , old.trfc_src_cd               as old_trfc_src_cd               ,
    new.roi_rule_values           as new_roi_rule_values           , old.roi_rule_values           as old_roi_rule_values           ,
    new.rt_rule_flag1             as new_rt_rule_flag1             , old.rt_rule_flag1             as old_rt_rule_flag1             ,
    new.rt_rule_flag2             as new_rt_rule_flag2             , old.rt_rule_flag2             as old_rt_rule_flag2             ,
    new.rt_rule_flag3             as new_rt_rule_flag3             , old.rt_rule_flag3             as old_rt_rule_flag3             ,
    new.rt_rule_flag4             as new_rt_rule_flag4             , old.rt_rule_flag4             as old_rt_rule_flag4             ,
    new.rt_rule_flag5             as new_rt_rule_flag5             , old.rt_rule_flag5             as old_rt_rule_flag5             ,
    new.rt_rule_flag6             as new_rt_rule_flag6             , old.rt_rule_flag6             as old_rt_rule_flag6             ,
    new.rt_rule_flag7             as new_rt_rule_flag7             , old.rt_rule_flag7             as old_rt_rule_flag7             ,
    new.rt_rule_flag8             as new_rt_rule_flag8             , old.rt_rule_flag8             as old_rt_rule_flag8             ,
    new.rt_rule_flag9             as new_rt_rule_flag9             , old.rt_rule_flag9             as old_rt_rule_flag9             ,
    new.rt_rule_flag10            as new_rt_rule_flag10            , old.rt_rule_flag10            as old_rt_rule_flag10            ,
    new.rt_rule_flag11            as new_rt_rule_flag11            , old.rt_rule_flag11            as old_rt_rule_flag11            ,
    new.rt_rule_flag12            as new_rt_rule_flag12            , old.rt_rule_flag12            as old_rt_rule_flag12            ,
    new.rt_rule_flag13            as new_rt_rule_flag13            , old.rt_rule_flag13            as old_rt_rule_flag13            ,
    new.rt_rule_flag14            as new_rt_rule_flag14            , old.rt_rule_flag14            as old_rt_rule_flag14            ,
    new.rt_rule_flag15            as new_rt_rule_flag15            , old.rt_rule_flag15            as old_rt_rule_flag15            ,
    new.rt_rule_flag16            as new_rt_rule_flag16            , old.rt_rule_flag16            as old_rt_rule_flag16            ,
    new.rt_rule_flag17            as new_rt_rule_flag17            , old.rt_rule_flag17            as old_rt_rule_flag17            ,
    new.rt_rule_flag18            as new_rt_rule_flag18            , old.rt_rule_flag18            as old_rt_rule_flag18            ,
    new.rt_rule_flag19            as new_rt_rule_flag19            , old.rt_rule_flag19            as old_rt_rule_flag19            ,
    new.rt_rule_flag20            as new_rt_rule_flag20            , old.rt_rule_flag20            as old_rt_rule_flag20            ,
    new.rt_rule_flag21            as new_rt_rule_flag21            , old.rt_rule_flag21            as old_rt_rule_flag21            ,
    new.rt_rule_flag22            as new_rt_rule_flag22            , old.rt_rule_flag22            as old_rt_rule_flag22            ,
    new.rt_rule_flag23            as new_rt_rule_flag23            , old.rt_rule_flag23            as old_rt_rule_flag23            ,
    new.rt_rule_flag24            as new_rt_rule_flag24            , old.rt_rule_flag24            as old_rt_rule_flag24            ,
    new.nrt_rule_flag1            as new_nrt_rule_flag1            , old.nrt_rule_flag1            as old_nrt_rule_flag1            ,
    new.nrt_rule_flag2            as new_nrt_rule_flag2            , old.nrt_rule_flag2            as old_nrt_rule_flag2            ,
    new.nrt_rule_flag3            as new_nrt_rule_flag3            , old.nrt_rule_flag3            as old_nrt_rule_flag3            ,
    new.nrt_rule_flag4            as new_nrt_rule_flag4            , old.nrt_rule_flag4            as old_nrt_rule_flag4            ,
    new.nrt_rule_flag5            as new_nrt_rule_flag5            , old.nrt_rule_flag5            as old_nrt_rule_flag5            ,
    new.nrt_rule_flag6            as new_nrt_rule_flag6            , old.nrt_rule_flag6            as old_nrt_rule_flag6            ,
    new.nrt_rule_flag7            as new_nrt_rule_flag7            , old.nrt_rule_flag7            as old_nrt_rule_flag7            ,
    new.nrt_rule_flag8            as new_nrt_rule_flag8            , old.nrt_rule_flag8            as old_nrt_rule_flag8            ,
    new.nrt_rule_flag9            as new_nrt_rule_flag9            , old.nrt_rule_flag9            as old_nrt_rule_flag9            ,
    new.nrt_rule_flag10           as new_nrt_rule_flag10           , old.nrt_rule_flag10           as old_nrt_rule_flag10           ,
    new.nrt_rule_flag11           as new_nrt_rule_flag11           , old.nrt_rule_flag11           as old_nrt_rule_flag11           ,
    new.nrt_rule_flag12           as new_nrt_rule_flag12           , old.nrt_rule_flag12           as old_nrt_rule_flag12           ,
    new.nrt_rule_flag13           as new_nrt_rule_flag13           , old.nrt_rule_flag13           as old_nrt_rule_flag13           ,
    new.nrt_rule_flag14           as new_nrt_rule_flag14           , old.nrt_rule_flag14           as old_nrt_rule_flag14           ,
    new.nrt_rule_flag15           as new_nrt_rule_flag15           , old.nrt_rule_flag15           as old_nrt_rule_flag15           ,
    new.nrt_rule_flag16           as new_nrt_rule_flag16           , old.nrt_rule_flag16           as old_nrt_rule_flag16           ,
    new.nrt_rule_flag17           as new_nrt_rule_flag17           , old.nrt_rule_flag17           as old_nrt_rule_flag17           ,
    new.nrt_rule_flag18           as new_nrt_rule_flag18           , old.nrt_rule_flag18           as old_nrt_rule_flag18           ,
    new.nrt_rule_flag19           as new_nrt_rule_flag19           , old.nrt_rule_flag19           as old_nrt_rule_flag19           ,
    new.nrt_rule_flag20           as new_nrt_rule_flag20           , old.nrt_rule_flag20           as old_nrt_rule_flag20           ,
    new.nrt_rule_flag21           as new_nrt_rule_flag21           , old.nrt_rule_flag21           as old_nrt_rule_flag21           ,
    new.nrt_rule_flag22           as new_nrt_rule_flag22           , old.nrt_rule_flag22           as old_nrt_rule_flag22           ,
    new.nrt_rule_flag23           as new_nrt_rule_flag23           , old.nrt_rule_flag23           as old_nrt_rule_flag23           ,
    new.nrt_rule_flag24           as new_nrt_rule_flag24           , old.nrt_rule_flag24           as old_nrt_rule_flag24           ,
    new.nrt_rule_flag25           as new_nrt_rule_flag25           , old.nrt_rule_flag25           as old_nrt_rule_flag25           ,
    new.nrt_rule_flag26           as new_nrt_rule_flag26           , old.nrt_rule_flag26           as old_nrt_rule_flag26           ,
    new.nrt_rule_flag27           as new_nrt_rule_flag27           , old.nrt_rule_flag27           as old_nrt_rule_flag27           ,
    new.nrt_rule_flag28           as new_nrt_rule_flag28           , old.nrt_rule_flag28           as old_nrt_rule_flag28           ,
    new.nrt_rule_flag29           as new_nrt_rule_flag29           , old.nrt_rule_flag29           as old_nrt_rule_flag29           ,
    new.nrt_rule_flag30           as new_nrt_rule_flag30           , old.nrt_rule_flag30           as old_nrt_rule_flag30           ,
    new.nrt_rule_flag31           as new_nrt_rule_flag31           , old.nrt_rule_flag31           as old_nrt_rule_flag31           ,
    new.nrt_rule_flag32           as new_nrt_rule_flag32           , old.nrt_rule_flag32           as old_nrt_rule_flag32           ,
    new.nrt_rule_flag33           as new_nrt_rule_flag33           , old.nrt_rule_flag33           as old_nrt_rule_flag33           ,
    new.nrt_rule_flag34           as new_nrt_rule_flag34           , old.nrt_rule_flag34           as old_nrt_rule_flag34           ,
    new.nrt_rule_flag35           as new_nrt_rule_flag35           , old.nrt_rule_flag35           as old_nrt_rule_flag35           ,
    new.nrt_rule_flag36           as new_nrt_rule_flag36           , old.nrt_rule_flag36           as old_nrt_rule_flag36           ,
    new.nrt_rule_flag37           as new_nrt_rule_flag37           , old.nrt_rule_flag37           as old_nrt_rule_flag37           ,
    new.nrt_rule_flag38           as new_nrt_rule_flag38           , old.nrt_rule_flag38           as old_nrt_rule_flag38           ,
    new.nrt_rule_flag39           as new_nrt_rule_flag39           , old.nrt_rule_flag39           as old_nrt_rule_flag39           ,
    new.nrt_rule_flag40           as new_nrt_rule_flag40           , old.nrt_rule_flag40           as old_nrt_rule_flag40           ,
    new.nrt_rule_flag41           as new_nrt_rule_flag41           , old.nrt_rule_flag41           as old_nrt_rule_flag41           ,
    new.nrt_rule_flag42           as new_nrt_rule_flag42           , old.nrt_rule_flag42           as old_nrt_rule_flag42           ,
    new.nrt_rule_flag43           as new_nrt_rule_flag43           , old.nrt_rule_flag43           as old_nrt_rule_flag43           ,
    new.nrt_rule_flag44           as new_nrt_rule_flag44           , old.nrt_rule_flag44           as old_nrt_rule_flag44           ,
    new.nrt_rule_flag45           as new_nrt_rule_flag45           , old.nrt_rule_flag45           as old_nrt_rule_flag45           ,
    new.nrt_rule_flag46           as new_nrt_rule_flag46           , old.nrt_rule_flag46           as old_nrt_rule_flag46           ,
    new.nrt_rule_flag47           as new_nrt_rule_flag47           , old.nrt_rule_flag47           as old_nrt_rule_flag47           ,
    new.nrt_rule_flag48           as new_nrt_rule_flag48           , old.nrt_rule_flag48           as old_nrt_rule_flag48           ,
    new.nrt_rule_flag49           as new_nrt_rule_flag49           , old.nrt_rule_flag49           as old_nrt_rule_flag49           ,
    new.nrt_rule_flag50           as new_nrt_rule_flag50           , old.nrt_rule_flag50           as old_nrt_rule_flag50           ,
    new.nrt_rule_flag51           as new_nrt_rule_flag51           , old.nrt_rule_flag51           as old_nrt_rule_flag51           ,
    new.nrt_rule_flag52           as new_nrt_rule_flag52           , old.nrt_rule_flag52           as old_nrt_rule_flag52           ,
    new.nrt_rule_flag53           as new_nrt_rule_flag53           , old.nrt_rule_flag53           as old_nrt_rule_flag53           ,
    new.nrt_rule_flag54           as new_nrt_rule_flag54           , old.nrt_rule_flag54           as old_nrt_rule_flag54           ,
    new.nrt_rule_flag55           as new_nrt_rule_flag55           , old.nrt_rule_flag55           as old_nrt_rule_flag55           ,
    new.nrt_rule_flag56           as new_nrt_rule_flag56           , old.nrt_rule_flag56           as old_nrt_rule_flag56           ,
    new.nrt_rule_flag57           as new_nrt_rule_flag57           , old.nrt_rule_flag57           as old_nrt_rule_flag57           ,
    new.nrt_rule_flag58           as new_nrt_rule_flag58           , old.nrt_rule_flag58           as old_nrt_rule_flag58           ,
    new.nrt_rule_flag59           as new_nrt_rule_flag59           , old.nrt_rule_flag59           as old_nrt_rule_flag59           ,
    new.nrt_rule_flag60           as new_nrt_rule_flag60           , old.nrt_rule_flag60           as old_nrt_rule_flag60           ,
    new.nrt_rule_flag61           as new_nrt_rule_flag61           , old.nrt_rule_flag61           as old_nrt_rule_flag61           ,
    new.nrt_rule_flag62           as new_nrt_rule_flag62           , old.nrt_rule_flag62           as old_nrt_rule_flag62           ,
    new.nrt_rule_flag63           as new_nrt_rule_flag63           , old.nrt_rule_flag63           as old_nrt_rule_flag63           ,
    new.nrt_rule_flag64           as new_nrt_rule_flag64           , old.nrt_rule_flag64           as old_nrt_rule_flag64           ,
    new.nrt_rule_flag65           as new_nrt_rule_flag65           , old.nrt_rule_flag65           as old_nrt_rule_flag65           ,
    new.nrt_rule_flag66           as new_nrt_rule_flag66           , old.nrt_rule_flag66           as old_nrt_rule_flag66           ,
    new.nrt_rule_flag67           as new_nrt_rule_flag67           , old.nrt_rule_flag67           as old_nrt_rule_flag67           ,
    new.nrt_rule_flag68           as new_nrt_rule_flag68           , old.nrt_rule_flag68           as old_nrt_rule_flag68           ,
    new.nrt_rule_flag69           as new_nrt_rule_flag69           , old.nrt_rule_flag69           as old_nrt_rule_flag69           ,
    new.nrt_rule_flag70           as new_nrt_rule_flag70           , old.nrt_rule_flag70           as old_nrt_rule_flag70           ,
    new.nrt_rule_flag71           as new_nrt_rule_flag71           , old.nrt_rule_flag71           as old_nrt_rule_flag71           ,
    new.nrt_rule_flag72           as new_nrt_rule_flag72           , old.nrt_rule_flag72           as old_nrt_rule_flag72           ,
    new.nrt_rule_flag73           as new_nrt_rule_flag73           , old.nrt_rule_flag73           as old_nrt_rule_flag73           ,
    new.nrt_rule_flag74           as new_nrt_rule_flag74           , old.nrt_rule_flag74           as old_nrt_rule_flag74           ,
    new.nrt_rule_flag75           as new_nrt_rule_flag75           , old.nrt_rule_flag75           as old_nrt_rule_flag75           ,
    new.nrt_rule_flag76           as new_nrt_rule_flag76           , old.nrt_rule_flag76           as old_nrt_rule_flag76           ,
    new.nrt_rule_flag77           as new_nrt_rule_flag77           , old.nrt_rule_flag77           as old_nrt_rule_flag77           ,
    new.nrt_rule_flag78           as new_nrt_rule_flag78           , old.nrt_rule_flag78           as old_nrt_rule_flag78           ,
    new.nrt_rule_flag79           as new_nrt_rule_flag79           , old.nrt_rule_flag79           as old_nrt_rule_flag79           ,
    new.nrt_rule_flag80           as new_nrt_rule_flag80           , old.nrt_rule_flag80           as old_nrt_rule_flag80
FROM (select * from choco_data.ams_click_new_test where click_dt='#{click_dt}' and click_id is not  null) new
         LEFT outer join (select * from choco_data.ams_click_old_test where click_dt='#{click_dt}' and click_id is not  null) old
                         on  old.click_id = new.click_id and old.guid_txt=new.guid_txt and new.user_id=old.user_id and new.ams_trans_rsn_cd = old.ams_trans_rsn_cd where  !(
            length(trim(old.rover_url_txt))<=>0 or trim(old.rover_url_txt)<=>'0' or (

            (old.imprsn_cntnr_id           is null or new.imprsn_cntnr_id            <=>     old.imprsn_cntnr_id           ) and
            (old.file_schm_vrsn_num        is null or new.file_schm_vrsn_num         <=>     old.file_schm_vrsn_num        ) and
            (old.file_id                   is null or new.file_id                    <=>     old.file_id                   ) and
            (old.batch_id                  is null or new.batch_id                   <=>     old.batch_id                  ) and
            (old.click_id                  is null or new.click_id                   <=>     old.click_id                  ) and
            (old.chnl_id                   is null or new.chnl_id                    <=>     old.chnl_id                   ) and
            (old.crltn_guid_txt            is null or new.crltn_guid_txt             <=>     old.crltn_guid_txt            ) and
            (old.guid_txt                  is null or new.guid_txt                   <=>     old.guid_txt                  ) and
            (old.user_id                   is null or new.user_id                    <=>     old.user_id                   ) and
            (old.clnt_rmt_ip               is null or new.clnt_rmt_ip                <=>     old.clnt_rmt_ip               ) and
            (old.brwsr_type_num            is null or new.brwsr_type_num             <=>     old.brwsr_type_num            ) and
            (old.brwsr_name                is null or new.brwsr_name                 <=>     old.brwsr_name                ) and
            (old.rfr_url_name              is null or new.rfr_url_name               <=>     old.rfr_url_name              ) and
            (old.encryptd_ind              is null or new.encryptd_ind               <=>     old.encryptd_ind              ) and
            (old.plcmnt_data_txt           is null or new.plcmnt_data_txt            <=>     old.plcmnt_data_txt           ) and
            (old.pblshr_id                 is null or new.pblshr_id                  <=>     old.pblshr_id                 ) and
            (old.ams_pblshr_cmpgn_id       is null or new.ams_pblshr_cmpgn_id        <=>     old.ams_pblshr_cmpgn_id       ) and
            (old.ams_tool_id               is null or new.ams_tool_id                <=>     old.ams_tool_id               ) and
            (old.cstm_id                   is null or new.cstm_id                    <=>     old.cstm_id                   ) and
            (old.lndng_page_url_name       is null or new.lndng_page_url_name        <=>     old.lndng_page_url_name       ) and
            (old.user_query_txt            is null or new.user_query_txt             <=>     old.user_query_txt            ) and
            (old.flex_fld_vrsn_num         is null or new.flex_fld_vrsn_num          <=>     old.flex_fld_vrsn_num         ) and
            (old.imprsn_ts                 is null or new.imprsn_ts                  <=>     old.imprsn_ts                 ) and
            (old.click_ts                  is null or new.click_ts                   <=>     old.click_ts                  ) and
            (old.last_adn_click_id         is null or new.last_adn_click_id          <=>     old.last_adn_click_id         ) and
            (old.last_adn_click_ts         is null or new.last_adn_click_ts          <=>     old.last_adn_click_ts         ) and
            (old.flex_fld_5_txt            is null or new.flex_fld_5_txt             <=>     old.flex_fld_5_txt            ) and
            (old.flex_fld_6_txt            is null or new.flex_fld_6_txt             <=>     old.flex_fld_6_txt            ) and
            (old.flex_fld_7_txt            is null or new.flex_fld_7_txt             <=>     old.flex_fld_7_txt            ) and
            (old.flex_fld_8_txt            is null or new.flex_fld_8_txt             <=>     old.flex_fld_8_txt            ) and
            (old.flex_fld_9_txt            is null or new.flex_fld_9_txt             <=>     old.flex_fld_9_txt            ) and
            (old.flex_fld_10_txt           is null or new.flex_fld_10_txt            <=>     old.flex_fld_10_txt           ) and
            (old.flex_fld_11_txt           is null or new.flex_fld_11_txt            <=>     old.flex_fld_11_txt           ) and
            (old.flex_fld_12_txt           is null or new.flex_fld_12_txt            <=>     old.flex_fld_12_txt           ) and
            (old.flex_fld_13_txt           is null or new.flex_fld_13_txt            <=>     old.flex_fld_13_txt           ) and
            (old.flex_fld_14_txt           is null or new.flex_fld_14_txt            <=>     old.flex_fld_14_txt           ) and
            (old.flex_fld_15_txt           is null or new.flex_fld_15_txt            <=>     old.flex_fld_15_txt           ) and
            (old.flex_fld_16_txt           is null or new.flex_fld_16_txt            <=>     old.flex_fld_16_txt           ) and
            (old.flex_fld_17_txt           is null or new.flex_fld_17_txt            <=>     old.flex_fld_17_txt           ) and
            (old.flex_fld_18_txt           is null or new.flex_fld_18_txt            <=>     old.flex_fld_18_txt           ) and
            (old.flex_fld_19_txt           is null or new.flex_fld_19_txt            <=>     old.flex_fld_19_txt           ) and
            (old.flex_fld_20_txt           is null or new.flex_fld_20_txt            <=>     old.flex_fld_20_txt           ) and
            (old.icep_flex_fld_vrsn_id     is null or new.icep_flex_fld_vrsn_id      <=>     old.icep_flex_fld_vrsn_id     ) and
            (old.icep_flex_fld_1_txt       is null or new.icep_flex_fld_1_txt        <=>     old.icep_flex_fld_1_txt       ) and
            (old.icep_flex_fld_2_txt       is null or new.icep_flex_fld_2_txt        <=>     old.icep_flex_fld_2_txt       ) and
            (old.icep_flex_fld_3_txt       is null or new.icep_flex_fld_3_txt        <=>     old.icep_flex_fld_3_txt       ) and
            (old.icep_flex_fld_4_txt       is null or new.icep_flex_fld_4_txt        <=>     old.icep_flex_fld_4_txt       ) and
            (old.icep_flex_fld_5_txt       is null or new.icep_flex_fld_5_txt        <=>     old.icep_flex_fld_5_txt       ) and
            (old.icep_flex_fld_6_txt       is null or new.icep_flex_fld_6_txt        <=>     old.icep_flex_fld_6_txt       ) and
            (old.ams_prgrm_id              is null or new.ams_prgrm_id               <=>     old.ams_prgrm_id              ) and
            (old.advrtsr_id                is null or new.advrtsr_id                 <=>     old.advrtsr_id                ) and
            (old.ams_click_fltr_type_id    is null or new.ams_click_fltr_type_id     <=>     old.ams_click_fltr_type_id    ) and
            (old.imprsn_loose_match_ind    is null or new.imprsn_loose_match_ind     <=>     old.imprsn_loose_match_ind    ) and
            (old.fltr_yn_ind               is null or new.fltr_yn_ind                <=>     old.fltr_yn_ind               ) and
            (old.ams_trans_rsn_cd          is null or new.ams_trans_rsn_cd           <=>     old.ams_trans_rsn_cd          ) and
            (old.ams_page_type_map_id      is null or new.ams_page_type_map_id       <=>     old.ams_page_type_map_id      ) and
            (old.rfrng_dmn_name            is null or new.rfrng_dmn_name             <=>     old.rfrng_dmn_name            ) and
            (old.tfs_rfrng_dmn_name        is null or new.tfs_rfrng_dmn_name         <=>     old.tfs_rfrng_dmn_name        ) and
            (old.geo_trgtd_rsn_cd          is null or new.geo_trgtd_rsn_cd           <=>     old.geo_trgtd_rsn_cd          ) and
            (old.src_plcmnt_data_txt       is null or new.src_plcmnt_data_txt        <=>     old.src_plcmnt_data_txt       ) and
            (old.geo_trgtd_cntry_cd        is null or new.geo_trgtd_cntry_cd         <=>     old.geo_trgtd_cntry_cd        ) and
            (old.tool_lvl_optn_ind         is null or new.tool_lvl_optn_ind          <=>     old.tool_lvl_optn_ind         ) and
            (old.acnt_lvl_optn_ind         is null or new.acnt_lvl_optn_ind          <=>     old.acnt_lvl_optn_ind         ) and
            (old.geo_trgtd_ind             is null or new.geo_trgtd_ind              <=>     old.geo_trgtd_ind             ) and
            (old.pblshr_acptd_prgrm_ind    is null or new.pblshr_acptd_prgrm_ind     <=>     old.pblshr_acptd_prgrm_ind    ) and
            (old.incmng_click_url_vctr_id  is null or new.incmng_click_url_vctr_id   <=>     old.incmng_click_url_vctr_id  ) and
            (old.str_name_txt              is null or new.str_name_txt               <=>     old.str_name_txt              ) and
            (old.item_id                   is null or new.item_id                    <=>     old.item_id                   ) and
            (old.ctgry_id                  is null or new.ctgry_id                   <=>     old.ctgry_id                  ) and
            (old.keyword_txt               is null or new.keyword_txt                <=>     old.keyword_txt               ) and
            (old.prgrm_excptn_list_ind     is null or new.prgrm_excptn_list_ind      <=>     old.prgrm_excptn_list_ind     ) and
            (old.roi_fltr_yn_ind           is null or new.roi_fltr_yn_ind            <=>     old.roi_fltr_yn_ind           ) and
            (old.seller_name               is null or new.seller_name                <=>     old.seller_name               ) and
            (old.rover_url_txt             is null or new.rover_url_txt              <=>     old.rover_url_txt             ) and
            (old.mplx_timeout_flag         is null or new.mplx_timeout_flag          <=>     old.mplx_timeout_flag         ) and
            (old.app_id                    is null or new.app_id                     <=>     old.app_id                    ) and
            (old.app_package_name          is null or new.app_package_name           <=>     old.app_package_name          ) and
            (old.app_name                  is null or new.app_name                   <=>     old.app_name                  ) and
            (old.app_version               is null or new.app_version                <=>     old.app_version               ) and
            (old.device_name               is null or new.device_name                <=>     old.device_name               ) and
            (old.os_name                   is null or new.os_name                    <=>     old.os_name                   ) and
            (old.os_version                is null or new.os_version                 <=>     old.os_version                ) and
            (old.udid                      is null or new.udid                       <=>     old.udid                      ) and
            (old.sdk_name                  is null or new.sdk_name                   <=>     old.sdk_name                  ) and
            (old.sdk_version               is null or new.sdk_version                <=>     old.sdk_version               ) and
            (old.trfc_src_cd               is null or new.trfc_src_cd                <=>     old.trfc_src_cd               ) and
            (old.roi_rule_values           is null or new.roi_rule_values            <=>     old.roi_rule_values           ) and
            (old.rt_rule_flag1             is null or new.rt_rule_flag1              <=>     old.rt_rule_flag1             ) and
            (old.rt_rule_flag2             is null or new.rt_rule_flag2              <=>     old.rt_rule_flag2             ) and
            (old.rt_rule_flag3             is null or new.rt_rule_flag3              <=>     old.rt_rule_flag3             ) and
            (old.rt_rule_flag4             is null or new.rt_rule_flag4              <=>     old.rt_rule_flag4             ) and
            (old.rt_rule_flag5             is null or new.rt_rule_flag5              <=>     old.rt_rule_flag5             ) and
            (old.rt_rule_flag6             is null or new.rt_rule_flag6              <=>     old.rt_rule_flag6             ) and
            (old.rt_rule_flag7             is null or new.rt_rule_flag7              <=>     old.rt_rule_flag7             ) and
            (old.rt_rule_flag8             is null or new.rt_rule_flag8              <=>     old.rt_rule_flag8             ) and
            (old.rt_rule_flag9             is null or new.rt_rule_flag9              <=>     old.rt_rule_flag9             ) and
            (old.rt_rule_flag10            is null or new.rt_rule_flag10             <=>     old.rt_rule_flag10            ) and
            (old.rt_rule_flag11            is null or new.rt_rule_flag11             <=>     old.rt_rule_flag11            ) and
            (old.rt_rule_flag12            is null or new.rt_rule_flag12             <=>     old.rt_rule_flag12            ) and
            (old.rt_rule_flag13            is null or new.rt_rule_flag13             <=>     old.rt_rule_flag13            ) and
            (old.rt_rule_flag14            is null or new.rt_rule_flag14             <=>     old.rt_rule_flag14            ) and
            (old.rt_rule_flag15            is null or new.rt_rule_flag15             <=>     old.rt_rule_flag15            ) and
            (old.rt_rule_flag16            is null or new.rt_rule_flag16             <=>     old.rt_rule_flag16            ) and
            (old.rt_rule_flag17            is null or new.rt_rule_flag17             <=>     old.rt_rule_flag17            ) and
            (old.rt_rule_flag18            is null or new.rt_rule_flag18             <=>     old.rt_rule_flag18            ) and
            (old.rt_rule_flag19            is null or new.rt_rule_flag19             <=>     old.rt_rule_flag19            ) and
            (old.rt_rule_flag20            is null or new.rt_rule_flag20             <=>     old.rt_rule_flag20            ) and
            (old.rt_rule_flag21            is null or new.rt_rule_flag21             <=>     old.rt_rule_flag21            ) and
            (old.rt_rule_flag22            is null or new.rt_rule_flag22             <=>     old.rt_rule_flag22            ) and
            (old.rt_rule_flag23            is null or new.rt_rule_flag23             <=>     old.rt_rule_flag23            ) and
            (old.rt_rule_flag24            is null or new.rt_rule_flag24             <=>     old.rt_rule_flag24            ) and
            (old.nrt_rule_flag1            is null or new.nrt_rule_flag1             <=>     old.nrt_rule_flag1            ) and
            (old.nrt_rule_flag2            is null or new.nrt_rule_flag2             <=>     old.nrt_rule_flag2            ) and
            (old.nrt_rule_flag3            is null or new.nrt_rule_flag3             <=>     old.nrt_rule_flag3            ) and
            (old.nrt_rule_flag4            is null or new.nrt_rule_flag4             <=>     old.nrt_rule_flag4            ) and
            (old.nrt_rule_flag5            is null or new.nrt_rule_flag5             <=>     old.nrt_rule_flag5            ) and
            (old.nrt_rule_flag6            is null or new.nrt_rule_flag6             <=>     old.nrt_rule_flag6            ) and
            (old.nrt_rule_flag7            is null or new.nrt_rule_flag7             <=>     old.nrt_rule_flag7            ) and
            (old.nrt_rule_flag8            is null or new.nrt_rule_flag8             <=>     old.nrt_rule_flag8            ) and
            (old.nrt_rule_flag9            is null or new.nrt_rule_flag9             <=>     old.nrt_rule_flag9            ) and
            (old.nrt_rule_flag10           is null or new.nrt_rule_flag10            <=>     old.nrt_rule_flag10           ) and
            (old.nrt_rule_flag11           is null or new.nrt_rule_flag11            <=>     old.nrt_rule_flag11           ) and
            (old.nrt_rule_flag12           is null or new.nrt_rule_flag12            <=>     old.nrt_rule_flag12           ) and
            (old.nrt_rule_flag13           is null or new.nrt_rule_flag13            <=>     old.nrt_rule_flag13           ) and
            (old.nrt_rule_flag14           is null or new.nrt_rule_flag14            <=>     old.nrt_rule_flag14           ) and
            (old.nrt_rule_flag15           is null or new.nrt_rule_flag15            <=>     old.nrt_rule_flag15           ) and
            (old.nrt_rule_flag16           is null or new.nrt_rule_flag16            <=>     old.nrt_rule_flag16           ) and
            (old.nrt_rule_flag17           is null or new.nrt_rule_flag17            <=>     old.nrt_rule_flag17           ) and
            (old.nrt_rule_flag18           is null or new.nrt_rule_flag18            <=>     old.nrt_rule_flag18           ) and
            (old.nrt_rule_flag19           is null or new.nrt_rule_flag19            <=>     old.nrt_rule_flag19           ) and
            (old.nrt_rule_flag20           is null or new.nrt_rule_flag20            <=>     old.nrt_rule_flag20           ) and
            (old.nrt_rule_flag21           is null or new.nrt_rule_flag21            <=>     old.nrt_rule_flag21           ) and
            (old.nrt_rule_flag22           is null or new.nrt_rule_flag22            <=>     old.nrt_rule_flag22           ) and
            (old.nrt_rule_flag23           is null or new.nrt_rule_flag23            <=>     old.nrt_rule_flag23           ) and
            (old.nrt_rule_flag24           is null or new.nrt_rule_flag24            <=>     old.nrt_rule_flag24           ) and
            (old.nrt_rule_flag25           is null or new.nrt_rule_flag25            <=>     old.nrt_rule_flag25           ) and
            (old.nrt_rule_flag26           is null or new.nrt_rule_flag26            <=>     old.nrt_rule_flag26           ) and
            (old.nrt_rule_flag27           is null or new.nrt_rule_flag27            <=>     old.nrt_rule_flag27           ) and
            (old.nrt_rule_flag28           is null or new.nrt_rule_flag28            <=>     old.nrt_rule_flag28           ) and
            (old.nrt_rule_flag29           is null or new.nrt_rule_flag29            <=>     old.nrt_rule_flag29           ) and
            (old.nrt_rule_flag30           is null or new.nrt_rule_flag30            <=>     old.nrt_rule_flag30           ) and
            (old.nrt_rule_flag31           is null or new.nrt_rule_flag31            <=>     old.nrt_rule_flag31           ) and
            (old.nrt_rule_flag32           is null or new.nrt_rule_flag32            <=>     old.nrt_rule_flag32           ) and
            (old.nrt_rule_flag33           is null or new.nrt_rule_flag33            <=>     old.nrt_rule_flag33           ) and
            (old.nrt_rule_flag34           is null or new.nrt_rule_flag34            <=>     old.nrt_rule_flag34           ) and
            (old.nrt_rule_flag35           is null or new.nrt_rule_flag35            <=>     old.nrt_rule_flag35           ) and
            (old.nrt_rule_flag36           is null or new.nrt_rule_flag36            <=>     old.nrt_rule_flag36           ) and
            (old.nrt_rule_flag37           is null or new.nrt_rule_flag37            <=>     old.nrt_rule_flag37           ) and
            (old.nrt_rule_flag38           is null or new.nrt_rule_flag38            <=>     old.nrt_rule_flag38           ) and
            (old.nrt_rule_flag39           is null or new.nrt_rule_flag39            <=>     old.nrt_rule_flag39           ) and
            (old.nrt_rule_flag40           is null or new.nrt_rule_flag40            <=>     old.nrt_rule_flag40           ) and
            (old.nrt_rule_flag41           is null or new.nrt_rule_flag41            <=>     old.nrt_rule_flag41           ) and
            (old.nrt_rule_flag42           is null or new.nrt_rule_flag42            <=>     old.nrt_rule_flag42           ) and
            (old.nrt_rule_flag43           is null or new.nrt_rule_flag43            <=>     old.nrt_rule_flag43           ) and
            (old.nrt_rule_flag44           is null or new.nrt_rule_flag44            <=>     old.nrt_rule_flag44           ) and
            (old.nrt_rule_flag45           is null or new.nrt_rule_flag45            <=>     old.nrt_rule_flag45           ) and
            (old.nrt_rule_flag46           is null or new.nrt_rule_flag46            <=>     old.nrt_rule_flag46           ) and
            (old.nrt_rule_flag47           is null or new.nrt_rule_flag47            <=>     old.nrt_rule_flag47           ) and
            (old.nrt_rule_flag48           is null or new.nrt_rule_flag48            <=>     old.nrt_rule_flag48           ) and
            (old.nrt_rule_flag49           is null or new.nrt_rule_flag49            <=>     old.nrt_rule_flag49           ) and
            (old.nrt_rule_flag50           is null or new.nrt_rule_flag50            <=>     old.nrt_rule_flag50           ) and
            (old.nrt_rule_flag51           is null or new.nrt_rule_flag51            <=>     old.nrt_rule_flag51           ) and
            (old.nrt_rule_flag52           is null or new.nrt_rule_flag52            <=>     old.nrt_rule_flag52           ) and
            (old.nrt_rule_flag53           is null or new.nrt_rule_flag53            <=>     old.nrt_rule_flag53           ) and
            (old.nrt_rule_flag54           is null or new.nrt_rule_flag54            <=>     old.nrt_rule_flag54           ) and
            (old.nrt_rule_flag55           is null or new.nrt_rule_flag55            <=>     old.nrt_rule_flag55           ) and
            (old.nrt_rule_flag56           is null or new.nrt_rule_flag56            <=>     old.nrt_rule_flag56           ) and
            (old.nrt_rule_flag57           is null or new.nrt_rule_flag57            <=>     old.nrt_rule_flag57           ) and
            (old.nrt_rule_flag58           is null or new.nrt_rule_flag58            <=>     old.nrt_rule_flag58           ) and
            (old.nrt_rule_flag59           is null or new.nrt_rule_flag59            <=>     old.nrt_rule_flag59           ) and
            (old.nrt_rule_flag60           is null or new.nrt_rule_flag60            <=>     old.nrt_rule_flag60           ) and
            (old.nrt_rule_flag61           is null or new.nrt_rule_flag61            <=>     old.nrt_rule_flag61           ) and
            (old.nrt_rule_flag62           is null or new.nrt_rule_flag62            <=>     old.nrt_rule_flag62           ) and
            (old.nrt_rule_flag63           is null or new.nrt_rule_flag63            <=>     old.nrt_rule_flag63           ) and
            (old.nrt_rule_flag64           is null or new.nrt_rule_flag64            <=>     old.nrt_rule_flag64           ) and
            (old.nrt_rule_flag65           is null or new.nrt_rule_flag65            <=>     old.nrt_rule_flag65           ) and
            (old.nrt_rule_flag66           is null or new.nrt_rule_flag66            <=>     old.nrt_rule_flag66           ) and
            (old.nrt_rule_flag67           is null or new.nrt_rule_flag67            <=>     old.nrt_rule_flag67           ) and
            (old.nrt_rule_flag68           is null or new.nrt_rule_flag68            <=>     old.nrt_rule_flag68           ) and
            (old.nrt_rule_flag69           is null or new.nrt_rule_flag69            <=>     old.nrt_rule_flag69           ) and
            (old.nrt_rule_flag70           is null or new.nrt_rule_flag70            <=>     old.nrt_rule_flag70           ) and
            (old.nrt_rule_flag71           is null or new.nrt_rule_flag71            <=>     old.nrt_rule_flag71           ) and
            (old.nrt_rule_flag72           is null or new.nrt_rule_flag72            <=>     old.nrt_rule_flag72           ) and
            (old.nrt_rule_flag73           is null or new.nrt_rule_flag73            <=>     old.nrt_rule_flag73           ) and
            (old.nrt_rule_flag74           is null or new.nrt_rule_flag74            <=>     old.nrt_rule_flag74           ) and
            (old.nrt_rule_flag75           is null or new.nrt_rule_flag75            <=>     old.nrt_rule_flag75           ) and
            (old.nrt_rule_flag76           is null or new.nrt_rule_flag76            <=>     old.nrt_rule_flag76           ) and
            (old.nrt_rule_flag77           is null or new.nrt_rule_flag77            <=>     old.nrt_rule_flag77           ) and
            (old.nrt_rule_flag78           is null or new.nrt_rule_flag78            <=>     old.nrt_rule_flag78           ) and
            (old.nrt_rule_flag79           is null or new.nrt_rule_flag79            <=>     old.nrt_rule_flag79           ) and
            (old.nrt_rule_flag80           is null or new.nrt_rule_flag80            <=>     old.nrt_rule_flag80           )
        )
    )