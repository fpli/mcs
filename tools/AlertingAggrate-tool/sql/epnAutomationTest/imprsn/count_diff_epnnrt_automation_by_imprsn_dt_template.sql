insert into table choco_data.epnnrt_imprsn_automation_diff_tmp
select
    new.imprsn_cntnr_id           as new_imprsn_cntnr_id           , old.imprsn_cntnr_id           as old_imprsn_cntnr_id           ,
    new.file_schm_vrsn_num        as new_file_schm_vrsn_num        , old.file_schm_vrsn_num        as old_file_schm_vrsn_num        ,
    new.file_id                   as new_file_id                   , old.file_id                   as old_file_id                   ,
    new.batch_id                  as new_batch_id                  , old.batch_id                  as old_batch_id                  ,
    new.chnl_id                   as new_chnl_id                   , old.chnl_id                   as old_chnl_id                   ,
    new.crltn_guid_txt            as new_crltn_guid_txt            , old.crltn_guid_txt            as old_crltn_guid_txt            ,
    new.guid_txt                  as new_guid_txt                  , old.guid_txt                  as old_guid_txt                  ,
    new.user_id                   as new_user_id                   , old.user_id                   as old_user_id                   ,
    new.clnt_rmt_ip               as new_clnt_rmt_ip               , old.clnt_rmt_ip               as old_clnt_rmt_ip               ,
    new.brwsr_type_num            as new_brwsr_type_num            , old.brwsr_type_num            as old_brwsr_type_num            ,
    new.brwsr_name                as new_brwsr_name                , old.brwsr_name                as old_brwsr_name                ,
    new.rfr_url_name              as new_rfr_url_name              , old.rfr_url_name              as old_rfr_url_name              ,
    new.prvs_rfr_dmn_name         as new_prvs_rfr_dmn_name         , old.prvs_rfr_dmn_name         as old_prvs_rfr_dmn_name         ,
    new.user_query_txt            as new_user_query_txt            , old.user_query_txt            as old_user_query_txt            ,
    new.plcmnt_data_txt           as new_plcmnt_data_txt           , old.plcmnt_data_txt           as old_plcmnt_data_txt           ,
    new.pblshr_id                 as new_pblshr_id                 , old.pblshr_id                 as old_pblshr_id                 ,
    new.ams_pblshr_cmpgn_id       as new_ams_pblshr_cmpgn_id       , old.ams_pblshr_cmpgn_id       as old_ams_pblshr_cmpgn_id       ,
    new.ams_tool_id               as new_ams_tool_id               , old.ams_tool_id               as old_ams_tool_id               ,
    new.cstm_id                   as new_cstm_id                   , old.cstm_id                   as old_cstm_id                   ,
    new.flex_fld_vrsn_num         as new_flex_fld_vrsn_num         , old.flex_fld_vrsn_num         as old_flex_fld_vrsn_num         ,
    new.flex_fld_1_txt            as new_flex_fld_1_txt            , old.flex_fld_1_txt            as old_flex_fld_1_txt            ,
    new.flex_fld_2_txt            as new_flex_fld_2_txt            , old.flex_fld_2_txt            as old_flex_fld_2_txt            ,
    new.flex_fld_3_txt            as new_flex_fld_3_txt            , old.flex_fld_3_txt            as old_flex_fld_3_txt            ,
    new.flex_fld_4_txt            as new_flex_fld_4_txt            , old.flex_fld_4_txt            as old_flex_fld_4_txt            ,
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
    new.ctx_txt                   as new_ctx_txt                   , old.ctx_txt                   as old_ctx_txt                   ,
    new.ctx_cld_txt               as new_ctx_cld_txt               , old.ctx_cld_txt               as old_ctx_cld_txt               ,
    new.ctx_rslt_txt              as new_ctx_rslt_txt              , old.ctx_rslt_txt              as old_ctx_rslt_txt              ,
    new.rfr_dmn_name              as new_rfr_dmn_name              , old.rfr_dmn_name              as old_rfr_dmn_name              ,
    new.imprsn_ts                 as new_imprsn_ts                 , old.imprsn_ts                 as old_imprsn_ts                 ,
    new.ams_prgrm_id              as new_ams_prgrm_id              , old.ams_prgrm_id              as old_ams_prgrm_id              ,
    new.advrtsr_id                as new_advrtsr_id                , old.advrtsr_id                as old_advrtsr_id                ,
    new.ams_click_fltr_type_id    as new_ams_click_fltr_type_id    , old.ams_click_fltr_type_id    as old_ams_click_fltr_type_id    ,
    new.filter_yn_ind             as new_filter_yn_ind             , old.filter_yn_ind             as old_filter_yn_ind             ,
    new.rover_url_txt             as new_rover_url_txt             , old.rover_url_txt             as old_rover_url_txt             ,
    new.mplx_timeout_flag         as new_mplx_timeout_flag         , old.mplx_timeout_flag         as old_mplx_timeout_flag         ,
    new.cb_kw                     as new_cb_kw                     , old.cb_kw                     as old_cb_kw                     ,
    new.cb_cat                    as new_cb_cat                    , old.cb_cat                    as old_cb_cat                    ,
    new.cb_ex_kw                  as new_cb_ex_kw                  , old.cb_ex_kw                  as old_cb_ex_kw                  ,
    new.cb_ex_cat                 as new_cb_ex_cat                 , old.cb_ex_cat                 as old_cb_ex_cat                 ,
    new.fb_used                   as new_fb_used                   , old.fb_used                   as old_fb_used                   ,
    new.ad_format                 as new_ad_format                 , old.ad_format                 as old_ad_format                 ,
    new.ad_content_type           as new_ad_content_type           , old.ad_content_type           as old_ad_content_type           ,
    new.load_time                 as new_load_time                 , old.load_time                 as old_load_time                 ,
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
    new.ams_trans_rsn_cd          as new_ams_trans_rsn_cd          , old.ams_trans_rsn_cd          as old_ams_trans_rsn_cd          ,
    new.trfc_src_cd               as new_trfc_src_cd               , old.trfc_src_cd               as old_trfc_src_cd               ,
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
    new.nrt_rule_flag72           as new_nrt_rule_flag72           , old.nrt_rule_flag72           as old_nrt_rule_flag72
from (select * from choco_data.ams_imprsn_new_test where imprsn_dt='#{imprsn_dt}' and imprsn_cntnr_id is not  null) new
         full outer join (select * from choco_data.ams_imprsn_old_test where imprsn_dt='#{imprsn_dt}' and imprsn_cntnr_id is not  null) old
                         on  old.imprsn_cntnr_id = new.imprsn_cntnr_id
where  !(
            (old.imprsn_cntnr_id           is null or new.imprsn_cntnr_id            <=>     old.imprsn_cntnr_id           ) and
            (old.file_schm_vrsn_num        is null or new.file_schm_vrsn_num         <=>     old.file_schm_vrsn_num        ) and
            (old.file_id                   is null or new.file_id                    <=>     old.file_id                   ) and
            (old.batch_id                  is null or new.batch_id                   <=>     old.batch_id                  ) and
            (old.chnl_id                   is null or new.chnl_id                    <=>     old.chnl_id                   ) and
            (old.crltn_guid_txt            is null or new.crltn_guid_txt             <=>     old.crltn_guid_txt            ) and
            (old.guid_txt                  is null or new.guid_txt                   <=>     old.guid_txt                  ) and
            (old.user_id                   is null or new.user_id                    <=>     old.user_id                   ) and
            (old.clnt_rmt_ip               is null or new.clnt_rmt_ip                <=>     old.clnt_rmt_ip               ) and
            (old.brwsr_type_num            is null or new.brwsr_type_num             <=>     old.brwsr_type_num            ) and
            (old.brwsr_name                is null or new.brwsr_name                 <=>     old.brwsr_name                ) and
            (old.rfr_url_name              is null or new.rfr_url_name               <=>     old.rfr_url_name              ) and
            (old.prvs_rfr_dmn_name         is null or new.prvs_rfr_dmn_name          <=>     old.prvs_rfr_dmn_name         ) and
            (old.user_query_txt            is null or new.user_query_txt             <=>     old.user_query_txt            ) and
            (old.plcmnt_data_txt           is null or new.plcmnt_data_txt            <=>     old.plcmnt_data_txt           ) and
            (old.pblshr_id                 is null or new.pblshr_id                  <=>     old.pblshr_id                 ) and
            (old.ams_pblshr_cmpgn_id       is null or new.ams_pblshr_cmpgn_id        <=>     old.ams_pblshr_cmpgn_id       ) and
            (old.ams_tool_id               is null or new.ams_tool_id                <=>     old.ams_tool_id               ) and
            (old.cstm_id                   is null or new.cstm_id                    <=>     old.cstm_id                   ) and
            (old.flex_fld_vrsn_num         is null or new.flex_fld_vrsn_num          <=>     old.flex_fld_vrsn_num         ) and
            (old.flex_fld_1_txt            is null or new.flex_fld_1_txt             <=>     old.flex_fld_1_txt            ) and
            (old.flex_fld_2_txt            is null or new.flex_fld_2_txt             <=>     old.flex_fld_2_txt            ) and
            (old.flex_fld_3_txt            is null or new.flex_fld_3_txt             <=>     old.flex_fld_3_txt            ) and
            (old.flex_fld_4_txt            is null or new.flex_fld_4_txt             <=>     old.flex_fld_4_txt            ) and
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
            (old.ctx_txt                   is null or new.ctx_txt                    <=>     old.ctx_txt                   ) and
            (old.ctx_cld_txt               is null or new.ctx_cld_txt                <=>     old.ctx_cld_txt               ) and
            (old.ctx_rslt_txt              is null or new.ctx_rslt_txt               <=>     old.ctx_rslt_txt              ) and
            (old.rfr_dmn_name              is null or new.rfr_dmn_name               <=>     old.rfr_dmn_name              ) and
            (old.imprsn_ts                 is null or new.imprsn_ts                  <=>     old.imprsn_ts                 ) and
            (old.ams_prgrm_id              is null or new.ams_prgrm_id               <=>     old.ams_prgrm_id              ) and
            (old.advrtsr_id                is null or new.advrtsr_id                 <=>     old.advrtsr_id                ) and
            (old.ams_click_fltr_type_id    is null or new.ams_click_fltr_type_id     <=>     old.ams_click_fltr_type_id    ) and
            (old.filter_yn_ind             is null or new.filter_yn_ind              <=>     old.filter_yn_ind             ) and
            (old.rover_url_txt             is null or new.rover_url_txt              <=>     old.rover_url_txt             ) and
            (old.mplx_timeout_flag         is null or new.mplx_timeout_flag          <=>     old.mplx_timeout_flag         ) and
            (old.cb_kw                     is null or new.cb_kw                      <=>     old.cb_kw                     ) and
            (old.cb_cat                    is null or new.cb_cat                     <=>     old.cb_cat                    ) and
            (old.cb_ex_kw                  is null or new.cb_ex_kw                   <=>     old.cb_ex_kw                  ) and
            (old.cb_ex_cat                 is null or new.cb_ex_cat                  <=>     old.cb_ex_cat                 ) and
            (old.fb_used                   is null or new.fb_used                    <=>     old.fb_used                   ) and
            (old.ad_format                 is null or new.ad_format                  <=>     old.ad_format                 ) and
            (old.ad_content_type           is null or new.ad_content_type            <=>     old.ad_content_type           ) and
            (old.load_time                 is null or new.load_time                  <=>     old.load_time                 ) and
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
            (old.ams_trans_rsn_cd          is null or new.ams_trans_rsn_cd           <=>     old.ams_trans_rsn_cd          ) and
            (old.trfc_src_cd               is null or new.trfc_src_cd                <=>     old.trfc_src_cd               ) and
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
            (old.nrt_rule_flag72           is null or new.nrt_rule_flag72            <=>     old.nrt_rule_flag72           )
    )