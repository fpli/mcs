insert into table choco_data.epnnrt_imprsn_automation_diff
select
    new.imprsn_cntnr_id        as new_imprsn_cntnr_id        , old.imprsn_cntnr_id        as old_imprsn_cntnr_id        ,
    new.ams_trans_rsn_cd       as new_ams_trans_rsn_cd       , old.ams_trans_rsn_cd       as old_ams_trans_rsn_cd       ,
    new.pblshr_id              as new_pblshr_id              , old.pblshr_id              as old_pblshr_id              ,
    new.ams_pblshr_cmpgn_id    as new_ams_pblshr_cmpgn_id    , old.ams_pblshr_cmpgn_id    as old_ams_pblshr_cmpgn_id
FROM (select * from choco_data.ams_imprsn_new_test where imprsn_dt='#{imprsn_dt}' and imprsn_cntnr_id is not  null) new
         left outer join (select * from choco_data.ams_imprsn_old_test where imprsn_dt='#{imprsn_dt}' and imprsn_cntnr_id is not  null) old
                         on  new.imprsn_cntnr_id = old.imprsn_cntnr_id where  !(
            new.ams_trans_rsn_cd        <=>     old.ams_trans_rsn_cd        and
            new.pblshr_id               <=>     old.pblshr_id               and
            new.ams_pblshr_cmpgn_id     <>     old.ams_pblshr_cmpgn_id     and
            new.imprsn_cntnr_id is not null and old.imprsn_cntnr_id is not null) limit 2