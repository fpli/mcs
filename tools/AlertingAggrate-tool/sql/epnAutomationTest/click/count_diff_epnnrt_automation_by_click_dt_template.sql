insert into choco_data.epnnrt_click_automation_diff
select
    new_click.click_id               as click_id               ,
    new_click.ams_trans_rsn_cd       as new_ams_trans_rsn_cd       , old_click.ams_trans_rsn_cd       as old_ams_trans_rsn_cd       ,
    new_click.fltr_yn_ind            as new_fltr_yn_ind            , old_click.fltr_yn_ind            as old_fltr_yn_ind            ,
    new_click.roi_fltr_yn_ind        as new_roi_fltr_yn_ind        , old_click.roi_fltr_yn_ind        as old_roi_fltr_yn_ind        ,
    new_click.roi_rule_values        as new_roi_rule_values        , old_click.roi_rule_values        as old_roi_rule_values        ,
    '#{click_dt}' as click_dt
FROM (select * from choco_data.ams_click_new_test where click_dt='#{click_dt}' and click_id is not  null) new_click
    full outer join (select * from choco_data.ams_click_old_test where click_dt='#{click_dt}' and click_id is not  null) old_click
on  new_click.click_id = old_click.click_id where  !(
    new_click.ams_trans_rsn_cd        <=>     old_click.ams_trans_rsn_cd        and
    new_click.roi_fltr_yn_ind         <=>     old_click.roi_fltr_yn_ind         and
    new_click.roi_rule_values         <>     old_click.roi_rule_values         and
    new_click.click_id is not null and old_click.click_id is not null) limit 2