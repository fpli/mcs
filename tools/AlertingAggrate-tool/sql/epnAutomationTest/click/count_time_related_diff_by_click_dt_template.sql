select round(t1.count/t2.totalCount*100,1) as last_vwd_item_id_not_null_percent from
(select count(*) as count from choco_data.ams_click_new_test where last_vwd_item_id is not null and click_dt='#{click_dt}')t1,
(select count(*) as totalCount from choco_data.ams_click_new_test where click_dt='#{click_dt}') t2