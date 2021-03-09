CREATE EXTERNAL TABLE choco_data.ams_click_diff_tmp(
  `click_id` decimal(18),
  `click_dt` string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ''
LOCATION
  'viewfs://apollo-rno/apps/b_marketing_tracking/delta_test/ams_click_diff_tmp'
TBLPROPERTIES (
  'transient_lastDdlTime'='1601444855');