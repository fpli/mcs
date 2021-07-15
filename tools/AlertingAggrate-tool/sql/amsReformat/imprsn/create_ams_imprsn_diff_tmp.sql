CREATE EXTERNAL TABLE choco_data.ams_imprsn_diff_tmp(
  `imprsn_cntnr_id` decimal(18),
  `imprsn_dt` string
  )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ''
LOCATION
  'viewfs://apollo-rno/apps/b_marketing_tracking/delta_test/ams_imprsn_diff_tmp'
TBLPROPERTIES (
  'transient_lastDdlTime'='1601444855');