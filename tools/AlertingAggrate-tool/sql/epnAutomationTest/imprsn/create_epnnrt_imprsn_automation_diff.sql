CREATE EXTERNAL TABLE choco_data.epnnrt_imprsn_automation_diff(
  `imprsn_cntnr_id` decimal(18),
  `new_ams_trans_rsn_cd` smallint,
  `old_ams_trans_rsn_cd` smallint,
  `new_pblshr_id` decimal(18),
  `old_pblshr_id` decimal(18),
  `new_ams_pblshr_cmpgn_id` decimal(18),
  `old_ams_pblshr_cmpgn_id` decimal(18),
  `imprsn_dt` string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ''
LOCATION
  'viewfs://apollo-rno/apps/b_marketing_tracking/epnnrt-automation-diff/imp'
TBLPROPERTIES (
  'transient_lastDdlTime'='1601444855');