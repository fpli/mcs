CREATE EXTERNAL TABLE choco_data.epnnrt_click_automation_diff(
  `click_id` decimal(18),
  `new_ams_trans_rsn_cd` smallint,
  `old_ams_trans_rsn_cd` smallint,
  `new_fltr_yn_ind` tinyint,
  `old_fltr_yn_ind` tinyint,
  `new_roi_fltr_yn_ind` tinyint,
  `old_roi_fltr_yn_ind` tinyint,
  `new_roi_rule_values` int,
  `old_roi_rule_values` int,
  `click_dt` string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ''
LOCATION
  'viewfs://apollo-rno/apps/b_marketing_tracking/epnnrt-automation-diff/click'
TBLPROPERTIES (
  'transient_lastDdlTime'='1601444855');