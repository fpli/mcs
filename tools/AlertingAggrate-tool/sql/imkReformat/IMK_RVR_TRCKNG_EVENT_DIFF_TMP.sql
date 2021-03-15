CREATE EXTERNAL TABLE `choco_data.IMK_RVR_TRCKNG_EVENT_DIFF_TMP`(
  `dt` string,
  `type` string,
  `diff_count` int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ''
LOCATION
  'viewfs://apollo-rno/apps/b_marketing_tracking/delta_test/imk_diff_tmp'
TBLPROPERTIES (
  'transient_lastDdlTime'='1601444855');