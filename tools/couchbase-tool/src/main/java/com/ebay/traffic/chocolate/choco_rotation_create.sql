set mapreduce.job.queuename=hdlq-commrce-default;

set hive.support.concurrency=true;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=1;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

DROP TABLE IF EXISTS choco_data.choco_rotation_info;

CREATE TABLE choco_data.choco_rotation_info
(
  rotation_id STRING,
  rotation_string STRING,
  rotation_name STRING,
  rotation_desc_txt STRING,
  placement_id INT,
  channel_id INT,
  channel_name INT,
  site_id INT,
  site_name STRING,
  campaign_id BIGINT,
  campaign_name STRING,
  vendor_id INT,
  vendor_name STRING,
  rover_channel_id BIGINT,
  additional_info STRING,
  create_date timestamp,
  update_date timestamp,
  create_user STRING,
  update_user STRING
)
Clustered by (rotation_id) into 4 buckets
STORED AS ORC
TBLPROPERTIES ('transactional'='true','orc.compress'='SNAPPY');


