set mapreduce.job.queuename=hdlq-commrce-default;

set hive.support.concurrency=true;
set hive.enforce.bucketing=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.compactor.initiator.on=true;
set hive.compactor.worker.threads=1;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

DROP TABLE IF EXISTS choco_data.choco_rotation_latest;

create external table choco_data.choco_rotation_latest (rotation_info STRING)
STORED AS TEXTFILE
LOCATION '==rotation_hdp==';

DROP TABLE IF EXISTS choco_data.choco_rotation_info_temp;

CREATE TABLE choco_data.choco_rotation_info_temp
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
  client_id INT,
  client_name STRING,
  vendor_id INT,
  vendor_name STRING,
  rover_channel_id BIGINT,
  additional_info STRING,
  create_date timestamp,
  update_date timestamp,
  create_user STRING,
  update_user STRING
)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE choco_data.choco_rotation_info_temp
select
get_json_object(rotation_info, '$.rotation_id') as rotation_id,
get_json_object(rotation_info, '$.rotation_string') as rotation_string,
get_json_object(rotation_info, '$.rotation_name') as rotation_name,
get_json_object(rotation_info, '$.rotation_description') as rotation_desc_txt,
get_json_object(rotation_info, '$.rotation_tag.placement_id') as placement_id,
get_json_object(rotation_info, '$.channel_id') as channel_id,
get_json_object(rotation_info, '$.rotation_tag.channel_name') as channel_name,
get_json_object(rotation_info, '$.site_id') as site_id,
get_json_object(rotation_info, '$.rotation_tag.site_name') as site_name,
get_json_object(rotation_info, '$.campaign_id') as campaign_id,
get_json_object(rotation_info, '$.campaign_name') as campaign_name,
get_json_object(rotation_info, '$.rotation_tag.client_id') as client_id,
get_json_object(rotation_info, '$.rotation_tag.client_name') as client_name,
get_json_object(rotation_info, '$.vendor_id') as vendor_id,
get_json_object(rotation_info, '$.vendor_name') as vendor_name,
get_json_object(rotation_info, '$.rotation_tag.rover_channel_id') as rover_channel_id,
get_json_object(rotation_info, '$.rotation_tag') as additional_info,
get_json_object(rotation_info, '$.create_date') as create_date,
get_json_object(rotation_info, '$.update_date') as update_date,
get_json_object(rotation_info, '$.create_user') as create_user,
get_json_object(rotation_info, '$.update_user') as update_user
from choco_data.choco_rotation_latest;

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
  client_id INT,
  client_name STRING,
  vendor_id INT,
  vendor_name STRING,
  rover_channel_id BIGINT,
  additional_info STRING,
  create_date timestamp,
  update_date timestamp,
  create_user STRING,
  update_user STRING
)STORED AS TEXTFILE;

insert into table choco_data.choco_rotation_info select * from choco_data.choco_rotation_info_temp;