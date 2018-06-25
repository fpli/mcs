set mapreduce.job.queuename=hdlq-commrce-default;
set mapreduce.job.split.metainfo.maxsize=-1;
set hive.vectorized.execution.enabled = true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.vectorized.execution.reduce.groupby.enabled = true;

set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;
SET hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.parallel=true;

SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

set hive.merge.smallfiles.avgsize=128000000;
set hive.merge.size.per.task=128000000;
add jar hdfs://apollo-phx-nn-ha/user/b_marketing_tracking/chocolate/hive-hcatalog-core-0.13.0.jar;

DROP TABLE IF EXISTS CHOCO_DATA.choco_rotations;

create table CHOCO_DATA.choco_rotations (
  rotation_id STRING,
  rotation_string STRING,
  channel_id INT,
  site_id INT,
  campaign_id BIGINT,
  campaign_name STRING,
  vendor_id INT,
  vendor_name STRING,
  rotation_name STRING,
  rotation_description STRING,
  last_update_time BIGINT,
  update_date STRING,
  rotation_tag map<string,string>
)
PARTITIONED by (dt string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs://apollo-phx-nn-ha/user/b_marketing_tracking/chocolate/rotation';

msck repair table CHOCO_DATA.choco_rotations;