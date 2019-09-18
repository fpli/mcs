set mapreduce.job.queuename=hdlq-commrce-default;
set hive.optimize.index.filter=false;
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

create external table im_tracking.dw_mpx_campaigns (
  campaign_id decimal(18,0),
  campaign_name varchar(1000),
  client_id int,
  cre_date string,
  cre_user varchar(10),
  upd_date timestamp,
  upd_user varchar(10)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
NULL DEFINED AS ""
STORED AS TEXTFILE
LOCATION 'hdfs://hercules/sys/edw/imk/im_tracking/rotation/dw_mpx_campaigns/snapshot';


create external table im_tracking.dw_mpx_campaigns_temp (
  campaign_id decimal(18,0),
  campaign_name varchar(1000),
  client_id int,
  cre_date string,
  cre_user varchar(10),
  upd_date timestamp,
  upd_user varchar(10)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
NULL DEFINED AS ""
STORED AS TEXTFILE
LOCATION 'hdfs://hercules/sys/edw/imk/im_tracking/rotation/dw_mpx_campaigns_temp/snapshot';


create external table im_tracking.dw_mpx_campaigns_ups_distinct (
  campaign_id decimal(18,0),
  campaign_name varchar(1000),
  client_id int,
  cre_date string,
  cre_user varchar(10),
  upd_user varchar(10)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
NULL DEFINED AS ""
STORED AS TEXTFILE
LOCATION 'hdfs://hercules/sys/edw/imk/im_tracking/rotation/dw_mpx_campaigns_ups_distinct/snapshot';


create external table im_tracking.dw_mpx_campaigns_ups (
  campaign_id decimal(20,0),
  campaign_name varchar(1000),
  client_id int,
  cre_date string,
  cre_user varchar(10),
  upd_user varchar(10)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
NULL DEFINED AS ""
STORED AS TEXTFILE
LOCATION 'hdfs://hercules/sys/edw/imk/im_tracking/rotation/dw_mpx_campaigns_ups/snapshot';
