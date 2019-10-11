set mapreduce.job.queuename=hdlq-data-default;
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

create external table im_tracking.dw_mpx_vendors (
  vendor_id int,
  vendor_name varchar(200),
  vendor_url varchar(4000),
  status_id smallint,
  cre_date string,  -- DATE FORMAT 'YYYY-MM-DD'
  cre_user varchar(10),
  upd_date timestamp,
  upd_user varchar(10)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
NULL DEFINED AS ""
STORED AS TEXTFILE
LOCATION 'hdfs://hercules/sys/edw/imk/im_tracking/rotation/dw_mpx_vendors/snapshot';


create external table im_tracking.dw_mpx_vendors_temp (
  vendor_id int,
  vendor_name varchar(200),
  vendor_url varchar(4000),
  status_id smallint,
  cre_date string,  -- DATE FORMAT 'YYYY-MM-DD'
  cre_user varchar(10),
  upd_date timestamp,
  upd_user varchar(10)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
NULL DEFINED AS ""
STORED AS TEXTFILE
LOCATION 'hdfs://hercules/sys/edw/imk/im_tracking/rotation/dw_mpx_vendors_temp/snapshot';


CREATE external TABLE im_tracking.dw_mpx_rotations_w_ve_volatile
(
  vendor_id int,
  vendor_name varchar(200),
  vendor_url varchar(4000),
  status_id smallint,
  CRE_DATE STRING,
  CRE_USER varchar(30),
  UPD_USER varchar(30)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\|'
LINES TERMINATED BY '\n'
NULL DEFINED AS ""
STORED AS TEXTFILE
LOCATION 'hdfs://hercules/sys/edw/imk/im_tracking/rotation/dw_mpx_rotations_w_ve_volatile/snapshot';