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

create table choco_data.dw_mpx_clients (
  client_id int,
  client_name varchar(200),
  client_cntry_id decimal(9,0),
  client_site_id int,
  status_id smallint,
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
LOCATION 'viewfs://apollo-rno/apps/b_marketing_tracking/rotation/dw_mpx_clients';


create table choco_data.dw_mpx_clients_temp (
  client_id int,
  client_name varchar(200),
  client_cntry_id decimal(9,0),
  client_site_id int,
  status_id smallint,
  cre_date string,
  cre_user varchar(10),
  upd_date timestamp,
  upd_user varchar(10)
)STORED AS TEXTFILE;


CREATE TABLE choco_data.dw_mpx_rotations_w_mc_volatile
(
  client_id int,
  client_name varchar(200),
  client_cntry_id decimal(9,0),
  client_site_id int,
  status_id smallint,
  CRE_DATE STRING,
  CRE_USER varchar(30),
  UPD_USER varchar(30)
)STORED AS TEXTFILE;