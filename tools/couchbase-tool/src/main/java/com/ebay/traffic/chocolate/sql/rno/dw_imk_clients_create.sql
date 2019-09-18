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

create external table choco_data.dw_imk_clients (
  client_id int,
  client_cntry_id decimal(9,0),
  client_site_id int,
  prtl_pay_out_curncy_id decimal(9,0),
  cntry_rgn_rel_id decimal(18,0),
  rgn_id smallint,
  rgn_desc varchar(50),
  rgn_abrvn_txt varchar(10),
  cre_date string,
  cre_user varchar(30),
  upd_date timestamp,
  upd_user varchar(30)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\44'
LINES TERMINATED BY '\n'
NULL DEFINED AS ""
STORED AS TEXTFILE
LOCATION 'viewfs://apollo-rno/apps/b_marketing_tracking/rotation/static_table/dw_imk_clients/result';

msck repair table choco_data.dw_imk_clients;