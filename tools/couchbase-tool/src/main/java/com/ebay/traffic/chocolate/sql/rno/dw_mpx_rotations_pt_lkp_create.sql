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

create external table choco_data.dw_mpx_rotations_pt_lkp (
  rot_pt_id decimal(18,0),
  perf_track_1 varchar(100),
  perf_track_2 varchar(100),
  perf_track_3 varchar(100),
  perf_track_4 varchar(100),
  perf_track_5 varchar(100),
  perf_track_6 varchar(100),
  perf_track_7 varchar(100),
  perf_track_8 varchar(100),
  perf_track_9 varchar(100),
  perf_track_10 varchar(100),
  cre_date string,
  cre_user char(10),
  upd_date timestamp,
  upd_user char(10)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\44'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'viewfs://apollo-rno/apps/b_marketing_tracking/rotation/static_table/dw_mpx_rotations_pt_lkp/result';

msck repair table choco_data.dw_mpx_rotations_pt_lkp;