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
SET hive.auto.convert.join=false;
set mapred.task.timeout=1200000;

INSERT OVERWRITE LOCAL DIRECTORY '/home/_choco_admin/trackingEvent/herculeslvs' select dt ,COUNT(1) count from IM_TRACKING.TRACKING_EVENT WHERE dt>date_sub(current_date,10) AND dt<current_date GROUP BY dt ORDER BY dt;