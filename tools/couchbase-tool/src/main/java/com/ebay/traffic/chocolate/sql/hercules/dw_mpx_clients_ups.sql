set mapreduce.job.queuename=hdlq-data-batch-low;
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


insert overwrite table im_tracking.dw_mpx_rotations_w_mc_volatile
select origin.client_id,
       origin.client_name,
       origin.client_cntry_id,
       origin.site_id,
       origin.status_id,
       origin.cre_date,
       origin.cre_user,
       origin.upd_user
from (
select mr.client_id client_id,
       nvl(mr.client_name, 'Null client_name') client_name,
       nvl(ic.client_cntry_id,-999) client_cntry_id,
       nvl(ic.client_site_id,-999) site_id,
       1 status_id,
       mr.cre_date,
       nvl(mr.cre_user, 'b_marketing_tracking') cre_user,
       mr.upd_user,
       row_number() over (partition by mr.client_id order by mr.cre_date desc) as row_number
from im_tracking.dw_mpx_rotations_ups mr
left outer join im_tracking.dw_imk_clients ic
on ic.client_id = mr.client_id
) origin
where origin.row_number = 1;


insert overwrite table im_tracking.dw_mpx_clients_temp
select mc.client_id,
       mc.client_name,
       mc.client_cntry_id,
       mc.client_site_id,
       mc.status_id,
       mc.cre_date,
       mc.cre_user,
       mc.upd_date,
       mc.upd_user
from im_tracking.dw_mpx_clients mc
left outer join im_tracking.dw_mpx_rotations_w_mc_volatile cl
on mc.client_id = cl.client_id
where cl.client_id is null;



insert into table im_tracking.dw_mpx_clients_temp
select upd.client_id,
       upd.client_name,
       upd.client_cntry_id,
       upd.client_site_id,
       upd.status_id,
       ori.cre_date,
       ori.cre_user,
       current_timestamp(),
       upd.upd_user
from im_tracking.dw_mpx_rotations_w_mc_volatile upd
left outer join im_tracking.dw_mpx_clients ori
on upd.client_id = ori.client_id
where ori.client_id is not null;



insert into table im_tracking.dw_mpx_clients_temp
select upd.client_id,
       upd.client_name,
       upd.client_cntry_id,
       upd.client_site_id,
       upd.status_id,
       upd.cre_date,
       upd.cre_user,
       current_timestamp(),
       upd.upd_user
from im_tracking.dw_mpx_rotations_w_mc_volatile upd
left outer join im_tracking.dw_mpx_clients ori
on upd.client_id = ori.client_id
where ori.client_id is null;



insert overwrite table im_tracking.dw_mpx_clients
select *
from im_tracking.dw_mpx_clients_temp;