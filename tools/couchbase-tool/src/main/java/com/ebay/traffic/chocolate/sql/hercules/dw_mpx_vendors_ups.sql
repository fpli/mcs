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


insert overwrite table im_tracking.dw_mpx_rotations_w_ve_volatile
select origin.vendor_id,
       trim(origin.vendor_name) vendor_name,
       trim(origin.vendor_url) vendor_url,
       origin.status_id,
       origin.cre_date,
       origin.cre_user,
       origin.upd_user
from (
select upd.vendor_id,
       nvl(upd.vendor_name, 'Null vendor_name') vendor_name,
       nvl(upd.vendor_url, 'Null vendor_url') vendor_url,
       stat.status_id,
       upd.cre_date,
       nvl(upd.cre_user, 'b_marketing_tracking') cre_user,
       upd.upd_user,
       row_number() over (partition by upd.vendor_id order by upd.cre_date desc) as row_number
from im_tracking.dw_mpx_rotations_ups upd
inner join
(select vendor_id,
       max(case when vendor_type = 'live' then 1
           when vendor_type = 'archive' then 2
           else 4
           end) status_id
from im_tracking.dw_mpx_rotations_ups
group by vendor_id
) stat
on upd.vendor_id = stat.vendor_id
) origin
where origin.row_number = 1;



insert overwrite table im_tracking.dw_mpx_vendors_temp
select ven.vendor_id,
       ven.vendor_name,
       ven.vendor_url,
       ven.status_id,
       ven.cre_date,
       ven.cre_user,
       ven.upd_date,
       ven.upd_user
from im_tracking.dw_mpx_vendors ven
left outer join im_tracking.dw_mpx_rotations_w_ve_volatile ve
on ven.vendor_id = ve.vendor_id
where ve.vendor_id is null;



insert into table im_tracking.dw_mpx_vendors_temp
select upd.vendor_id,
       upd.vendor_name,
       upd.vendor_url,
       upd.status_id,
       ori.cre_date,
       ori.cre_user,
       current_timestamp(),
       upd.upd_user
from im_tracking.dw_mpx_rotations_w_ve_volatile upd
left outer join im_tracking.dw_mpx_vendors ori
on upd.vendor_id = ori.vendor_id
where ori.vendor_id is not null;



insert into table im_tracking.dw_mpx_vendors_temp
select upd.vendor_id,
       upd.vendor_name,
       upd.vendor_url,
       upd.status_id,
       upd.cre_date,
       upd.cre_user,
       current_timestamp(),
       upd.upd_user
from im_tracking.dw_mpx_rotations_w_ve_volatile upd
left outer join im_tracking.dw_mpx_vendors ori
on upd.vendor_id = ori.vendor_id
where ori.vendor_id is null;



insert overwrite table im_tracking.dw_mpx_vendors
select *
from im_tracking.dw_mpx_vendors_temp;
