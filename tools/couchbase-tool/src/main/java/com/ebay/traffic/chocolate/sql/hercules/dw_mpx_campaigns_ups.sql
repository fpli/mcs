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
set mapred.task.timeout= 1200000;



msck repair table im_tracking.dw_mpx_campaigns_ups;


insert overwrite table im_tracking.dw_mpx_campaigns_ups_distinct
select origin.campaign_id,
       origin.campaign_name,
       nvl(origin.client_id, -999),
       origin.cre_date,
       origin.cre_user,
       origin.upd_user
from(
select campaign_id,
       trim(campaign_name) campaign_name,
       client_id,
       cre_date,
       cre_user,
       upd_user,
       row_number() over (partition by campaign_id order by cre_date desc) as row_number
from im_tracking.dw_mpx_campaigns_ups
) origin
where origin.row_number = 1;


insert overwrite table im_tracking.dw_mpx_campaigns_temp
select o.campaign_id,
       o.campaign_name,
       o.client_id,
       o.cre_date,
       o.cre_user,
       o.upd_date,
       o.upd_user
from im_tracking.dw_mpx_campaigns o
left outer join im_tracking.dw_mpx_campaigns_ups_distinct u
on o.campaign_id = u.campaign_id
where u.campaign_id is null;


insert into table im_tracking.dw_mpx_campaigns_temp
select w.campaign_id,
       nvl(nvl(w.campaign_name, t.campaign_name), 'Undefined'),
       w.client_id,
       t.cre_date,
       t.cre_user,
       current_timestamp(),
       w.upd_user
from im_tracking.dw_mpx_campaigns_ups_distinct w
left outer join im_tracking.dw_mpx_campaigns t
on t.campaign_id = w.campaign_id
where t.campaign_id is not null;


insert into table im_tracking.dw_mpx_campaigns_temp
select caw.campaign_id,
       nvl(caw.campaign_name, 'Undefined'),
       caw.client_id,
       caw.cre_date,
       nvl(caw.cre_user, 'b_marketing_tracking') cre_user,
       current_timestamp(),
       caw.upd_user
from im_tracking.dw_mpx_campaigns_ups_distinct caw
left outer join im_tracking.dw_mpx_campaigns ca
on caw.campaign_id = ca.campaign_id
where ca.campaign_id is null;


insert overwrite table im_tracking.dw_mpx_campaigns
select *
from im_tracking.dw_mpx_campaigns_temp;
