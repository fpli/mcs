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


msck repair table choco_data.dw_mpx_rotations;

DROP TABLE IF EXISTS choco_data.choco_rotation_latest;

create external table choco_data.choco_rotation_latest (rotation_info STRING)
STORED AS TEXTFILE
LOCATION '==rotation_hdp==';


INSERT OVERWRITE TABLE choco_data.choco_rotation_info_temp
select
get_json_object(rotation_info, '$.rotation_id') as rotation_id,
get_json_object(rotation_info, '$.rotation_string') as rotation_string,
get_json_object(rotation_info, '$.rotation_name') as rotation_name,
get_json_object(rotation_info, '$.rotation_description') as rotation_desc_txt,
get_json_object(rotation_info, '$.rotation_tag.placement_id') as placement_id,
get_json_object(rotation_info, '$.channel_id') as channel_id,
get_json_object(rotation_info, '$.rotation_tag.channel_name') as channel_name,
get_json_object(rotation_info, '$.site_id') as site_id,
get_json_object(rotation_info, '$.rotation_tag.site_name') as site_name,
get_json_object(rotation_info, '$.campaign_id') as campaign_id,
get_json_object(rotation_info, '$.campaign_name') as campaign_name,
get_json_object(rotation_info, '$.rotation_tag.client_id') as client_id,
get_json_object(rotation_info, '$.rotation_tag.client_name') as client_name,
get_json_object(rotation_info, '$.vendor_id') as vendor_id,
get_json_object(rotation_info, '$.vendor_name') as vendor_name,
get_json_object(rotation_info, '$.rotation_tag.rover_channel_id') as rover_channel_id,
get_json_object(rotation_info, '$.rotation_tag') as additional_info,
get_json_object(rotation_info, '$.create_date') as create_date,
get_json_object(rotation_info, '$.update_date') as update_date,
get_json_object(rotation_info, '$.create_user') as create_user,
get_json_object(rotation_info, '$.update_user') as update_user
from choco_data.choco_rotation_latest;

insert overwrite table choco_data.choco_rotation_info_temp_one
select a.rotation_id,
       a.rotation_string,
       a.rotation_name,
       a.rotation_desc_txt,
       a.placement_id,
       a.channel_id,
       a.channel_name,
       coalesce(a.site_id,b.site_id) as site_id,
       a.site_name,
       a.campaign_id,
       a.campaign_name,
       coalesce(a.client_id, b.client_id) as client_id,
       a.client_name,
       a.vendor_id,
       a.vendor_name,
       a.rover_channel_id,
       a.additional_info,
       a.create_date,
       a.update_date,
       a.create_user,
       a.update_user
from choco_data.choco_rotation_info_temp a left outer join choco_data.dw_mpx_rotations b
on a.rotation_id = b.rotation_id;


insert overwrite table choco_data.choco_rotation_info_temp_two
SELECT A.*
FROM choco_data.choco_rotation_info_temp A
LEFT OUTER JOIN choco_data.choco_rotation_info_temp_one B
ON (B.rotation_id = A.rotation_id)
WHERE B.rotation_id IS null;


insert into table choco_data.choco_rotation_info_temp_two
select *
from choco_data.choco_rotation_info_temp_one;

insert overwrite table choco_data.choco_rotation_info select * from choco_data.choco_rotation_info_temp_two;