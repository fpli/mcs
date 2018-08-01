select
r.rotation_id,
r.rotation_string,
r.rotation_name,
r.campaign_id,
b.campaign_name,
r.vendor_id,
v.vendor_name,
r.client_id,
c.client_name,
r.rotation_desc_txt,
r.placement_id,
cl.mpx_chnl_id,
cl.mpx_chnl_name,
r.portl_bkt_id,
r.portl_sub_bkt_id,
r.rotation_date_start,
r.rotation_date_end,
r.rotation_sts_name,
r.upd_date,
r.cre_date
from (
  select
    a.rotation_id, a.rotation_string,
    a.rotation_name, a.rotation_desc_txt,
    a.campaign_id, a.vendor_id,
    a1.client_id, a.mpx_chnl_id,
    a.placement_id,
    a.rotation_ct_url_name,
    a.portl_bkt_id, a.portl_sub_bkt_id,
    a.rotation_date_start, a.rotation_date_end, a.rotation_sts_name,
    a.upd_date, a.cre_date
  from Access_Views.dw_mpx_rotations a
  INNER JOIN (select rotation_id, SUBSTR(rotation_string, 0, POSITION('-' IN rotation_string)) as client_id from Access_Views.dw_mpx_rotations) a1
  on a.rotation_id = a1.rotation_id
) as r
left join Access_Views.DW_MPX_CAMPAIGNS b
on r.campaign_id = b.campaign_id
left join Access_Views.DW_MPX_VENDORS v
on r.vendor_id = v.vendor_id
left join Access_Views.DW_MPX_CLIENTS c
on r.client_id = c.client_id
left join Access_Views.DW_MPX_CHNL_LKP cl
on r.mpx_chnl_id = cl.mpx_chnl_id;