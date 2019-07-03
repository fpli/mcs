sel a.* from syslib.parallel_export(
on (
  SELECT A.ROTATION_ID as rotation_id,
         C.CLIENT_ID as client_id,
         C.CLIENT_CNTRY_ID as site_id
  FROM DW_MPX_ROTATIONS A
  JOIN DW_MPX_CAMPAIGNS B
  ON A.CAMPAIGN_ID = B.CAMPAIGN_ID
  JOIN DW_MPX_CLIENTS C
  ON B.CLIENT_ID = C.CLIENT_ID
)
using
configname('mzt_dm_apollorno')
configserver('bridge-gateway-mzt:1025')
sinkClass('HDFSTextFileSink')
nullValue('')
delimiterCharCode('44')
dataPath('{:HDP:}')
fileErrorLimit(0)
childThreadCount(10)
recv_eofresponse(1)
sock_timeout(600)
) a;