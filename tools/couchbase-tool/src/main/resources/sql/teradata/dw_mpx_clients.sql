sel a.* from syslib.parallel_export(
on (
  select client_id as client_id,
         client_cntry_id as client_cntry_id
  from dw_mpx_clients
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