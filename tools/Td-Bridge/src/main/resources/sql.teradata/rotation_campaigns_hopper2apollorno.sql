sel a.* from syslib.parallel_export(
on (
  select count(*) from Access_Views.dw_mpx_campaigns
)
using
configname('hop_dm_apollorno')
configserver('bridge-gateway-hop:1025')
sinkClass('HDFSTextFileSink')
nullValue('')
delimiterCharCode('7')
dataPath('{:HDP:}')
fileErrorLimit(0)
childThreadCount(2)
recv_eofresponse(1)
sock_timeout(1000)
sock_buf_size('268435456')
remoteSock_Buf_Size('268435456')
) a;

