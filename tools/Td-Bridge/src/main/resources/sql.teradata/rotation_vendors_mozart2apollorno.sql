sel a.* from syslib.parallel_export(
on (
  select count(*) from Access_Views.dw_mpx_vendors
)
using
configname('mzn_dm_apollorno')
configserver('bridge-gateway-mzn:1025')
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

