#!/bin/bash

set -x

# not use SubmitJob shell, this scp job is one high IO job, not want do this job in spark submit hosts

function start_job(){
    host=$1
    ssh -o ServerAliveInterval=180 -o ServerAliveCountMax=10 -T -i /usr/azkaban/id_rsa_spark _choco_admin@${host} <<EOSSH
    hostname
    cd /datashare/mkttracking/jobs/chocolate-sparknrt/bin/prod
    pwd
    export HADOOP_USER_NAME=chocolate
    echo $HADOOP_USER_NAME
    ./distcpImkToRenoAndHercules.sh /apps/tracking-events/imkTransform/imkOutput /apps/tracking-events/crabTransform/imkOutput /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event /sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event/snapshot false true &&
    ./distcpImkToRenoAndHercules.sh /apps/tracking-events/imkTransform/dtlOutput /apps/tracking-events/crabTransform/dtlOutput /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl /sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_dtl/snapshot true true
EOSSH
}

nc -zv slcchocolatepits-1242730.stratus.slc.ebay.com 22
rcode=$?
if [ ${rcode} -eq 0 ]
then
    start_job "slcchocolatepits-1242730.stratus.slc.ebay.com"
else
    echo "slcchocolatepits-1242730.stratus.slc.ebay.com is DOWN, please check!!!"
    echo "change to slcchocolatepits-1154246.stratus.slc.ebay.com"

    start_job "slcchocolatepits-1154246.stratus.slc.ebay.com"
fi