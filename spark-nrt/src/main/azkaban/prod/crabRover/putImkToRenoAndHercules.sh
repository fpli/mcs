#!/bin/bash

set -x

# not use SubmitJob shell, this scp job is one high IO job, not want do this job in spark submit hosts

function start_job(){
    host=$1
    ssh -T -i /usr/azkaban/id_rsa_spark _choco_admin@${host} <<EOSSH
    hostname
    cd /datashare/mkttracking/jobs/chocolate-sparknrt/bin/prod
    pwd
    export HADOOP_USER_NAME=chocolate
    echo $HADOOP_USER_NAME
    ./putImkToRenoAndHercules.sh /apps/tracking-events/crabTransform/imkOutput /datashare/mkttracking/work/imkToReno/crab/imkTemp /apps/b_marketing_tracking/imk_tracking/imk_scp_middle/imk_rvr_trckng_event /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event /apps/b_marketing_tracking/IMK_RVR_TRCKNG_EVENT/imk_scp_middle/imk_rvr_trckng_event /sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event/snapshot &&
    ./putImkToRenoAndHercules.sh /apps/tracking-events/imkTransformMerged/imkOutput /datashare/mkttracking/work/imkToReno/imk/imkTemp /apps/b_marketing_tracking/imk_tracking/imk_scp_middle/imk_rvr_trckng_event /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event /apps/b_marketing_tracking/IMK_RVR_TRCKNG_EVENT/imk_scp_middle/imk_rvr_trckng_event /sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event/snapshot &&
    ./putImkToRenoAndHercules.sh /apps/tracking-events/crabTransform/dtlOutput /datashare/mkttracking/work/imkToReno/crab/dtlTemp /apps/b_marketing_tracking/imk_tracking/imk_scp_middle/imk_rvr_trckng_event_dtl /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl /apps/b_marketing_tracking/IMK_RVR_TRCKNG_EVENT/imk_scp_middle/imk_rvr_trckng_event_dtl /sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_dtl/snapshot &&
    ./putImkToRenoAndHercules.sh /apps/tracking-events/imkTransformMerged/dtlOutput /datashare/mkttracking/work/imkToReno/imk/dtlTemp /apps/b_marketing_tracking/imk_tracking/imk_scp_middle/imk_rvr_trckng_event_dtl /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl  /apps/b_marketing_tracking/IMK_RVR_TRCKNG_EVENT/imk_scp_middle/imk_rvr_trckng_event_dtl /sys/edw/imk/im_tracking/imk/imk_rvr_trckng_event_dtl/snapshot
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