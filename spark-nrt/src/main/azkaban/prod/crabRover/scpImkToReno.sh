#!/bin/bash

function start_job(){
    host=$1
    ssh -T -i /usr/azkaban/id_rsa_spark stack@${host} <<EOSSH
    hostname
    cd /datashare/mkttracking/jobs/chocolate-sparknrt/bin/prod
    pwd
    export HADOOP_USER_NAME=chocolate
    echo $HADOOP_USER_NAME
    ./scpImkToReno.sh /apps/tracking-events/crabTransform/imkOutput /apps/b_marketing_tracking/imk_tracking/imk_scp_middle/imk_rvr_trckng_event /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event /mnt/imkToReno/crab/imkTemp &&
    ./scpImkToReno.sh /apps/tracking-events/imkTransform/imkOutput /apps/b_marketing_tracking/imk_tracking/imk_scp_middle/imk_rvr_trckng_event /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event /mnt/imkToReno/imk/imkTemp &&
    ./scpImkToReno.sh /apps/tracking-events/crabTransform/dtlOutput /apps/b_marketing_tracking/imk_tracking/imk_scp_middle/imk_rvr_trckng_event_dtl /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl /mnt/imkToReno/crab/dtlTemp &&
    ./scpImkToReno.sh /apps/tracking-events/imkTransform/dtlOutput /apps/b_marketing_tracking/imk_tracking/imk_scp_middle/imk_rvr_trckng_event_dtl /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl /mnt/imkToReno/imk/dtlTemp
EOSSH
}

nc -zv slcchocolatepits-1154246.stratus.slc.ebay.com 22
rcode=$?
if [ ${rcode} -eq 0 ]
then
    start_job "slcchocolatepits-1154246.stratus.slc.ebay.com"
else
    echo "slcchocolatepits-1154246.stratus.slc.ebay.com is DOWN, please check!!!"
    echo "change to slcchocolatepits-1242730.stratus.slc.ebay.com"

    start_job "slcchocolatepits-1242730.stratus.slc.ebay.com"
fi
