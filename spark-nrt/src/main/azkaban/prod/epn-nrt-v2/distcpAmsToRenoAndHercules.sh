#!/bin/bash

set -x

function start_job(){
    host=$1
    ssh -o ServerAliveInterval=180 -o ServerAliveCountMax=10 -T -i /usr/azkaban/id_rsa_spark _choco_admin@${host} <<EOSSH
    hostname
    cd /datashare/mkttracking/jobs/tracking/epnnrt_v2/bin/prod
    pwd
    export HADOOP_USER_NAME=chocolate
    echo $HADOOP_USER_NAME
    ./distcpAmsToRenoAndHercules.sh /apps/epn-nrt/click_v2 /apps/b_marketing_tracking/chocolate/epnnrt_v2/click /sys/edw/imk/im_tracking/epn/ams_click_v2/snapshot click &&
    ./distcpAmsToRenoAndHercules.sh /apps/epn-nrt/impression_v2 /apps/b_marketing_tracking/chocolate/epnnrt_v2/imp /sys/edw/imk/im_tracking/epn/ams_impression_v2/snapshot imp
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