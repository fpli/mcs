#!/bin/bash

function start_job(){
    host=$1
    ssh -T -i /usr/azkaban/id_rsa_spark stack@${host} <<EOSSH
    hostname
    cd /datashare/mkttracking/jobs/chocolate-sparknrt/bin/prod
    pwd
    export HADOOP_USER_NAME=chocolate
    echo $HADOOP_USER_NAME
    ./putImkHourlyDoneToRenoAndHercules.sh /apps/tracking-events/watch /apps/b_marketing_tracking/watch /apps/b_marketing_tracking/watch /datashare/mkttracking/work/imkToReno/imk/imkTemp
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