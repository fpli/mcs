#!/bin/bash
whoami

function start_job(){
    host=$1
    ssh -T -i /usr/azkaban/id_rsa_spark stack@${host} <<EOSSH
    hostname
    export HADOOP_USER_NAME=chocolate
    echo $HADOOP_USER_NAME
    /datashare/mkttracking/jobs/tracking/sparknrt/bin/prod/dedupeAndSink.sh PAID_SEARCH marketingtech.ap.tracking-events.filtered-paid-search /apps/tracking-events-workdir /apps/tracking-events http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200 1 true
EOSSH
}

nc -zv lvschocolatepits-1585074.stratus.lvs.ebay.com 22
rcode=$?
if [ ${rcode} -eq 0 ]
then
    start_job "lvschocolatepits-1585074.stratus.lvs.ebay.com"
else
    echo "lvschocolatepits-1585074.stratus.lvs.ebay.com is DOWN, please check!!!"
    echo "change to lvschocolatepits-1448901.stratus.lvs.ebay.com"

    start_job "lvschocolatepits-1448901.stratus.lvs.ebay.com"
fi