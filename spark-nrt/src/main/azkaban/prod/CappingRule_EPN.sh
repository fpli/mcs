#!/bin/bash
whoami

function start_job(){
    host=$1
    ssh -T -i /usr/azkaban/id_rsa_spark stack@${host} <<EOSSH
    hostname
    export HADOOP_USER_NAME=chocolate
    echo $HADOOP_USER_NAME
    export hdfs=hdfs://elvisha
    echo $hdfs
    /datashare/mkttracking/jobs/tracking/sparknrt/bin/prod/cappingRule.sh EPN $hdfs/apps/tracking-events-workdir $hdfs/apps/tracking-events $hdfs/apps/tracking-events-archiveDir 150 http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200
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
