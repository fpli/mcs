#!/bin/bash
CHANNEL=$1
WORK_DIR=$2
OUTPUT_DIR=$3

function start_job(){
    host=$1
    ssh -T -i /usr/azkaban/id_rsa_spark stack@${host} <<EOSSH
    hostname
    cd /datashare/mkttracking/jobs/tracking/chocolate-sparknrt-imk/bin/prod
    pwd
    export HADOOP_USER_NAME=chocolate
    echo $HADOOP_USER_NAME

    ./imkDump.sh ${CHANNEL} ${WORK_DIR} ${OUTPUT_DIR} http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200
EOSSH
}

nc -zv lvschocolatepits-1585074.stratus.lvs.ebay.com 22
rcode=$?
if [ ${rcode} -eq 0 ]
then
    start_job "lvschocolatepits-1585074.stratus.lvs.ebay.com"
else
    echo "lvschocolatepits-1585074.stratus.lvs.ebay.com is DOWN, please check!!!"
    echo "change to lvschocolatepits-1583703.stratus.lvs.ebay.com"

    start_job "lvschocolatepits-1583703.stratus.lvs.ebay.com"
fi

