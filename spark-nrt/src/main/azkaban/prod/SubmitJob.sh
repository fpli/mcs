#!/bin/bash

function start_job(){
    host=$1
    folder=$2
    ssh -T -i /usr/azkaban/id_rsa_spark _choco_admin@${host} <<EOSSH
    hostname
    cd ${folder}
    pwd
    export HADOOP_USER_NAME=chocolate
    echo $HADOOP_USER_NAME
    $3
EOSSH
}

function submit_job_in_SLC(){
    nc -zv slcchocolatepits-1154246.stratus.slc.ebay.com 22
    rcode=$?
    if [ ${rcode} -eq 0 ]
    then
        start_job "slcchocolatepits-1154246.stratus.slc.ebay.com" $1 "$2"
    else
        echo "slcchocolatepits-1154246.stratus.slc.ebay.com is DOWN, please check!!!"
        echo "change to slcchocolatepits-1242730.stratus.slc.ebay.com"
        start_job "slcchocolatepits-1242730.stratus.slc.ebay.com" $1 "$2"
    fi
}

function submit_job_in_LVS(){
    nc -zv lvschocolatepits-1585074.stratus.lvs.ebay.com 22
    rcode=$?
    if [ ${rcode} -eq 0 ]
    then
        start_job "lvschocolatepits-1585074.stratus.lvs.ebay.com" $1 "$2"
    else
        echo "lvschocolatepits-1585074.stratus.lvs.ebay.com is DOWN, please check!!!"
        echo "change to lvschocolatepits-1583703.stratus.lvs.ebay.com"
        start_job "lvschocolatepits-1583703.stratus.lvs.ebay.com" $1 "$2"
    fi
}


if [ $1 = "LVS" ]
then
    submit_job_in_LVS $2 "$3"
elif [ $1 = "SLC" ]
then
    submit_job_in_SLC $2 "$3"
else
    echo "please choose one correct cluster to run job"
    echo "USAGE: SubmitJob.sh [CLUSTER_RUN_SPARK] [SPARK_JOB_TYPE(sparknrt|epnnrt)] [COMMAND] "
    echo 1
fi