#!/bin/bash

function start_job(){
    host=$1
    ssh -T -i /usr/azkaban/id_rsa_spark stack@${host} <<EOSSH
    hostname
    cd /datashare/mkttracking/jobs/chocolate-sparknrt/bin/prod
    pwd
    export HADOOP_USER_NAME=chocolate
    echo $HADOOP_USER_NAME
    ./putRenoKwLkpToSlc.sh /apps/hdmi-technology/b_marketing_psdm/production/PSDM_META_TABLES/DW_KWDM_KW_LKP/1/1 /apps/kw_lkp /datashare/mkttracking/jobs/chocolate-sparknrt/bin/prod/temp_reno_kw_to_slc &&
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