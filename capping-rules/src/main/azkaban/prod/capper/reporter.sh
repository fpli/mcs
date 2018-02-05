#!/bin/bash
scanStopTime=$(cat rundate.text)
if [[ -n "$scanStopTime" ]]; then
    echo "$scanStopTime"
else
    echo "argument error"
fi
whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
cd /home/hbase/sparkjobs/chocolate-1.0-SNAPSHOT-bin/chocolate-cappingrule/bin/prod
pwd
echo "on remote server's param = $scanStopTime"
export HADOOP_USER_NAME=spark
echo $HADOOP_USER_NAME
./reportGeneratorJob.sh prod_transactional prod_transactional EPN $scanStopTime 1440 15 CASSANDRA PROD
EOSSH