#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
cd /home/spark/chocolate-sparknrt/bin/prod
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
./reporting.sh EPN /apps/tracking-events-workdir