#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
cd /home/chocolate/chocolate-sparknrt/bin/prod
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
./cappingRule.sh EPN /apps/tracking-events-workdir /apps/tracking-events 150 http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200 true