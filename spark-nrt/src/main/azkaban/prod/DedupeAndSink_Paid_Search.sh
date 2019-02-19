#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
cd /home/chocolate/chocolate-sparknrt/bin/prod
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
./dedupeAndSink.sh PAID_SEARCH marketingtech.ap.tracking-events.filtered-paid-search /apps/tracking-events-workdir /apps/tracking-events http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200 1 true