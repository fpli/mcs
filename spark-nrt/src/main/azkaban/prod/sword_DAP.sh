#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark _choco_admin@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
cd /home/chocolate/chocolate-sparknrt/bin/prod
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
./sword.sh DISPLAY /apps/tracking-events/ /apps/tracking-events-workdir/sword/