#!/usr/bin/env bash
whoami
ssh -T -i /usr/azkaban/id_rsa_epn_to_apollo _choco_admin@lvschocolatepits-1583717.stratus.lvs.ebay.com <<EOSSH
hostname
cd /apache/distcp
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
./EPNToApollo.sh