#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_es jialili1@chocolate-grafana <<EOSSH
hostname
cd /home/chocolate/es/bin
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
./indexCleaner.sh