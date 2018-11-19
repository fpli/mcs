#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_es jialili1@chocolate-grafana <<EOSSH
hostname
cd /home/chocolate/es/bin
pwd
./indexCleaner.sh