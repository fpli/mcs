#!/bin/bash
# this job will move to EPN nrt2batch machine, pending choosing machine
#whoami
#ssh -T -i /usr/azkaban/id_rsa_panda panda@10.182.75.42 <<EOSSH
#cd /ebay/rovertracking/longterm/prod
#pwd
#export HADOOP_USER_NAME=chocolate
#echo $HADOOP_USER_NAME
#java -classpath cho-panda-etl-scp-*.jar com.ebay.traffic.chocolate.ScpJob prod