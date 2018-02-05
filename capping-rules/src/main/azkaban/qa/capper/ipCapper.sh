#!/bin/bash
scanStopTime=$(cat rundate.text)
if [[ -n "$scanStopTime" ]]; then
    echo "$scanStopTime"
else
    echo "argument error"
fi
whoami
ssh -T -i /home/stack/.ssh/cloud.key stack@chocolate-qa-slc-7-5904.slc01.dev.ebayc3.com <<EOSSH
hostname
cd /usr/spark/jobs/chocolate-1.0-SNAPSHOT-bin/chocolate-cappingrule/bin/qa
sudo su spark
pwd
echo "on remote server's param = $scanStopTime"
./ipCappingRuleJob.sh qa_transactional catherine_test EPN $scanStopTime 1440 15 300
EOSSH