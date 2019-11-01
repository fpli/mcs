#!/bin/bash
whoami
echo "ssh -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583707.stratus.lvs.ebay.com"
ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583707.stratus.lvs.ebay.com -o ServerAliveInterval=30 <<EOSSH
hostname
/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_rno_hive_daily_job.sh
EOSSH

if [ $? -eq 0 ];then
    echo "job success"
else
	echo "job failed, retry another machine"
	echo "ssh -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583720.stratus.lvs.ebay.com"
	ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583720.stratus.lvs.ebay.com -o ServerAliveInterval=30 <<EOSSH
	hostname
	/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_rno_hive_daily_job.sh
EOSSH
fi

if [ $? -eq 0 ];then
	echo "job success"
	exit 0
else
	exit -1
fi