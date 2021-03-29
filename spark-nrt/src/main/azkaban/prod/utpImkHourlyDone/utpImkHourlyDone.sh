#!/bin/bash
whoami
echo "ssh -i /usr/azkaban/id_rsa_spark _choco_admin@slcchocolatepits-1242730.stratus.slc.ebay.com"
ssh -T -i /usr/azkaban/id_rsa_spark _choco_admin@slcchocolatepits-1242730.stratus.slc.ebay.com -o ServerAliveInterval=30 <<EOSSH
hostname
/datashare/mkttracking/jobs/utp/bin/prod/utpImkHourlyDone.sh ${1}
EOSSH

if [ $? -eq 0 ];then
    echo "job success"
else
	echo "job failed, retry another machine"
	echo "ssh -i /usr/azkaban/id_rsa_spark _choco_admin@slcchocolatepits-1242736.stratus.slc.ebay.com"
	ssh -T -i /usr/azkaban/id_rsa_spark _choco_admin@slcchocolatepits-1242736.stratus.slc.ebay.com -o ServerAliveInterval=30 <<EOSSH
	hostname
	/datashare/mkttracking/jobs/utp/bin/prod/utpImkHourlyDone.sh ${1}
EOSSH
fi

if [ $? -eq 0 ];then
	echo "job success"
	exit 0
else
	exit 1
fi
