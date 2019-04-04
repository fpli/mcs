#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
/datashare/mkttracking/jobs/tracking/epnnrt/bin/prod/epnnrt-scheduler.sh
EOSSH