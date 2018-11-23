#!/bin/bash
# run job to pull transaction data from TD to apollo
whoami
ssh -t -t -i /usr/azkaban/id_rsa_rotation yimeng@slcchocolatepits-1242752.stratus.slc.ebay.com <<EOSSH
/chocolate/seed/bin/pj_seed_job.sh 1200 60
rc=$?
if [[ ${rc} != 0 ]]; then
   echo "=====================================================JOB ERROR!!======================================================"
   exit ${rc}
else
   echo "=====================================================JOB Completed======================================================"
   exit 0
fi
EOSSH
