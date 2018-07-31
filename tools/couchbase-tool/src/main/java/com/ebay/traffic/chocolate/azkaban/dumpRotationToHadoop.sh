#!/bin/bash
# run job to pull transaction data from TD to apollo
whoami
ssh -t -t -i /usr/azkaban/id_rsa_rotation yimeng@slcchocolatepits-1242746.stratus.slc.ebay.com <<EOSSH
/chocolate/rotation/bin/dumpRotationToHadoop.sh
rc=$?
if [[ ${rc} != 0 ]]; then
   echo "=====================================================JOB ERROR!!======================================================"
   exit ${rc}
else
   echo "=====================================================JOB Completed======================================================"
   exit 0
fi
EOSSH
