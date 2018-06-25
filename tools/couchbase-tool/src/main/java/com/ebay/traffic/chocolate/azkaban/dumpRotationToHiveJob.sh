#!/bin/bash
# run job to pull transaction data from TD to apollo
whoami
ssh -T -i /usr/azkaban/id_rsa_mta yimeng@lvschocolatepits-1583710.stratus.lvs.ebay.com <<EOSSH
/chocolate/rotation/dumpRotationToHive.sh
EOSSH
