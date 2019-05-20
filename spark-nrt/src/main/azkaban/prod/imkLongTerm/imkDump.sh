#!/bin/bash
CHANNEL=$1
WORK_DIR=$2
OUTPUT_DIR=$3

../SubmitJob.sh "LVS" "/datashare/mkttracking/jobs/tracking/chocolate-sparknrt-imk/bin/prod" "./imkDump.sh ${CHANNEL} ${WORK_DIR} ${OUTPUT_DIR} http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200"