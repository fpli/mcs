#!/bin/bash
# run spark job on YARN - epnReporting

usage="Usage: epnReporting.sh [workDir] [outPutDir]"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`

. ${bin}/../chocolate-env.sh

WORK_DIR=$1
OUTPUT_DIR=$2
ES_URL=http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200
