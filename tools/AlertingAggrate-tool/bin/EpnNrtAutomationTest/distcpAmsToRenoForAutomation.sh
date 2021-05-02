#!/bin/bash

# Put files from chocolate hadoop to Apollo RNO and Hercules. The input files will be deleted.
# Input:    lvs Hadoop
#           /apps/tracking-events-workdir/meta/EPN/output/epnnrt_scp_click
#           hdfs://elvisha/apps/epn-nrt/click/date=
# Output:   Apollo RNO
#           /apps/b_marketing_tracking/chocolate/epnnrt/click/click_dt=
# Input:    lvs Hadoop
#           /apps/tracking-events-workdirmeta/EPN/output/epnnrt_scp_imp
#           hdfs://elvisha/apps/epn-nrt/impression/date=
# Output:   Apollo RNO
#           /apps/b_marketing_tracking/chocolate/epnnrt/imp/imprsn_dt=
# Schedule: /3 * ? * *
# case：
#./distcpAmsToRenoAndHercules.sh /apps/epn-nrt/click /apps/b_marketing_tracking/chocolate/epnnrt/click
set -x

usage="Usage: distcpAmsToRenoAndHercules.sh [metaDir] [renoDestDir] [type] [date]"

if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

export HADOOP_USER_NAME=chocolate
whoami
META_DIR=$1
RENO_DEST_DIR=$2
TYPE=$3
today=$4

if [ ${TYPE} == "click" ]
then
  DEST_DIR_PREFIX="click_dt"
elif [ ${TYPE} == "imp"  ]; then
  DEST_DIR_PREFIX="imprsn_dt"
else
  echo $usage
  exit 1
fi

echo "META_DIR:${META_DIR}"
echo "RENO_DEST_DIR:${RENO_DEST_DIR}"

HOST_NAME=`hostname -f`
RENO_DISTCP_DIR='/datashare/mkttracking/tools/apache/distcp/apollo'
echo "HOST_NAME ：${HOST_NAME}"
echo "RENO_DISTCP_DIR ：${RENO_DISTCP_DIR}"
echo "today ：${today}"

ENV_PATH='/datashare/mkttracking/tools/cake'
JOB_NAME='DistcpAmsToRenoForAutomationBatchJob'
echo "ENV_PATH:${ENV_PATH}"
echo "JOB_NAME:${JOB_NAME}"

cd ${RENO_DISTCP_DIR}

pwd
kinit -kt b_marketing_tracking_clients_PROD.keytab b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM
klist

META_PATH="hdfs://slickha${META_DIR}/date=${today}"
RENO_DEST_PATH="viewfs://apollo-rno${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${today}"
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -rm -r "${RENO_DEST_DIR}/*"
/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -mkdir -p "${RENO_DEST_DIR}/${DEST_DIR_PREFIX}=${today}"
hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_clients_PROD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking/${HOST_NAME}@PROD.EBAY.COM -targetkeytab b_marketing_tracking_clients_PROD.keytab -skipcrccheck -update ${META_PATH} ${RENO_DEST_PATH}
distcp_result_code=$?
echo "distcp_result_code:${distcp_result_code}"
if [ ${distcp_result_code} -ne 0 ]; then
  echo "Fail to distcp from local to Apollo, please check!!!"
  exit ${distcp_result_code};
fi


