#!/bin/bash

export HADOOP_USER_NAME=chocolate
TOUCH_DT=$(date -d "yesterday" +%Y%m%d)
hdfs -touchz viewfs://apollo-rno/apps/b_marketing_tracking/watch/${TOUCH_DT}/test_nd.done.${TOUCH_DT}
rc=$?
if [[ $rc != 0 ]]; then
    echo "$TOUCH_DT""===  Apollo-Rno Done File error ==="
    exit $rc
else
    echo "$TOUCH_DT"" =============== Apollo-Rno Done File End ==========="
fi
