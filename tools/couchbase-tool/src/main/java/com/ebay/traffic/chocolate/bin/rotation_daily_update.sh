#!/usr/bin/env bash
# run job to update rotation related hive table daily

usage="Usage: rotation_daily_update.sh"

/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/dumpRotationDaily.sh
rc=$?
if [[ $rc != 0 ]]; then
    echo "=== dumpRotationDaily error ==="
    exit $rc
else
    echo " ===== dumpRotationDaily succeed===="
fi

/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_rno_hive_daily_job.sh
rc=$?
if [[ $rc != 0 ]]; then
    echo "=== rotation_rno_daily_job error ==="
    exit $rc
else
    echo " ===== rotation_rno_daily_job succeed===="
fi

/datashare/mkttracking/jobs/tdmoveoff/rotation/bin/rotation_hercules_hive_daily_job.sh
rc=$?
if [[ $rc != 0 ]]; then
    echo "=== rotation_hercules_daily_job error ==="
    exit $rc
else
    echo " ===== rotation_hercules_daily_job succeed===="
fi

rc=$?
if [[ $rc != 0 ]]; then
    echo " ===== rotation_daily_update END With ERROR ====="
    exit $rc
else
    echo " =============== Job End ==========="
fi