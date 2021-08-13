#!/bin/bash
export HADOOP_USER_NAME=chocolate
RESULT_PATH=viewfs://apollo-rno//apps/b_marketing_tracking/chocolate/epnnrt-automation-report/click_result.txt
SUBJECT="epnnrt click automation result"
EMAIL_CONTENT=`/datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hdfs dfs -text ${RESULT_PATH}`
export mail_list='Marketing-Tracking-oncall@ebay.com','DL-eBay-Chocolate-GC@ebay.com'
export mail_smtp='mx.vip.lvs.ebay.com:25'
export mail_type='message-content-type=html'
export mail_fm='_choco_admin@slcchocolatepits-1242736.stratus.slc.ebay.com'
echo -e "${EMAIL_CONTENT}" | /datashare/mkttracking/jobs/tracking/epnnrt_new_test/bin/sendEmail -u "$SUBJECT" -f $mail_fm  -t $mail_list -s $mail_smtp -o $mail_type message-charset=utf-8
