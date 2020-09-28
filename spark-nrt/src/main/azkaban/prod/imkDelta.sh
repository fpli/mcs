#
# Copyright (c) 2020. eBay inc. All rights reserved.
#

bin=$(dirname "$0")
bin=$(
  cd "$bin" >/dev/null
  pwd
)

if [[ $? -ne 0 ]]; then
  echo "get latest path failed"
  exit 1
fi

JOB_NAME="IMK_DELTA"

for f in $(find $bin/../../conf/prod -name '*.*'); do
  FILES=${FILES},file://$f;
done

/datashare/mkttracking/tools/apollo_rno/spark_delta_lake/bin/spark-submit    \
--files ${FILES} \
--class com.ebay.traffic.chocolate.sparknrt.delta.imk.ImkDeltaNrtJob \
--master yarn \
--deploy-mode cluster \
--queue hdlq-commrce-default \
${bin}/../../lib/chocolate-spark-nrt-*.jar \
--inputSource choco_data.tracking_event \
--deltaDir viewfs://apollo-rno/apps/b_marketing_tracking/delta_test/delta_table_imk/ \
--deltaDoneFileDir viewfs://apollo-rno/apps/b_marketing_tracking/delta_test/delta_done_files \
--outPutDir viewfs://apollo-rno/apps/b_marketing_tracking/delta_test/output_imk \
--outputDoneFileDir viewfs://apollo-rno/apps/b_marketing_tracking/delta_test/out_put_done_files \
--jobDir viewfs://apollo-rno/apps/b_marketing_tracking/delta_test/delta_job \
--doneFilePrefix delta_test_done_file_ \