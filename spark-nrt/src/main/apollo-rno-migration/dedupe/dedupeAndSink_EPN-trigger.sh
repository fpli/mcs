#!/bin/bash
./SubmitJob.sh "LVS" "/datashare/mkttracking/jobs/tracking/sparknrt_v2/bin/prod" "./dedupeAndSink.sh EPN marketing.tracking.ssl.filtered-epn viewfs://apollo-rno/user/b_marketing_tracking/tracking-events-workdir viewfs://apollo-rno/user/b_marketing_tracking/tracking-events 1 true"
