#!/bin/bash
# shellcheck disable=SC2164
cd /datashare/mkttracking/jobs/tracking/sparknrt/bin/prod
./dedupeAndSink.sh EPN marketing.tracking.ssl.filtered-epn viewfs://apollo-rno/user/b_marketing_tracking/tracking-events-workdir viewfs://apollo-rno/user/b_marketing_tracking/tracking-events 1 true
