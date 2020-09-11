#!/bin/bash

echo "start produce_rotation_check_data_apollorno.sh"

echo "start count rotations"
/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -f /datashare/mkttracking/tools/AlertingAggrate-tool/sql/rotations_apollorno.sql
echo "end count rotations"

echo "start count campaigns"
/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -f /datashare/mkttracking/tools/AlertingAggrate-tool/sql/campaigns_apollorno.sql
echo "end count campaigns"

echo "start count clients"
/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -f /datashare/mkttracking/tools/AlertingAggrate-tool/sql/clients_apollorno.sql
echo "end count clients"

echo "start count vendors"
/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -f /datashare/mkttracking/tools/AlertingAggrate-tool/sql/vendors_apollorno.sql
echo "end count vendors"

echo "start count tracking_event"
/datashare/mkttracking/tools/apollo_rno/hive_apollo_rno/bin/hive -f /datashare/mkttracking/tools/AlertingAggrate-tool/sql/tracking_event_apollorno.sql
echo "end count tracking_event"