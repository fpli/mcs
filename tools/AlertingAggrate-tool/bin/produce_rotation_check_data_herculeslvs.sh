#!/bin/bash

echo "start produce_rotation_check_data_herculeslvs.sh"

echo "start count rotations"
/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -f /datashare/mkttracking/tools/AlertingAggrate-tool/sql/rotations_herculeslvs.sql
echo "end count rotations"

echo "start count campaigns"
/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -f /datashare/mkttracking/tools/AlertingAggrate-tool/sql/campaigns_herculeslvs.sql
echo "end count campaigns"

echo "start count clients"
/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -f /datashare/mkttracking/tools/AlertingAggrate-tool/sql/clients_herculeslvs.sql
echo "end count clients"

echo "start count vendors"
/datashare/mkttracking/tools/hercules_lvs/hive-hercules/bin/hive -f /datashare/mkttracking/tools/AlertingAggrate-tool/sql/vendors_herculeslvs.sql
echo "end count vendors"