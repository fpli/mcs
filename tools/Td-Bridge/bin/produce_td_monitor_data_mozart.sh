#!/usr/bin/env bash
usage="Usage: produce_td_monitor_data_mozart.sh]"

echo "mozart count start"
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh imk_rvr_trckng_event imk_mozart2apollorno.sql mozart
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh imk_rvr_trckng_event_dtl imk_dtl_mozart2apollorno.sql mozart
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh ams_click epn_click_mozart2apollorno.sql mozart
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh ams_imprsn epn_impression_mozart2apollorno.sql mozart
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh dw_mpx_rotations rotation_rotations_mozart2apollorno.sql mozart
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh dw_mpx_campaigns rotation_campaigns_mozart2apollorno.sql mozart
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh dw_mpx_clients rotation_clients_mozart2apollorno.sql mozart
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh dw_mpx_vendors rotation_vendors_mozart2apollorno.sql mozart
echo "mozart count start"