#!/usr/bin/env bash
usage="Usage: produce_td_monitor_data_hopper.sh]"

echo "hopper count start"
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh imk_rvr_trckng_event imk_hopper2apollorno.sql hopper
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh imk_rvr_trckng_event_dtl imk_dtl_hopper2apollorno.sql hopper
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh ams_click epn_click_hopper2apollorno.sql hopper
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh ams_imprsn epn_impression_hopper2apollorno.sql hopper
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh dw_mpx_rotations rotation_rotations_hopper2apollorno.sql hopper
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh dw_mpx_campaigns rotation_campaigns_hopper2apollorno.sql hopper
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh dw_mpx_clients rotation_clients_hopper2apollorno.sql hopper
/datashare/mkttracking/tools/AlertingAggrate-tool/bin/TD_to_apollorno.sh dw_mpx_vendors rotation_vendors_hopper2apollorno.sql hopper
echo "hopper count end"