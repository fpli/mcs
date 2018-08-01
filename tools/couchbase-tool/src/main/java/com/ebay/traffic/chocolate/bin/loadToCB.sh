#!/bin/bash
ROTATION_CONFIG_FILE=/chocolate/rotation/couchbase.properties
java -cp /chocolate/rotation/couchbase-tool-3.2-SNAPSHOT-fat.jar com.ebay.traffic.chocolate.couchbase.LoadRotationInfoIntoCB ${ROTATION_CONFIG_FILE}
