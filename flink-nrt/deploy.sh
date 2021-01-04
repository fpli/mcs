#!/bin/bash

set -x

CUR_MIN=$(date +%Y%m%d%H%M)
SHORT_GIT_COMMIT=$(git rev-parse --short HEAD)
CUR_VERSION="${CUR_MIN}-${SHORT_GIT_COMMIT}"

mvn -DskipTests=true clean package -Dcommit.id=${CUR_VERSION} job-uploader:upload -Dusername=6a67e51fdcb84e7aa50b41a354f295b9  -Dpassword=PbXzklR5hPlwtsRHj3rlsdcSL8GPiVzojGIpfa4SyCNsBb5I8eJ0BVmMMOfL23My -Dnamespace=marketing-tracking -DjobJarName=chocolate-flink-nrt -DjobJarTag=2.0-SNAPSHOT-${CUR_VERSION}
