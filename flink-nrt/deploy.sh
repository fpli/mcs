#!/bin/bash

set -x

CUR_MIN=$(date +%Y%m%d%H%M)
SHORT_GIT_COMMIT=$(git rev-parse --short HEAD)
CUR_VERSION="${CUR_MIN}-${SHORT_GIT_COMMIT}"

mvn -DskipTests=true clean package -Dcommit.id=${CUR_VERSION} job-uploader:upload -Dusername=${1} -Dpassword=${2} -Dnamespace=marketing-tracking -DjobJarName=chocolate-flink-nrt -DjobJarTag=2.0-SNAPSHOT-${CUR_VERSION}