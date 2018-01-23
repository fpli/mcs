#!/usr/bin/env bash
# make-distribution.sh

set -o pipefail
set -e
set -x

# Figure out where the Chocolate framework is installed
CHOCOLATE_HOME="$(cd "`dirname "$0"`"; pwd)"

MAKE_TGZ=false
MVN="mvn"

function exit_with_usage {
  echo "make-distribution.sh - tool for making binary distributions of Chocolate"
  echo ""
  echo "usage:"
  cl_options="[--tgz] [--mvn <mvn-command>]"
  echo "./make-distribution.sh $cl_options <maven build options>"
  echo "See Chocolate' \"Building Chocolate\" doc for correct Maven options."
  echo ""
  exit 1
}

# Parse arguments
while (( "$#" )); do
  case $1 in
    --tgz)
      MAKE_TGZ=true
      ;;
    --mvn)
      MVN="$2"
      shift
      ;;
    --help)
      exit_with_usage
      ;;
    *)
      break
      ;;
  esac
  shift
done

if [ $(command -v git) ]; then
    GITREV=$(git rev-parse --short HEAD 2>/dev/null || :)
    if [ ! -z "$GITREV" ]; then
	 GITREVSTRING=" (git revision $GITREV)"
    fi
    unset GITREV
fi

if [ ! "$(command -v "$MVN")" ] ; then
    echo -e "Could not locate Maven command: '$MVN'."
    echo -e "Specify the Maven command with the --mvn flag"
    exit -1;
fi

VERSION=$("$MVN" help:evaluate -Dexpression=project.version $@ 2>/dev/null | grep -v "INFO" | tail -n 1)
SCALA_VERSION=$("$MVN" help:evaluate -Dexpression=scala.compat.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | tail -n 1)
SPARK_VERSION=$("$MVN" help:evaluate -Dexpression=spark.version $@ 2>/dev/null\
    | grep -v "INFO"\
    | tail -n 1)

echo "Chocolate version is $VERSION"

DISTDIR="$CHOCOLATE_HOME/chocolate-$VERSION-bin"

if [ "$MAKE_TGZ" == "true" ]; then
  echo "Making chocolate-$VERSION-bin.tgz"
else
  echo "Making distribution for chocolate $VERSION in $DISTDIR..."
fi

# Build uber fat JAR
cd "$CHOCOLATE_HOME"

export MAVEN_OPTS="${MAVEN_OPTS:--Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m}"

# Store the command as an array because $MVN variable might have spaces in it.
# Normal quoting tricks don't work.
# See: http://mywiki.wooledge.org/BashFAQ/050
BUILD_COMMAND=("$MVN" clean package -DskipTests $@)

# Actually build the jar
echo -e "\nBuilding with..."
echo -e "\$ ${BUILD_COMMAND[@]}\n"

"${BUILD_COMMAND[@]}"

# Make directories
rm -rf "$DISTDIR"
mkdir -p "$DISTDIR"
echo "Chocolate $VERSION$GITREVSTRING built for Spark $SPARK_VERSION" > "$DISTDIR/RELEASE"
echo "Build flags: $@" >> "$DISTDIR/RELEASE"

# Chocolate Capping Rule
CHOCOLATE_CAPPING_RULE_BIN="$DISTDIR/chocolate-cappingrule"
mkdir -p "$CHOCOLATE_CAPPING_RULE_BIN/lib"
cp "$CHOCOLATE_HOME"/capping-rules/target/chocolate-capping-rules-*-fat.jar "$CHOCOLATE_CAPPING_RULE_BIN"/lib/

#mkdir -p "$CHOCOLATE_CAPPING_RULE_BIN/conf"
#cp -r "$CHOCOLATE_HOME"/common/src/main/conf/* "$CHOCOLATE_CAPPING_RULE_BIN"/conf
#cp -r "$CHOCOLATE_HOME"/capping-rules/src/main/conf/* "$CHOCOLATE_CAPPING_RULE_BIN"/conf

mkdir -p "$CHOCOLATE_CAPPING_RULE_BIN/bin"
cp -r "$CHOCOLATE_HOME"/common/src/main/bin/* "$CHOCOLATE_CAPPING_RULE_BIN"/bin
cp -r "$CHOCOLATE_HOME"/capping-rules/src/main/bin/* "$CHOCOLATE_CAPPING_RULE_BIN"/bin

# Chocolate Flume Ingester
CHOCOLATE_INGESTER_BIN="$DISTDIR/ingester"
mkdir -p "$CHOCOLATE_INGESTER_BIN/lib"
cp "$CHOCOLATE_HOME"/ingester/target/ingester-*-fat.jar "$CHOCOLATE_INGESTER_BIN"/lib/

mkdir -p "$CHOCOLATE_INGESTER_BIN/bin"
mkdir -p "$CHOCOLATE_INGESTER_BIN/conf"
# Generate bin and conf from template
cp "$CHOCOLATE_HOME"/ingester/src/bin/start_ingester.sh "$CHOCOLATE_INGESTER_BIN"/bin/

ZOOKEEPER_PHX="phxchocolatemaster-1446515.stratus.phx.ebay.com:2181,phxchocolatemaster-1446516.stratus.phx.ebay.com:2181,phxchocolatemaster-1446517.stratus.phx.ebay.com:2181,phxchocolatemaster-1650436.stratus.phx.ebay.com:2181,phxchocolatemaster-1650437.stratus.phx.ebay.com:2181"
ZOOKEEPER_SLC="slcchocolatemaster-1154249.stratus.slc.ebay.com:2181,slcchocolatemaster-1154250.stratus.slc.ebay.com:2181,slcchocolatemaster-1154251.stratus.slc.ebay.com:2181,slcchocolatemaster-1241743.stratus.slc.ebay.com:2181,slcchocolatemaster-1241744.stratus.slc.ebay.com:2181"
ZOOKEEPER_LVS="lvschocolatemaster-1448894.stratus.lvs.ebay.com:2181,lvschocolatemaster-1448895.stratus.lvs.ebay.com:2181,lvschocolatemaster-1448897.stratus.lvs.ebay.com:2181,lvschocolatemaster-1582061.stratus.lvs.ebay.com:2181,lvschocolatemaster-1582062.stratus.lvs.ebay.com:2181"
cp "$CHOCOLATE_HOME"/ingester/src/conf/flume_template.conf "$CHOCOLATE_INGESTER_BIN"/conf/flume_phx.conf
cp "$CHOCOLATE_HOME"/ingester/src/conf/flume_template.conf "$CHOCOLATE_INGESTER_BIN"/conf/flume_slc.conf
cp "$CHOCOLATE_HOME"/ingester/src/conf/flume_template.conf "$CHOCOLATE_INGESTER_BIN"/conf/flume_lvs.conf
sed -i '' "s/zookeeperConnectTemplate/$ZOOKEEPER_PHX/g" "$CHOCOLATE_INGESTER_BIN"/conf/flume_phx.conf
sed -i '' "s/zookeeperConnectTemplate/$ZOOKEEPER_SLC/g" "$CHOCOLATE_INGESTER_BIN"/conf/flume_slc.conf
sed -i '' "s/zookeeperConnectTemplate/$ZOOKEEPER_LVS/g" "$CHOCOLATE_INGESTER_BIN"/conf/flume_lvs.conf



if [ "$MAKE_TGZ" == "true" ]; then
  TARDIR_NAME=chocolate-cappingrule
  tar czf "$TARDIR_NAME.tgz" -C "$DISTDIR" "$TARDIR_NAME"
  mv "$TARDIR_NAME.tgz" "$DISTDIR"
  TARDIR_NAME=ingester
  tar czf "$TARDIR_NAME.tgz" -C "$DISTDIR" "$TARDIR_NAME"
  mv "$TARDIR_NAME.tgz" "$DISTDIR"

fi

if [ "$MAKE_TGZ" == "true" ]; then
  TARDIR_NAME=chocolate-$VERSION-bin
  tar czf "$TARDIR_NAME.tgz" -C "$CHOCOLATE_HOME" "$TARDIR_NAME"
fi


