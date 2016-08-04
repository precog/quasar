# To be sourced by bash scripts
#

set -euo pipefail # STRICT MODE
IFS=$'\n\t'       # http://redsymbol.net/articles/unofficial-bash-strict-mode/

[[ -n $TRAVIS ]] && unset SBT_OPTS JVM_OPTS JDK_HOME JAVA_HOME
jobId="${TRAVIS_JOB_NUMBER##*.}"
source "./scripts/constants"

case $TRAVIS_PULL_REQUEST in
  [0-9][0-9]*) pullReq="$TRAVIS_PULL_REQUEST" ;;
            *) pullReq=0 ;;
esac

travis_fold() {
  local action=$1
  local name=$2
  echo -en "travis_fold:${action}:${name}\r${ANSI_CLEAR}"
}
travis_fold_eval () {
  local foldid="$1" && shift
  travis_fold start "$foldid"
  eval "$@"
  travis_fold end "$foldid"
}

quasar_mongo_init () {
  echo "Installing mongo..."
  ./scripts/installMongo mongodb-linux-x86_64-${!MONGO_LINUX} mongo ${!MONGO_PORT} ${!MONGO_AUTH}

  echo "Setting up quasar test run..."
  echo "Script Path:   $SCRIPT_DIR"
  echo "Root Path:     $WS_DIR"
  echo "Version:       $QUASAR_VERSION"
  echo "Web Jar:       $QUASAR_WEB_JAR"
  echo "Web Jar Dir:   $QUASAR_WEB_JAR_DIR"
  echo "Web Jar Path:  $QUASAR_WEB_JAR_PATH"
  echo "REPL Jar:      $QUASAR_REPL_JAR"
  echo "REPL Jar Dir:  $QUASAR_REPL_JAR_DIR"
  echo "REPL Jar Path: $QUASAR_REPL_JAR_PATH"

  QUASAR_MONGODB_TESTDB="quasar-test"
  QUASAR_MONGODB_HOST_2_6="localhost:27018"
  QUASAR_MONGODB_HOST_3_0="localhost:27019"
  QUASAR_MONGODB_HOST_3_2="localhost:27020"

  # Enables running tests for a single mongo release by specifying an argument of
  # "2_6", "3_0", or "3_2" for the MongoDB 2.6.x, 3.0.x, and 3.2.x releases.
  if [[ "$#" == "1" ]]; then
    QUASAR_MONGODB_RELEASE="$1"
  else
    QUASAR_MONGODB_RELEASE="ALL"
  fi

  # Perform setup for integration tests:
  if [[ ${LOCAL_MONGODB:-} == "true" ]] ; then
    export QUASAR_TEST_PATH_PREFIX="/${QUASAR_MONGODB_TESTDB}/"

    if [[ "$QUASAR_MONGODB_RELEASE" == "2_6" ]]; then
      export QUASAR_MONGODB_2_6="{\"mongodb\": {\"connectionUri\": \"mongodb://${QUASAR_MONGODB_HOST_2_6}\"}}"
    elif [[ "$QUASAR_MONGODB_RELEASE" == "3_0" ]]; then
      export QUASAR_MONGODB_3_0="{\"mongodb\": {\"connectionUri\": \"mongodb://${QUASAR_MONGODB_HOST_3_0}\"}}"
    elif [[ "$QUASAR_MONGODB_RELEASE" == "3_0_RO" ]]; then
      export QUASAR_MONGODB_READ_ONLY="{\"mongodb\": {\"connectionUri\": \"mongodb://quasar-read:quasar@${QUASAR_MONGODB_HOST_3_0}/${QUASAR_MONGODB_TESTDB}\"}}"
      export QUASAR_MONGODB_READ_ONLY_INSERT="{\"mongodb\": {\"connectionUri\": \"mongodb://quasar-dbOwner:quasar@${QUASAR_MONGODB_HOST_3_0}/${QUASAR_MONGODB_TESTDB}\"}}"
    elif [[ "$QUASAR_MONGODB_RELEASE" == "3_2" ]]; then
      export QUASAR_MONGODB_3_2="{\"mongodb\": {\"connectionUri\": \"mongodb://${QUASAR_MONGODB_HOST_3_2}\"}}"
    else
      export QUASAR_MONGODB_2_6="{\"mongodb\": {\"connectionUri\": \"mongodb://${QUASAR_MONGODB_HOST_2_6}\"}}"
      export QUASAR_MONGODB_3_0="{\"mongodb\": {\"connectionUri\": \"mongodb://${QUASAR_MONGODB_HOST_3_0}\"}}"
      export QUASAR_MONGODB_READ_ONLY="{\"mongodb\": {\"connectionUri\": \"mongodb://quasar-read:quasar@${QUASAR_MONGODB_HOST_3_0}/${QUASAR_MONGODB_TESTDB}\"}}"
      export QUASAR_MONGODB_READ_ONLY_INSERT="{\"mongodb\": {\"connectionUri\": \"mongodb://quasar-dbOwner:quasar@${QUASAR_MONGODB_HOST_3_0}/${QUASAR_MONGODB_TESTDB}\"}}"
      export QUASAR_MONGODB_3_2="{\"mongodb\": {\"connectionUri\": \"mongodb://${QUASAR_MONGODB_HOST_3_2}\"}}"
    fi

    echo "Using local MongoDB config"
  fi
}

runSbtDirect () {
  echo "% sbt ++$TRAVIS_SCALA_VERSION -batch $@"
  ./sbt ++$TRAVIS_SCALA_VERSION -batch "$@"
}
runSbt () {
  local foldid="$1" && shift
  travis_fold_eval $foldid runSbtDirect "$@" '|' ts '%H:%M:%.S'
}
