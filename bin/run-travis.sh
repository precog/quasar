#!/usr/bin/env bash
#

source "./bin/bashlib.sh"

export MONGO_2_6_LINUX=2.6.11
export MONGO_2_6_PORT=27018
export MONGO_2_6_AUTH=""
export MONGO_3_0_LINUX=3.0.7
export MONGO_3_0_PORT=27019
export MONGO_3_0_AUTH=""
export MONGO_3_0_RO_LINUX=3.0.9
export MONGO_3_0_RO_PORT=27019
export MONGO_3_0_RO_AUTH="--auth"
export MONGO_3_2_LINUX=3.2.3
export MONGO_3_2_PORT=27020
export MONGO_3_2_AUTH=""
export MONGO_LINUX="MONGO_${MONGO_RELEASE}_LINUX"
export MONGO_PORT="MONGO_${MONGO_RELEASE}_PORT"
export MONGO_AUTH="MONGO_${MONGO_RELEASE}_AUTH"

exitSuccess () {
  echo "$@" && exit 0
}
runCoverage() {
  runSbt "sbt-update" -v clean update checkHeaders
  runSbt "sbt-test-coverage" -v coverage test exclusive:test coverageReport
  travis_fold_eval "publish-coverage" pip install --user codecov '&&' codecov
}
runJarAndDoc() {
  runSbt "sbt-update" -v clean update
  runSbt "sbt-compile" -v compile
  runSbt "sbt-oneJar" -v oneJar
  runSbt "sbt-doc" -v doc
  travis_fold_eval "jar-test-publish" "$SCRIPT_DIR/testJar" '&&' "$SCRIPT_DIR/publishJarIfMaster"
}
runTests() {
  runSbt "sbt-update" -v update
  runSbt "sbt-test" -v test exclusive:test
}

if (( jobId > 2 )); then
  if (( pullReq > 0 )); then
    exitSuccess "Skipping job $jobId on pull request $pullReq"
  elif [[ "$TRAVIS_BRANCH" != "master" ]]; then
    exitSuccess "Skipping job $jobId on push to non-master branch $TRAVIS_BRANCH"
  fi
fi

travis_fold_eval "quasar-mongo-init" quasar_mongo_init

if [[ -n ${CODE_COVERAGE:-} ]]; then
  runCoverage
elif (( jobId <= 2 )); then
  runJarAndDoc
else
  runTests
fi
