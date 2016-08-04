#!/usr/bin/env bash
#

source "./bin/bashlib.sh"

exitSuccess () {
  echo "$@" && exit 0
}
runTestJar () {
  set -x
  travis_fold_eval "run-test-jar" "$SCRIPT_DIR/testJar"
  set +x
}
runCoverage() {
  runSbt "sbt-update" -v update checkHeaders
  runSbt "sbt-test-with-coverage" -v coverage test exclusive:test coverageReport
}
runJarAndDoc() {
  runSbt "sbt-update" -v update
  runSbt "sbt-oneJar" -v oneJar
  runSbt "sbt-doc" -v doc
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
