#!/usr/bin/env bash
#

set -euo pipefail

jobId="${TRAVIS_JOB_NUMBER##*.}"
pullReq=0

case $TRAVIS_PULL_REQUEST in
  [0-9][0-9]*) pullReq="$TRAVIS_PULL_REQUEST" ;;
esac

if (( jobId > 1)); then
  if (( pullReq > 0 )); then
    echo "Skipping job $TRAVIS_JOB_NUMBER on pull request $pullReq"
    exit 0
  fi
  if [[ "$TRAVIS_BRANCH" != "master" ]]; then
    echo "Skipping job $TRAVIS_JOB_NUMBER on push to non-master branch $TRAVIS_BRANCH"
    exit 0
  fi
fi

runTests () {
  ./scripts/installMongo mongodb-linux-x86_64-${!MONGO_LINUX} mongo ${!MONGO_PORT} ${!MONGO_AUTH}
  ./scripts/build $MONGO_RELEASE
}

runTests
